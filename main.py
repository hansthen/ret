import zmq
import time
from gzip import GzipFile
import io
import csv

import itertools
import matplotlib.pyplot as plt
import logging
import os
import smopy
import geocoder
from datetime import datetime, timedelta
from collections import defaultdict, namedtuple
import threading
from contextlib import contextmanager

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))

#########################################################
# Load and filter the gtfs feed data
#########################################################


@contextmanager
def load_csv(fname):
    """Utility function to handle csv loading"""
    name = os.path.basename(fname).replace(".txt", "")
    with open(fname) as f:
        reader = csv.reader(f)
        headers = next(reader)
        NamedTuple = namedtuple(name, headers)
        yield (NamedTuple, reader)


def load_stops(fname, filter_=lambda row: row[1].startswith("HA")):
    """Load all stop data"""
    with load_csv(fname) as (Constructor, reader):
        d = {row[0]: Constructor._make(row) for row in reader if filter_(row)}
    return d


def load_routes(fname, filter_=lambda row: row[1] == "RET" and row[5] == "1"):
    """Load the routes data"""
    with load_csv(fname) as (Constructor, reader):
        d = {row[0]: Constructor._make(row) for row in reader if filter_(row)}
    return d


def load_services(
    fname, filter_=lambda row: row[1] == datetime.now().strftime("%Y%m%d")
):
    """Load the services data. This determines when the service is active"""
    with load_csv(fname) as (Constructor, reader):
        d = {row[0]: Constructor._make(row) for row in reader if filter_(row)}
    return d


def load_trips(fname, filter_=lambda row: row[0] in routes and row[1] in services):
    """Load the journey data"""
    with load_csv(fname) as (Constructor, reader):
        d = {row[2]: Constructor._make(row) for row in reader if filter_(row)}
    return d


def load_shapes(fname, filter_=lambda row: row[0] in shape_keys):
    """Load the shape of the routes"""
    d = defaultdict(list)
    with load_csv(fname) as (Constructor, reader):
        headers = Constructor._fields
        lat = headers.index("shape_pt_lat")
        lon = headers.index("shape_pt_lon")
        dist = headers.index("shape_dist_traveled")
        for row in reader:
            if filter_(row):
                row[lat] = float(row[lat])
                row[lon] = float(row[lon])
                row[dist] = int(row[dist])
                d[row[0]].append(Constructor._make(row))
    return d


def load_schedule(fname, filter_=lambda row: row[0] in trips):
    """Load the schedule (arrival and destination times) of each trip"""
    d = defaultdict(list)
    with load_csv(fname) as (Constructor, reader):
        headers = Constructor._fields
        arrival = headers.index("arrival_time")
        departure = headers.index("departure_time")
        dist = headers.index("shape_dist_traveled")
        for row in reader:
            if filter_(row):
                row[arrival] = normalize(row[arrival])
                row[departure] = normalize(row[departure])
                row[dist] = int(row[dist])
                journey_stop = Constructor._make(row)
                d[row[0]].append(journey_stop)
        for journey in d.keys():
            d[journey] = sorted(d[journey], key=lambda x: x.departure_time)

    return d


################################################################
# Create and display the basic map
################################################################


def show_map(name):
    """Display the map for a location"""
    g = geocoder.osm(name)
    bbox = g.json["bbox"]
    bbox = bbox["northeast"] + bbox["southwest"]
    map_ = smopy.Map(bbox, zoom=11)
    ax = map_.show_mpl(figsize=(10, 6))
    xs = []
    ys = []

    # This show all stops on the map, also not the ones
    # used for processing, but heck . . .
    for stop in stops.values():
        x, y = map_.to_pixels(float(stop[3]), float(stop[4]))
        xs.append(x)
        ys.append(y)

    plt.ion()
    plt.show()
    ax.plot(xs, ys, ".c")

    # Plot the subway tracks in blue
    for shape in shapes.values():
        xys = [
            map_.to_pixels(element.shape_pt_lat, element.shape_pt_lon)
            for element in shape
        ]

        xs = [elem[0] for elem in xys]
        ys = [elem[1] for elem in xys]
        ax.plot(xs, ys, "-b")

    plt.pause(0.001)
    return map_, ax


#################################################################
# Process updates from the ov feed on openov.nl
#################################################################


def subscribe_ov_feed(map_, ax, locks, stop_flag):
    context = zmq.Context()
    kv8 = context.socket(zmq.SUB)
    kv8.connect("tcp://kv7.openov.nl:7817")
    kv8.setsockopt(zmq.SUBSCRIBE, b"/GOVI/KV8")

    poller = zmq.Poller()
    poller.register(kv8, zmq.POLLIN)

    while not stop_flag.wait(0.0):
        keys = []
        socks = dict(poller.poll())
        if socks.get(kv8) == zmq.POLLIN:
            multipart = kv8.recv_multipart()
            content = GzipFile("", "r", 0, io.BytesIO(b"".join(multipart[1:]))).read()
            for line in content.split(b"\n"):
                line = line.decode("utf-8")
                if line.startswith("\L"):
                    keys = line[2:].split("|")
                    continue
                if not line.startswith("RET"):
                    continue
                values = line.split("|")
                d = {key: value for (key, value) in zip(keys, values)}
                if "UserStopCode" in d and d["UserStopCode"] in stops_by_stop_code:
                    stop = stops_by_stop_code[d["UserStopCode"]]
                    stop_id = stop[0]

                    # I ignored a few of the updates for now
                    # Most importantly, I do not check for cancelled stops
                    if not d["LinePlanningNumber"].startswith("M"):
                        continue
                    base = datetime.strptime(d["OperationDate"], "%Y-%m-%d")
                    expected_arrival = normalize(d["ExpectedArrivalTime"], base)
                    expected_departure = normalize(d["ExpectedDepartureTime"], base)

                    trip = trips_by_journey[d["JourneyNumber"]]
                    lock = locks[trip[2]]
                    with lock:
                        journey = schedule[trip[2]]
                        for index, item in enumerate(journey):
                            if item.stop_id == stop_id:
                                break
                        else:
                            continue

                        if item.arrival_time != expected_arrival:
                            logging.debug(
                                "%s %s %s",
                                item,
                                repr(expected_arrival),
                                repr(expected_departure),
                            )
                        journey[index] = item._replace(
                            arrival_time=expected_arrival,
                            departure_time=expected_departure,
                        )


#######################################################
# Periodically draw the location of a trip on the map
#######################################################


def update_map(map_, ax, locks):
    """ Update the map with the current vehicle locations.

        Args:
            map_ : the map object (mainly used to translate lat/lon to pixels)
            ax: the display object
            locks (dict): locks keyed by trip id
    """
    dots = []

    def pairwise(iterable):
        "s -> (s0,s1), (s1,s2), (s2, s3), ..."
        a, b = itertools.tee(iterable)
        next(b, None)
        return zip(a, b)

    while True:
        try:
            for dot in dots:
                dot[0].remove()
            dots = []

            now = datetime.now()
            xs = []
            ys = []
            for id_, trip in schedule.items():
                lock = locks[id_]
                with lock:
                    for start, stop in pairwise(trip):
                        if stop.arrival_time <= now <= stop.departure_time:
                            stop_info = stops[stop.stop_id]
                            x, y = map_.to_pixels(
                                float(stop_info.stop_lat), float(stop_info.stop_lon)
                            )
                            dots.append(ax.plot(x, y, "or"))
                            break
                        elif start.departure_time < now < stop.arrival_time:
                            ratio = point_ratio(
                                now, start.departure_time, stop.arrival_time
                            )
                            mid = int(
                                weighted(
                                    ratio,
                                    start.shape_dist_traveled,
                                    stop.shape_dist_traveled,
                                )
                            )

                            shape = shapes[trips[id_][9]]

                            seg_start, seg_end = find_surrounding(
                                shape, mid, key=lambda x, mid: int(mid[4] <= x)
                            )
                            ratio = point_ratio(
                                mid,
                                seg_start.shape_dist_traveled,
                                seg_end.shape_dist_traveled,
                            )
                            vehicle_lat = weighted(
                                ratio, seg_start.shape_pt_lat, seg_end.shape_pt_lat
                            )
                            vehicle_lon = weighted(
                                ratio, seg_start.shape_pt_lon, seg_end.shape_pt_lon
                            )
                            x, y = map_.to_pixels(vehicle_lat, vehicle_lon)
                            xs.append(x)
                            ys.append(y)
                            break
            dots.append(ax.plot(xs, ys, "om"))
            plt.pause(0.001)
            time.sleep(0.1)
        except KeyboardInterrupt:
            break


def point_ratio(point, start, end):
    """Find the ratio of two points on a line divided by a third"""
    return (end - point) / (end - start)


def weighted(ratio, start, end):
    """Given a ratio, find the weighted average of two points"""
    return ratio * start + (1 - ratio) * end


def find_surrounding(a, x, key=lambda x, mid: mid <= x):
    """Find the two elements in an array surrounding a point"""
    hi, lo = len(a), 0
    while lo < hi:
        mid = (lo + hi) // 2
        if int(a[mid][4]) <= x:
            lo = mid + 1
        else:
            hi = mid
    return a[lo - 1], a[lo]


def normalize(
    time_string, base=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
):

    """Normalize a time specified in 24h+ notation into a python date"""
    hours, minutes, seconds = time_string.split(":")
    delta = timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))
    return base + delta


stops = load_stops("data/stops.txt")
stops_by_stop_code = {stop[1]: stop for stop in stops.values()}
routes = load_routes("data/routes.txt")
services = load_services("data/calendar_dates.txt")
trips = load_trips("data/trips.txt")
shape_keys = {trip.shape_id for trip in trips.values()}
trips_by_journey = {trip.trip_short_name: trip for trip in trips.values()}
schedule = load_schedule("data/stop_times.txt")
shapes = load_shapes("data/shapes.txt")

map_, ax = show_map("Rotterdam")
locks = {id_: threading.Lock() for id_ in schedule}
stop = threading.Event()
thread = threading.Thread(target=subscribe_ov_feed, args=[map_, ax, locks, stop])
thread.start()
update_map(map_, ax, locks)
stop.set()
thread.join()
