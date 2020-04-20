import zmq
import time
from gzip import GzipFile
import io
import csv
import sys

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

import configargparse

logging.basicConfig(level=os.getenv("LOGLEVEL", "WARNING"))

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


class GtfsDisplay:
    def __init__(self, args):
        self.routes = self.load_routes(
            "data/routes.txt", filter_=lambda self, row: row[2] in args.route
        )
        assert len(self.routes) > 0, "Not enough routes"
        self.stops = self.load_stops("data/stops.txt")
        self.stops_by_stop_code = {stop[1]: stop for stop in self.stops.values()}
        self.services = self.load_services("data/calendar_dates.txt")
        self.trips = self.load_trips("data/trips.txt")
        self.shape_keys = {trip.shape_id for trip in self.trips.values()}
        self.trips_by_journey = {
            trip.trip_short_name: trip for trip in self.trips.values()
        }
        self.schedule = self.load_schedule("data/stop_times.txt")
        self.shapes = self.load_shapes("data/shapes.txt")
        self.location = args.show

    def load_stops(self, fname, filter_=lambda self, row: True):
        """Load all stop data"""
        with load_csv(fname) as (Constructor, reader):
            d = {row[0]: Constructor._make(row) for row in reader if filter_(self, row)}
        return d

    def load_routes(
        self, fname, filter_=lambda self, row: row[1] == "WATERBUS" and row[5] == "4"
    ):
        """Load the routes data"""
        with load_csv(fname) as (Constructor, reader):
            d = {row[0]: Constructor._make(row) for row in reader if filter_(self, row)}
        return d

    def load_services(
        self,
        fname,
        filter_=lambda self, row: row[1] == datetime.now().strftime("%Y%m%d"),
    ):
        """Load the services data. This determines when the service is active"""
        with load_csv(fname) as (Constructor, reader):
            d = {row[0]: Constructor._make(row) for row in reader if filter_(self, row)}
        return d

    def load_trips(
        self,
        fname,
        filter_=lambda self, row: row[0] in self.routes and row[1] in self.services,
    ):
        """Load the journey data"""
        with load_csv(fname) as (Constructor, reader):
            d = {row[2]: Constructor._make(row) for row in reader if filter_(self, row)}
        return d

    def load_shapes(self, fname, filter_=lambda self, row: row[0] in self.shape_keys):
        """Load the shape of the routes"""
        d = defaultdict(list)
        with load_csv(fname) as (Constructor, reader):
            headers = Constructor._fields
            lat = headers.index("shape_pt_lat")
            lon = headers.index("shape_pt_lon")
            dist = headers.index("shape_dist_traveled")
            for row in reader:
                if filter_(self, row):
                    row[lat] = float(row[lat])
                    row[lon] = float(row[lon])
                    row[dist] = int(row[dist])
                    d[row[0]].append(Constructor._make(row))
        return d

    def load_schedule(self, fname, filter_=lambda self, row: row[0] in self.trips):
        """Load the schedule (arrival and destination times) of each trip"""
        d = defaultdict(list)
        with load_csv(fname) as (Constructor, reader):
            headers = Constructor._fields
            arrival = headers.index("arrival_time")
            departure = headers.index("departure_time")
            dist = headers.index("shape_dist_traveled")
            for row in reader:
                if filter_(self, row):
                    row[arrival] = normalize(row[arrival])
                    row[departure] = normalize(row[departure])
                    row[dist] = int(row[dist] if row[dist] else 0)
                    journey_stop = Constructor._make(row)
                    d[row[0]].append(journey_stop)
            for journey in d.keys():
                d[journey] = sorted(d[journey], key=lambda x: x.departure_time)

        return d

    ################################################################
    # Create and display the basic map
    ################################################################
    def show_map(self, name):
        """Display the map for a location"""
        g = geocoder.osm(name)
        bbox = g.json["bbox"]
        bbox = bbox["northeast"] + bbox["southwest"]
        map_ = smopy.Map(bbox, zoom=11)
        logging.debug("Bounding box: %s", map_.box)
        ax = map_.show_mpl(figsize=(10, 6))
        xs = []
        ys = []

        # This show all stops on the map, also not the ones
        # used for processing, but heck . . .
        for stop in self.stops.values():
            x, y = map_.to_pixels(float(stop[3]), float(stop[4]))
            xs.append(x)
            ys.append(y)

        plt.ion()
        plt.show()
        ax.plot(xs, ys, ".c")

        # Plot the tracks in blue
        for shape in self.shapes.values():
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

    def subscribe_ov_feed(self, map_, ax, locks, stop_flag):
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
                content = GzipFile(
                    "", "r", 0, io.BytesIO(b"".join(multipart[1:]))
                ).read()
                for line in content.split(b"\n"):
                    line = line.decode("utf-8")
                    if line.startswith("\L"):
                        keys = line[2:].split("|")
                        continue
                    # Hacky, reduce the nr of updates based on what I am interested in
                    # if not line.startswith("RET"):
                    #    continue
                    values = line.split("|")
                    d = {key: value for (key, value) in zip(keys, values)}
                    if (
                        "UserStopCode" in d
                        and d["UserStopCode"] in self.stops_by_stop_code
                    ):
                        stop = self.stops_by_stop_code[d["UserStopCode"]]
                        stop_id = stop[0]

                        # I ignored a few of the updates for now
                        # Most importantly, I do not check for cancelled stops
                        # A first version only checked for Metro lines, but that
                        # is commented now
                        # if not d["LinePlanningNumber"].startswith("M"):
                        #    continue
                        base = datetime.strptime(d["OperationDate"], "%Y-%m-%d")
                        expected_arrival = normalize(d["ExpectedArrivalTime"], base)
                        expected_departure = normalize(d["ExpectedDepartureTime"], base)

                        journey = d["JourneyNumber"]
                        if journey not in self.trips_by_journey:
                            continue
                        trip = self.trips_by_journey[d["JourneyNumber"]]
                        lock = locks[trip[2]]
                        with lock:
                            journey = self.schedule[trip[2]]
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
    def update_map(self, map_, ax, locks):
        """ Update the map with the current vehicle locations.
        """
        logging.debug("Start updating map")
        dots = []

        def pairwise(iterable):
            "s -> (s0,s1), (s1,s2), (s2, s3), ..."
            a, b = itertools.tee(iterable)
            next(b, None)
            return zip(a, b)

        while True:
            logging.debug("Updating . . . ")
            try:
                for dot in dots:
                    dot[0].remove()
                dots = []

                now = datetime.now()
                xs = []
                ys = []
                for id_, trip in self.schedule.items():
                    logging.debug("Updating %s", id_)
                    lock = locks[id_]
                    logging.debug("Locking %s", id_)
                    with lock:
                        for start, stop in pairwise(trip):
                            if stop.arrival_time <= now <= stop.departure_time:
                                # vehicle is currently at a stop
                                stop_info = self.stops[stop.stop_id]
                                x, y = map_.to_pixels(
                                    float(stop_info.stop_lat), float(stop_info.stop_lon)
                                )
                                dots.append(ax.plot(x, y, "or"))
                                break
                            elif start.departure_time < now < stop.arrival_time:
                                # vehicle is driving, determine exact location

                                # find the ratio of current segment in time
                                ratio = point_ratio(
                                    now, start.departure_time, stop.arrival_time
                                )

                                # now find the location in distance
                                # / assuming constant speed :-(
                                mid = int(
                                    weighted(
                                        ratio,
                                        start.shape_dist_traveled,
                                        stop.shape_dist_traveled,
                                    )
                                )

                                shape = self.shapes[self.trips[id_][9]]
                                if not shape:
                                    logging.warning(
                                        "Shape for trip %s, %s is empty",
                                        id_,
                                        trips[id_][9],
                                    )
                                    break

                                # find the corresponding line segment
                                seg_start, seg_end = find_surrounding(
                                    shape, mid, key=lambda x, mid: int(mid[4]) <= x
                                )

                                # determine the ratio of the line segment
                                ratio = point_ratio(
                                    mid,
                                    seg_start.shape_dist_traveled,
                                    seg_end.shape_dist_traveled,
                                )

                                # translate to lat, lon
                                vehicle_lat = weighted(
                                    ratio, seg_start.shape_pt_lat, seg_end.shape_pt_lat
                                )
                                vehicle_lon = weighted(
                                    ratio, seg_start.shape_pt_lon, seg_end.shape_pt_lon
                                )

                                # translate from coords to pixels
                                x, y = map_.to_pixels(vehicle_lat, vehicle_lon)

                                # save for drawing on map later
                                xs.append(x)
                                ys.append(y)
                                break

                # draw the saved locations on map
                dots.append(ax.plot(xs, ys, "om"))
                plt.pause(0.001)
                time.sleep(0.01)
            except KeyboardInterrupt:
                break

    def start(self):
        map_, ax = self.show_map(self.location)
        locks = {id_: threading.Lock() for id_ in self.schedule}
        stop = threading.Event()
        thread = threading.Thread(
            target=self.subscribe_ov_feed, args=[map_, ax, locks, stop]
        )
        thread.start()
        self.update_map(map_, ax, locks)
        stop.set()
        thread.join()


def point_ratio(point, start, end):
    """Find the ratio of two points on a line divided by a third"""
    return (end - point) / (end - start)


def weighted(ratio, start, end):
    """Given a ratio, find the weighted average of two points"""
    return ratio * start + (1 - ratio) * end


def find_surrounding(a, x, key=lambda x, mid: mid <= x):
    """Find the two elements in an array surrounding a point"""
    logging.debug("starting %d", len(a))
    assert len(a) > 0, "no data in array"

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


def get_parser():
    parser = configargparse.ArgParser()
    parser.add("--show", default="Rotterdam")
    parser.add("-r", "--route", action="append")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    display = GtfsDisplay(args)
    display.start()


if __name__ == "__main__":
    main()
