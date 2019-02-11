import zmq
import time
from gzip import GzipFile
import io
import sys
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

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))


def load_stops(fname, filter_=lambda row: row[1].startswith("HA")):
    with open(fname) as f:
        reader = csv.reader(f)
        headers = next(reader)
        d = {row[0]: row for row in reader if filter_(row)}
    return d


def load_routes(fname, filter_=lambda row: row[1] == "RET" and row[5] == "1"):
    with open(fname) as f:
        reader = csv.reader(f)
        headers = next(reader)
        d = {row[0]: row for row in reader if filter_(row)}
    return d


def load_services(fname, filter_=lambda row: row[1] == "20190210"):
    with open(fname) as f:
        reader = csv.reader(f)
        headers = next(reader)
        d = {row[0]: row for row in reader if filter_(row)}
    return d


def load_trips(fname, filter_=lambda row: row[0] in routes and row[1] in services):
    with open(fname) as f:
        reader = csv.reader(f)
        headers = next(reader)
        d = {row[2]: row for row in reader if filter_(row)}
    shapes = {row[9] for row in d.values()}
    return d, shapes


def load_shapes(fname, filter_=lambda row: row[0] in shape_keys):
    d = defaultdict(list)
    with open(fname) as f:
        reader = csv.reader(f)
        headers = next(reader)

        for row in reader:
            if filter_(row):
                d[row[0]].append(row)
    return d


def load_schedule(fname, filter_=lambda row: row[0] in trips):
    d = defaultdict(list)
    with open(fname) as f:
        reader = csv.reader(f)
        headers = next(reader)
        arrival = headers.index("arrival_time")
        departure = headers.index("departure_time")
        JourneyStop = namedtuple("journey_stop", headers)
        for row in reader:
            if filter_(row):
                row[arrival] = normalize(row[arrival])
                row[departure] = normalize(row[departure])
                journey_stop = JourneyStop._make(row)
                d[row[0]].append(journey_stop)
        for journey in d.keys():
            d[journey] = sorted(d[journey], key=lambda x: x.departure_time)

    return d


def load_map(name):
    g = geocoder.osm(name)
    bbox = g.json["bbox"]
    bbox = bbox["northeast"] + bbox["southwest"]
    map_ = smopy.Map(bbox, zoom=11)
    ax = map_.show_mpl(figsize=(16, 12))
    xs = []
    ys = []
    for stop in stops.values():
        x, y = map_.to_pixels(float(stop[3]), float(stop[4]))
        xs.append(x)
        ys.append(y)

    plt.ion()
    plt.show()
    ax.plot(xs, ys, ".c")

    for shape in shapes.values():
        xys = [
            map_.to_pixels(float(element[2]), float(element[3])) for element in shape
        ]

        xs = [elem[0] for elem in xys]
        ys = [elem[1] for elem in xys]
        ax.plot(xs, ys, "-b")

    plt.pause(0.001)
    return map_, ax


def subscribe_ov_feed(map_, ax, locks):
    context = zmq.Context()
    kv8 = context.socket(zmq.SUB)
    kv8.connect("tcp://kv7.openov.nl:7817")
    kv8.setsockopt(zmq.SUBSCRIBE, b"/GOVI/KV8")

    poller = zmq.Poller()
    poller.register(kv8, zmq.POLLIN)

    while True:
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

                    if not d["LinePlanningNumber"].startswith("M"):
                        continue
                    base = datetime.strptime(d["OperationDate"], "%Y-%m-%d")
                    expected_arrival = normalize(d["ExpectedArrivalTime"])
                    expected_departure = normalize(d["ExpectedDepartureTime"])

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
                            print(item, expected_arrival, expected_departure)
                        journey[index] = item._replace(
                            arrival_time=expected_arrival,
                            departure_time=expected_departure,
                        )


def update_map(map_, ax, locks):
    dots = []

    def pairwise(iterable):
        "s -> (s0,s1), (s1,s2), (s2, s3), ..."
        a, b = itertools.tee(iterable)
        next(b, None)
        return zip(a, b)

    while True:
        for dot in dots:
            dot[0].remove()
        dots = []

        now = datetime.now()
        for id_, trip in schedule.items():
            lock = locks[id_]
            with lock:
                for start, stop in pairwise(trip):
                    if start.departure_time < now < stop.arrival_time:
                        ratio = (stop.arrival_time - now) / (
                            stop.arrival_time - start.departure_time
                        )
                        stop_info_start = stops[start.stop_id]
                        stop_info_stop = stops[stop.stop_id]

                        vehicle_lat = ratio * float(stop_info_start[3]) + (
                            1 - ratio
                        ) * float(stop_info_stop[3])
                        vehicle_lon = ratio * float(stop_info_start[4]) + (
                            1 - ratio
                        ) * float(stop_info_stop[4])
                        x, y = map_.to_pixels(vehicle_lat, vehicle_lon)
                        dots.append(ax.plot(x, y, "or"))
                        break
        plt.pause(0.001)
        time.sleep(0.01)


def normalize(
    time_string, base=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
):
    hours, minutes, seconds = time_string.split(":")
    delta = timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))
    return base + delta


stops = load_stops("data/stops.txt")
stops_by_stop_code = {stop[1]: stop for stop in stops.values()}
routes = load_routes("data/routes.txt")
services = load_services("data/calendar_dates.txt")
trips, shape_keys = load_trips("data/trips.txt")
trips_by_journey = {trip[6]: trip for trip in trips.values()}
schedule = load_schedule("data/stop_times.txt")
shapes = load_shapes("data/shapes.txt")
journeys = defaultdict(dict)

map_, ax = load_map("Rotterdam")
locks = {id_: threading.Lock() for id_ in schedule}
thread = threading.Thread(target=subscribe_ov_feed, args=[map_, ax, locks])
thread.start()
update_map(map_, ax, locks)
thread.join()
