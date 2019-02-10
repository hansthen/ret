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

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))


def load_stops(fname, filter_=lambda row: row[1].startswith("HA")):
    """Load the stops data
    """
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
    return map_


def subscribe_ov_feed(ax, map_):
    context = zmq.Context()
    kv8 = context.socket(zmq.SUB)
    kv8.connect("tcp://kv7.openov.nl:7817")
    kv8.setsockopt(zmq.SUBSCRIBE, b"/GOVI/KV8")

    poller = zmq.Poller()
    poller.register(kv8, zmq.POLLIN)

    interesting = [
        "DataOwnerCode",
        "OperationDate",
        "LinePlanningNumber",
        "JourneyNumber",
        "UserStopCode",
        "LastUpdateTimeStamp",
        "Time",
        "ExpectedDepartureTime",
        "TripStopStatus",
        "VehicleNumber",
        "BlockCode",
    ]

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
                if "UserStopCode" in d and d["UserStopCode"] in stops:
                    stop = stops[d["UserStopCode"]]
                    lat, long_ = stop[3:5]
                    d["Lat"] = float(lat)
                    d["Long"] = float(long_)

                    if not d["LinePlanningNumber"].startswith("M"):
                        continue
                    expected_arrival = normalize(d)
                    journeys[(d["JourneyNumber"], d["LineDirection"])][
                        int(d["UserStopOrderNumber"])
                    ] = (expected_arrival, d["UserStopCode"])


def update_map():
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
        plt.pause(0.001)
        time.sleep(0.01)


def normalize(
    d, base=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
):
    hours, minutes, seconds = d.split(":")
    delta = timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))
    return base + delta


stops = load_stops("data/stops.txt")
routes = load_routes("data/routes.txt")
services = load_services("data/calendar_dates.txt")
trips, shape_keys = load_trips("data/trips.txt")
schedule = load_schedule("data/stop_times.txt")
shapes = load_shapes("data/shapes.txt")
journeys = defaultdict(dict)

map_ = load_map("Rotterdam")
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
    xys = [map_.to_pixels(float(element[2]), float(element[3])) for element in shape]

    xs = [elem[0] for elem in xys]
    ys = [elem[1] for elem in xys]
    ax.plot(xs, ys, "-b")


# plt.draw()
plt.pause(0.001)
# subscribe_ov_feed(ax, map_)
update_map()
