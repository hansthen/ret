import zmq
import time
from gzip import GzipFile
import io
import sys
import csv


def load_stops(fname, filter_=lambda row: row[1].startswith("HA")):
    with open(fname) as f:
        reader = csv.reader(f)
        headers = next(reader)
        d = {row[1]: row for row in reader if filter_(row)}
    return d


def subscribe_ov_feed():
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
                if not line.startswith("RET"):
                    continue
                values = line.split("|")
                d = {
                    key: value
                    for (key, value) in zip(keys, values)
                    if key in interesting
                }
                if "UserStopCode" in d and d["UserStopCode"] in stops:
                    stop = stops[d["UserStopCode"]]
                    lat, long_ = stop[3:5]
                    d["Lat"] = lat
                    d["Long"] = long_
                print(d)


stops = load_stops("data/stops.txt")
subscribe_ov_feed()
