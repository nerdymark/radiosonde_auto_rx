#!/usr/bin/env python
#
#   radiosonde_auto_rx - Bluesky (atproto) Notifications
#
#   Posts sonde discovery and balloon-burst events to a Bluesky account,
#   in addition to (not instead of) the normal telemetry exporters.
#
#   Uses the atproto XRPC HTTP API directly via requests (already an auto_rx
#   dependency), so no additional SDK is required. Authentication is a
#   Bluesky App Password (Settings -> Privacy and Security -> App Passwords),
#   NOT the account password.
#
#   Released under GNU GPL v3 or later
#
import datetime
import json
import logging
import os
import re
import time

import requests

from queue import Queue
from threading import Thread

import autorx
from .utils import position_info, strip_sonde_serial
from .geometry import GenericTrack


# 16-wind compass names, for human-readable bearings.
_COMPASS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
]


def _compass(bearing):
    return _COMPASS[int((bearing % 360.0) / 22.5 + 0.5) % 16]


def _facets(text):
    """Generate atproto richtext facets for #hashtags and sondehub.org links
    in a post. Offsets are byte offsets into the UTF-8 encoded text."""
    facets = []
    _bytes = text.encode("utf-8")

    def _byte_index(char_index):
        return len(text[:char_index].encode("utf-8"))

    for _m in re.finditer(r"#(\w+)", text):
        facets.append(
            {
                "index": {
                    "byteStart": _byte_index(_m.start()),
                    "byteEnd": _byte_index(_m.end()),
                },
                "features": [
                    {"$type": "app.bsky.richtext.facet#tag", "tag": _m.group(1)}
                ],
            }
        )

    for _m in re.finditer(r"sondehub\.org/\S+", text):
        facets.append(
            {
                "index": {
                    "byteStart": _byte_index(_m.start()),
                    "byteEnd": _byte_index(_m.end()),
                },
                "features": [
                    {
                        "$type": "app.bsky.richtext.facet#link",
                        "uri": "https://" + _m.group(0),
                    }
                ],
            }
        )

    return facets


class BlueskyNotification(object):
    """Radiosonde Bluesky Notification Class.

    Accepts telemetry dictionaries from a decoder, and posts to Bluesky on
    newly detected sondes ('discovery') and on balloon burst / descent start
    ('burst'). Incoming telemetry is processed via a queue, so this object
    should be thread safe.
    """

    # We require the following fields to be present in the input telemetry dict.
    REQUIRED_FIELDS = ["id", "lat", "lon", "alt", "type", "freq"]

    # Discard state for sondes not heard in this long (seconds).
    SONDE_MAX_AGE = 3600 * 2

    # Don't re-announce a sonde id we already posted about within this window
    # (seconds). Persisted across restarts.
    NOTIFIED_TTL = 3600 * 24

    # Minimum interval between posts (seconds), as a Bluesky-politeness backstop.
    MIN_POST_INTERVAL = 30

    # Number of (not necessarily consecutive) frames with a strong descent rate
    # required before we call it a burst.
    BURST_DESCENT_TRIP = 10

    def __init__(
        self,
        handle=None,
        app_password=None,
        pds_url="https://bsky.social",
        discovery_notifications=True,
        burst_notifications=True,
        burst_altitude_threshold=5000,
        station_position=None,
        station_callsign=None,
    ):
        """Init a new Bluesky Notification Thread.

        Args:
            handle (str): Bluesky handle to post as, e.g. example.bsky.social
            app_password (str): A Bluesky App Password for the account.
            pds_url (str): Base URL of the PDS to authenticate against.
            discovery_notifications (bool): Post when a new sonde is heard.
            burst_notifications (bool): Post when a tracked sonde bursts.
            burst_altitude_threshold (float): A sonde must be seen above this
                altitude (metres) before a subsequent sustained descent is
                treated as a burst. Keeps ground-bounce and low-level noise
                from triggering bogus burst posts.
            station_position (tuple): (lat, lon, alt) of the station, for
                range/bearing lines in posts. Optional.
            station_callsign (str): Station callsign for post text. Optional.
        """
        self.handle = handle
        self.app_password = app_password
        self.pds_url = pds_url.rstrip("/")
        self.discovery_notifications = discovery_notifications
        self.burst_notifications = burst_notifications
        self.burst_altitude_threshold = burst_altitude_threshold
        self.station_position = station_position
        self.station_callsign = station_callsign

        # Dictionary to track sonde IDs and their flight state.
        self.sondes = {}

        # Persisted record of already-notified sonde ids, so a restart
        # mid-flight doesn't re-announce the same sonde.
        self.notified_file = os.path.join(autorx.logging_path, "bluesky_notified.json")
        self.notified = self._load_notified()

        self.last_post_time = 0

        # Input Queue.
        self.input_queue = Queue()

        # Start queue processing thread.
        self.input_processing_running = True
        self.input_thread = Thread(target=self.process_queue)
        self.input_thread.start()

        self.log_info("Started Bluesky Notifier Thread (posting as @%s)" % self.handle)

    # ------------------------------------------------------------------ queue

    def add(self, telemetry):
        """Add a telemetry dictionary to the input queue."""
        for _field in self.REQUIRED_FIELDS:
            if _field not in telemetry:
                self.log_error("JSON object missing required field %s" % _field)
                return

        if self.input_processing_running:
            self.input_queue.put(telemetry)

    def process_queue(self):
        """Process packets from the input queue."""
        while self.input_processing_running:
            while self.input_queue.qsize() > 0:
                try:
                    _telem = self.input_queue.get_nowait()
                    self.process_telemetry(_telem)
                except Exception as e:
                    self.log_error("Error processing telemetry dict - %s" % str(e))

            time.sleep(2)
            self.clean_telemetry_store()

    def close(self):
        """Shutdown the notifier thread."""
        self.input_processing_running = False
        if self.input_thread is not None:
            self.input_thread.join(60)
        self.log_debug("Stopped Bluesky Notifier Thread")

    # ------------------------------------------------------------ event logic

    def process_telemetry(self, telemetry):
        """Process a new telemetry dict; post on discovery and burst events."""
        _id = telemetry["id"]

        if _id not in self.sondes:
            self.sondes[_id] = {
                "last_time": time.time(),
                "max_alt": telemetry["alt"],
                "descending_trip": 0,
                "ascent_trip": False,
                "burst_notified": _id in self.notified
                and "burst" in self.notified[_id],
                "track": GenericTrack(max_elements=20),
            }
            self.sondes[_id]["track"].add_telemetry(
                {
                    "time": telemetry["datetime_dt"],
                    "lat": telemetry["lat"],
                    "lon": telemetry["lon"],
                    "alt": telemetry["alt"],
                }
            )

            if self.discovery_notifications:
                if _id in self.notified and "discovery" in self.notified[_id]:
                    self.log_debug(
                        "Sonde %s already announced, skipping discovery post." % _id
                    )
                else:
                    self.post_discovery(telemetry)
                    self._mark_notified(_id, "discovery")
            return

        # Existing sonde - update flight state.
        _sonde_state = self.sondes[_id]["track"].add_telemetry(
            {
                "time": telemetry["datetime_dt"],
                "lat": telemetry["lat"],
                "lon": telemetry["lon"],
                "alt": telemetry["alt"],
            }
        )
        self.sondes[_id]["last_time"] = time.time()
        if telemetry["alt"] > self.sondes[_id]["max_alt"]:
            self.sondes[_id]["max_alt"] = telemetry["alt"]

        if self.sondes[_id]["burst_notified"] or not _sonde_state:
            return

        # A burst is only plausible once the sonde has been seen well above
        # ground; this keeps pre-launch ground bounce from triggering.
        if telemetry["alt"] > self.burst_altitude_threshold:
            self.sondes[_id]["ascent_trip"] = True

        # Sustained strong descent after a confirmed ascent = burst.
        if self.sondes[_id]["ascent_trip"] and (_sonde_state["ascent_rate"] < -2.0):
            self.sondes[_id]["descending_trip"] += 1

        if self.sondes[_id]["descending_trip"] > self.BURST_DESCENT_TRIP:
            self.sondes[_id]["burst_notified"] = True
            self.log_info("Sonde %s burst detected." % _id)
            if self.burst_notifications:
                self.post_burst(telemetry, _sonde_state)
                self._mark_notified(_id, "burst")

    def clean_telemetry_store(self):
        """Remove any sondes we haven't heard from recently."""
        _now = time.time()
        for _id in list(self.sondes.keys()):
            if (_now - self.sondes[_id]["last_time"]) > self.SONDE_MAX_AGE:
                self.sondes.pop(_id)
                self.log_debug("Removed %s from tracked sondes." % _id)

    # ------------------------------------------------------------- post text

    def _type_str(self, telemetry):
        return telemetry.get("subtype", telemetry["type"])

    def _range_line(self, telemetry):
        if self.station_position is None or self.station_position[0] == 0.0:
            return None
        try:
            _rel = position_info(
                self.station_position,
                (telemetry["lat"], telemetry["lon"], telemetry["alt"]),
            )
            _line = "%.0f km %s of " % (
                _rel["straight_distance"] / 1000.0,
                _compass(_rel["bearing"]),
            )
            _line += self.station_callsign if self.station_callsign else "the station"
            return _line
        except Exception as e:
            self.log_debug("Could not compute station-relative position - %s" % str(e))
            return None

    def post_discovery(self, telemetry):
        _id = telemetry["id"]

        if telemetry.get("encrypted", False):
            _lines = [
                "🔒 Encrypted radiosonde detected: %s %s"
                % (self._type_str(telemetry), _id),
                "%s · telemetry is encrypted, no position available"
                % telemetry["freq"],
            ]
        else:
            _lines = ["🎈 New radiosonde: %s %s" % (self._type_str(telemetry), _id)]
            _stat = "%s · %s m" % (telemetry["freq"], "{:,}".format(int(telemetry["alt"])))
            if telemetry.get("vel_v", -9999.0) > -9999.0:
                _stat += " · %+.1f m/s" % telemetry["vel_v"]
            _lines.append(_stat)
            _range = self._range_line(telemetry)
            if _range:
                _lines.append(_range)
            _lines.append("sondehub.org/%s" % strip_sonde_serial(_id))

        _lines.append("#radiosonde #sondehub #hamradio")
        self.post("\n".join(_lines))

    def post_burst(self, telemetry, sonde_state):
        _id = telemetry["id"]
        _lines = [
            "💥 Balloon burst: %s %s" % (self._type_str(telemetry), _id),
            "Peak altitude %s m · now descending %.0f m/s"
            % (
                "{:,}".format(int(self.sondes[_id]["max_alt"])),
                abs(sonde_state["ascent_rate"]),
            ),
        ]
        _stat = telemetry["freq"]
        _range = self._range_line(telemetry)
        if _range:
            _stat += " · " + _range
        _lines.append(_stat)
        _lines.append("sondehub.org/%s" % strip_sonde_serial(_id))
        _lines.append("#radiosonde #sondehub")
        self.post("\n".join(_lines))

    # ------------------------------------------------------------------ xrpc

    def post(self, text):
        """Post text to Bluesky. Failures are logged, never raised - a Bluesky
        outage must not affect telemetry processing."""
        _since_last = time.time() - self.last_post_time
        if _since_last < self.MIN_POST_INTERVAL:
            time.sleep(self.MIN_POST_INTERVAL - _since_last)

        try:
            _session = requests.post(
                self.pds_url + "/xrpc/com.atproto.server.createSession",
                json={"identifier": self.handle, "password": self.app_password},
                timeout=30,
            )
            _session.raise_for_status()
            _session = _session.json()

            _record = {
                "$type": "app.bsky.feed.post",
                "text": text,
                "facets": _facets(text),
                "createdAt": datetime.datetime.now(datetime.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
            }
            _resp = requests.post(
                self.pds_url + "/xrpc/com.atproto.repo.createRecord",
                headers={"Authorization": "Bearer " + _session["accessJwt"]},
                json={
                    "repo": _session["did"],
                    "collection": "app.bsky.feed.post",
                    "record": _record,
                },
                timeout=30,
            )
            _resp.raise_for_status()
            self.last_post_time = time.time()
            self.log_info("Posted: %s" % text.splitlines()[0])
        except Exception as e:
            self.log_error("Error posting to Bluesky - %s" % str(e))

    # ------------------------------------------------------------ persistence

    def _load_notified(self):
        try:
            with open(self.notified_file, "r") as f:
                _data = json.load(f)
            _cutoff = time.time() - self.NOTIFIED_TTL
            return {
                _id: _events
                for (_id, _events) in _data.items()
                if max(_events.values()) > _cutoff
            }
        except FileNotFoundError:
            return {}
        except Exception as e:
            self.log_error("Could not read notified-sondes file - %s" % str(e))
            return {}

    def _mark_notified(self, _id, event):
        if _id not in self.notified:
            self.notified[_id] = {}
        self.notified[_id][event] = time.time()
        try:
            with open(self.notified_file, "w") as f:
                json.dump(self.notified, f)
        except Exception as e:
            self.log_error("Could not write notified-sondes file - %s" % str(e))

    # ----------------------------------------------------------------- logging

    def log_debug(self, line):
        logging.debug("Bluesky - %s" % line)

    def log_info(self, line):
        logging.info("Bluesky - %s" % line)

    def log_error(self, line):
        logging.error("Bluesky - %s" % line)


if __name__ == "__main__":
    # Test post: python -m autorx.bluesky <handle> <app_password>
    import sys

    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
    )
    _bsky = BlueskyNotification(
        handle=sys.argv[1],
        app_password=sys.argv[2],
        station_position=(37.32, -121.89, 30.0),
        station_callsign="TEST-STATION",
    )
    _bsky.post(
        "🎈 radiosonde_auto_rx Bluesky notifier test post\n#radiosonde #sondehub"
    )
    _bsky.close()
