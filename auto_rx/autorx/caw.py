#!/usr/bin/env python
#
#   radiosonde_auto_rx - Caw Notifications (nerdymark.com)
#
#   Posts radiosonde discovery / burst / lost-contact events to nerdymark.com's
#   "caw" feed — an ephemeral, location-based social feed (caws live one hour).
#   All caws are posted at the STATION location, so they appear in the operator's
#   local feed; the sonde's distance/bearing goes in the text. Anonymous POST,
#   no auth, like the site's other public write endpoints.
#
#   POST /api/caw  {latitude, longitude, timestamp, caw}
#
#   Released under GNU GPL v3 or later
#
import datetime
import json
import logging
import os
import time

import requests

from queue import Queue
from threading import Thread

import autorx
from .utils import position_info
from .geometry import GenericTrack


_COMPASS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
]


def _compass(bearing):
    return _COMPASS[int((bearing % 360.0) / 22.5 + 0.5) % 16]


class CawNotification(object):
    """Radiosonde Caw Notification Class.

    Accepts telemetry dictionaries from a decoder and posts a caw on discovery
    (first frame), burst (descent onset), and lost-contact (no telemetry for
    lost_contact_minutes). Incoming telemetry is processed via a queue, so this
    object should be thread safe. The lost-contact check runs off the queue loop's
    own tick, so it fires even when the sonde has gone silent.
    """

    REQUIRED_FIELDS = ["id", "lat", "lon", "alt", "type", "freq"]

    # Discard state for sondes not heard in this long (seconds). Must exceed the
    # lost-contact window so the lost-contact caw fires before cleanup.
    SONDE_MAX_AGE = 3600 * 3

    # Don't re-announce an event for a sonde id within this window (seconds).
    # Persisted, so the supervisor auto-restarting auto_rx doesn't re-caw.
    NOTIFIED_TTL = 3600 * 24

    # Politeness backstop between caws (seconds); the endpoint is also rate-limited.
    MIN_POST_INTERVAL = 8

    BURST_DESCENT_TRIP = 10
    CAW_MAX_CHARS = 280

    def __init__(
        self,
        caw_url="https://nerdymark.com/api/caw",
        discovery_notifications=True,
        burst_notifications=True,
        lost_contact_notifications=True,
        burst_altitude_threshold=5000,
        lost_contact_minutes=15,
        station_position=None,
        station_callsign=None,
    ):
        self.caw_url = caw_url
        self.discovery_notifications = discovery_notifications
        self.burst_notifications = burst_notifications
        self.lost_contact_notifications = lost_contact_notifications
        self.burst_altitude_threshold = burst_altitude_threshold
        self.lost_contact_s = lost_contact_minutes * 60.0
        self.station_position = station_position
        self.station_callsign = station_callsign

        self.sondes = {}

        self.notified_file = os.path.join(autorx.logging_path, "caw_notified.json")
        self.notified = self._load_notified()

        self.last_post_time = 0

        self.input_queue = Queue()
        self.input_processing_running = True
        self.input_thread = Thread(target=self.process_queue)
        self.input_thread.start()

        if self.station_position is None or self.station_position[0] == 0.0:
            self.log_error("No station position set — caws need a location; disabling.")
            self.input_processing_running = False
        else:
            self.log_info("Started Caw Notifier Thread (posting to %s)" % self.caw_url)

    # ------------------------------------------------------------------ queue

    def add(self, telemetry):
        for _field in self.REQUIRED_FIELDS:
            if _field not in telemetry:
                self.log_error("JSON object missing required field %s" % _field)
                return
        if self.input_processing_running:
            self.input_queue.put(telemetry)

    def process_queue(self):
        while self.input_processing_running:
            while self.input_queue.qsize() > 0:
                try:
                    _telem = self.input_queue.get_nowait()
                    self.process_telemetry(_telem)
                except Exception as e:
                    self.log_error("Error processing telemetry dict - %s" % str(e))

            time.sleep(2)
            self.check_lost_contact()      # fires on silence, off our own tick
            self.clean_telemetry_store()

    def close(self):
        self.input_processing_running = False
        if self.input_thread is not None:
            self.input_thread.join(60)
        self.log_debug("Stopped Caw Notifier Thread")

    # ------------------------------------------------------------ event logic

    def process_telemetry(self, telemetry):
        _id = telemetry["id"]

        if _id not in self.sondes:
            self.sondes[_id] = {
                "last_time": time.time(),
                "max_alt": telemetry["alt"],
                "descending_trip": 0,
                "ascent_trip": False,
                "burst_notified": _id in self.notified and "burst" in self.notified[_id],
                "lost_notified": _id in self.notified and "lost" in self.notified[_id],
                "last_telem": telemetry,
                "track": GenericTrack(max_elements=20),
            }
            self.sondes[_id]["track"].add_telemetry({
                "time": telemetry["datetime_dt"], "lat": telemetry["lat"],
                "lon": telemetry["lon"], "alt": telemetry["alt"],
            })
            if self.discovery_notifications:
                if _id in self.notified and "discovery" in self.notified[_id]:
                    self.log_debug("Sonde %s already announced, skipping discovery caw." % _id)
                else:
                    self.post_discovery(telemetry)
                    self._mark_notified(_id, "discovery")
            return

        # Existing sonde — update flight state.
        _state = self.sondes[_id]["track"].add_telemetry({
            "time": telemetry["datetime_dt"], "lat": telemetry["lat"],
            "lon": telemetry["lon"], "alt": telemetry["alt"],
        })
        self.sondes[_id]["last_time"] = time.time()
        self.sondes[_id]["last_telem"] = telemetry
        if telemetry["alt"] > self.sondes[_id]["max_alt"]:
            self.sondes[_id]["max_alt"] = telemetry["alt"]

        if self.sondes[_id]["burst_notified"] or not _state:
            return

        if telemetry["alt"] > self.burst_altitude_threshold:
            self.sondes[_id]["ascent_trip"] = True
        if self.sondes[_id]["ascent_trip"] and (_state["ascent_rate"] < -2.0):
            self.sondes[_id]["descending_trip"] += 1

        if self.sondes[_id]["descending_trip"] > self.BURST_DESCENT_TRIP:
            self.sondes[_id]["burst_notified"] = True
            self.log_info("Sonde %s burst detected." % _id)
            if self.burst_notifications:
                self.post_burst(telemetry, _state)
                self._mark_notified(_id, "burst")

    def check_lost_contact(self):
        """Post a lost-contact caw for any sonde we haven't heard from in
        lost_contact_minutes (once per sonde)."""
        if not self.lost_contact_notifications:
            return
        _now = time.time()
        for _id, _s in self.sondes.items():
            if _s["lost_notified"]:
                continue
            if (_now - _s["last_time"]) >= self.lost_contact_s:
                _s["lost_notified"] = True
                self.log_info("Sonde %s lost — no contact for %.0f min." %
                              (_id, self.lost_contact_s / 60.0))
                self.post_lost_contact(_id, _s)
                self._mark_notified(_id, "lost")

    def clean_telemetry_store(self):
        _now = time.time()
        for _id in list(self.sondes.keys()):
            if (_now - self.sondes[_id]["last_time"]) > self.SONDE_MAX_AGE:
                self.sondes.pop(_id)

    # ------------------------------------------------------------- caw text

    def _type_str(self, telemetry):
        return telemetry.get("subtype", telemetry["type"])

    def _range_str(self, telemetry):
        """'· 42 km NE' from the station to the sonde, or '' if no station pos."""
        if self.station_position is None or self.station_position[0] == 0.0:
            return ""
        try:
            _rel = position_info(
                self.station_position,
                (telemetry["lat"], telemetry["lon"], telemetry["alt"]))
            return " · %.0f km %s" % (
                _rel["straight_distance"] / 1000.0, _compass(_rel["bearing"]))
        except Exception:
            return ""

    def post_discovery(self, telemetry):
        _id = telemetry["id"]
        if telemetry.get("encrypted", False):
            _text = "🔒 Encrypted radiosonde detected: %s %s on %s (no position)" % (
                self._type_str(telemetry), _id, telemetry["freq"])
        else:
            _text = "🎈 New radiosonde: %s %s on %s · %s m%s" % (
                self._type_str(telemetry), _id, telemetry["freq"],
                "{:,}".format(int(telemetry["alt"])), self._range_str(telemetry))
        self.post_caw(_text)

    def post_burst(self, telemetry, sonde_state):
        _id = telemetry["id"]
        _text = "💥 Balloon burst! %s %s peaked at %s m, now descending %.0f m/s%s" % (
            self._type_str(telemetry), _id,
            "{:,}".format(int(self.sondes[_id]["max_alt"])),
            abs(sonde_state["ascent_rate"]), self._range_str(telemetry))
        self.post_caw(_text)

    def post_lost_contact(self, _id, sonde):
        _t = sonde.get("last_telem") or {}
        _mins = int(self.lost_contact_s / 60.0)
        _alt = "{:,} m".format(int(_t["alt"])) if "alt" in _t else "unknown alt"
        _text = "📡 Lost contact with %s %s — no telemetry for %d min, last heard at %s%s" % (
            (_t.get("subtype") or _t.get("type") or "sonde"), _id, _mins, _alt,
            self._range_str(_t) if "lat" in _t else "")
        self.post_caw(_text)

    # ------------------------------------------------------------------ post

    def post_caw(self, text):
        """POST a caw at the station location. Failures logged, never raised."""
        if self.station_position is None or self.station_position[0] == 0.0:
            self.log_error("No station position — cannot post caw.")
            return
        _since = time.time() - self.last_post_time
        if _since < self.MIN_POST_INTERVAL:
            time.sleep(self.MIN_POST_INTERVAL - _since)
        try:
            _resp = requests.post(self.caw_url, json={
                "latitude": self.station_position[0],
                "longitude": self.station_position[1],
                "timestamp": datetime.datetime.now(datetime.timezone.utc)
                .isoformat().replace("+00:00", "Z"),
                "caw": text[:self.CAW_MAX_CHARS],
            }, timeout=30)
            _resp.raise_for_status()
            self.last_post_time = time.time()
            self.log_info("Posted caw: %s" % text[:60])
        except Exception as e:
            self.log_error("Error posting caw - %s" % str(e))

    # ------------------------------------------------------------ persistence

    def _load_notified(self):
        try:
            with open(self.notified_file, "r") as f:
                _data = json.load(f)
            _cutoff = time.time() - self.NOTIFIED_TTL
            out = {}
            for _id, _events in _data.items():
                _ts = [v for v in _events.values() if isinstance(v, (int, float))]
                if _ts and max(_ts) > _cutoff:
                    out[_id] = _events
            return out
        except FileNotFoundError:
            return {}
        except Exception as e:
            self.log_error("Could not read caw-notified file - %s" % str(e))
            return {}

    def _mark_notified(self, _id, event):
        if _id not in self.notified:
            self.notified[_id] = {}
        self.notified[_id][event] = time.time()
        try:
            with open(self.notified_file, "w") as f:
                json.dump(self.notified, f)
        except Exception as e:
            self.log_error("Could not write caw-notified file - %s" % str(e))

    # ----------------------------------------------------------------- logging

    def log_debug(self, line):
        logging.debug("Caw - %s" % line)

    def log_info(self, line):
        logging.info("Caw - %s" % line)

    def log_error(self, line):
        logging.error("Caw - %s" % line)


if __name__ == "__main__":
    # Test: python -m autorx.caw   (posts a single test caw at the coords below)
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
    _c = CawNotification(station_position=(37.324, -121.890, 30.0), station_callsign="KO6ODW")
    _c.post_caw("🐦 caw notifier test — radiosonde bot online")
    _c.close()
