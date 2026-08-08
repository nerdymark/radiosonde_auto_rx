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
import math
import os
import random
import re
import time

import requests

from queue import Queue
from threading import Thread
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

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


# nerdymark.com map-render service, balloon mode. The page URL goes in the
# post's link embed; map.png (fetched with the IDENTICAL query string, which
# is cache-friendly server-side) becomes the card thumbnail. Bluesky link
# cards are client-built - there is no server-side unfurling.
MAP_PAGE_URL = "https://nerdymark.com/notam"
MAP_PNG_URL = "https://nerdymark.com/notam/map.png"
MAP_TIMEOUT = 20

# The page's view-on-Bluesky backlink (bsky= / bsky_handle=) is only accepted
# for nerdymark.com-domain handles (server-side anti-abuse allowlist). Other
# handles must omit both params - the map card still works without a backlink.
BACKLINK_DOMAIN = "nerdymark.com"

# The render service's reverse proxy rejects long URLs with a fast 502 (~2 KB
# query ceiling), well before the map API's documented 100-point track limit.
# Keep the whole query string under this so the map.png fetch AND the same-
# query page-embed URL both stay safe; the track is thinned to fit (the bsky
# backlink params are added later in post_card, hence the headroom).
MAX_QUERY_CHARS = 1800

# Timezone for human-readable times in card text (the host clock is UTC).
STATION_TZ = ZoneInfo("America/Los_Angeles")

_TID_CHARS = "234567abcdefghijklmnopqrstuvwxyz"


def _tid():
    """A TID record key (13-char base32-sortable): 53-bit microsecond
    timestamp + 10-bit clock id, top bit 0. Generated client-side so the map
    page URL can carry the post's own rkey BEFORE the post exists."""
    v = (time.time_ns() // 1_000 << 10) | random.getrandbits(10)
    return "".join(_TID_CHARS[(v >> (60 - 5 * i)) & 0x1F] for i in range(13))


def _downsample(points, limit=100):
    """Thin a [(lat, lon), ...] list to <= limit points, always keeping the
    first (the launch site's green dot) and the latest point. The map
    service rejects tracks with more than 100 points."""
    if len(points) <= limit:
        return points
    n = len(points)
    idx = sorted({round(i * (n - 1) / (limit - 1)) for i in range(limit)})
    return [points[i] for i in idx]


def _telem_time(telemetry):
    """Epoch seconds for a telemetry frame, from its datetime if present."""
    try:
        return telemetry["datetime_dt"].timestamp()
    except Exception:
        return time.time()


def _track_param(points):
    return ";".join("%.4f,%.4f" % (p[0], p[1]) for p in _downsample(points))


def _fit_track(points, budget):
    """Largest-detail track string whose URL-encoded length fits `budget`
    chars. Starts from the map API's 100-point cap and thins until it fits
    (each point encodes to ~22 chars, so this converges in a few steps)."""
    if budget <= 0:
        return None
    cap = min(len(points), 100)
    start = max(2, min(cap, budget // 22 + 4))
    for n in range(start, 1, -1):
        val = ";".join("%.4f,%.4f" % (p[0], p[1]) for p in _downsample(points, n))
        if len(urlencode({"track": val})) - len("track=") <= budget:
            return val
    return None


def _ft(alt_m):
    return "{:,}".format(int(alt_m * 3.28084))


_M_PER_DEG = 111320.0  # metres per degree of latitude (equirectangular approx)


def _wind_profile(path):
    """Turn an ascent path into a wind profile. On the way up the balloon is a
    near-perfect wind tracer, so each consecutive pair of fixes gives the
    horizontal wind (east/north m/s) at their mean altitude. Returns a list of
    (alt_m, wind_e, wind_n) sorted by altitude. `path` is [(lat, lon, alt, t),
    ...] oldest-first."""
    samples = []
    for a, b in zip(path, path[1:]):
        lat1, lon1, alt1, t1 = a[0], a[1], a[2], a[3]
        lat2, lon2, alt2, t2 = b[0], b[1], b[2], b[3]
        dt = t2 - t1
        if dt <= 0 or alt2 <= alt1:  # only ascending, forward-in-time legs
            continue
        lat0 = math.radians((lat1 + lat2) / 2.0)
        d_e = (lon2 - lon1) * math.cos(lat0) * _M_PER_DEG
        d_n = (lat2 - lat1) * _M_PER_DEG
        samples.append(((alt1 + alt2) / 2.0, d_e / dt, d_n / dt))
    samples.sort(key=lambda s: s[0])
    return samples


def _wind_at(samples, h):
    """Linearly interpolate the wind (e, n) at altitude h from a sorted
    profile, clamped to the ends."""
    if not samples:
        return 0.0, 0.0
    if h <= samples[0][0]:
        return samples[0][1], samples[0][2]
    if h >= samples[-1][0]:
        return samples[-1][1], samples[-1][2]
    for i in range(1, len(samples)):
        if h <= samples[i][0]:
            a0, e0, n0 = samples[i - 1]
            a1, e1, n1 = samples[i]
            f = (h - a0) / (a1 - a0) if a1 > a0 else 0.0
            return e0 + f * (e1 - e0), n0 + f * (n1 - n0)
    return samples[-1][1], samples[-1][2]


def predict_landing(path, lat, lon, alt):
    """Predict a radiosonde's landing point by integrating a parachute
    descent from (lat, lon, alt) back down through the winds we measured on
    the way up. The descent rate rises with altitude as air thins
    (v(h) = V0 * exp(h / 2H), the standard density-scaling used by hobby
    predictors), so the sonde spends most of its time - and drift - in the
    lower atmosphere. Returns {lat, lon, drift_m, dur_s} or None if the
    ascent profile is too thin to be useful."""
    samples = _wind_profile(path)
    if len(samples) < 3 or alt <= 0:
        return None

    V0 = 5.0        # sea-level parachute descent rate, m/s (typical RS41 ~5)
    SCALE_H = 7000.0  # atmospheric density scale height, m
    STEP = 250.0

    drift_e = drift_n = dur = 0.0
    h = alt
    while h > 0:
        dh = min(STEP, h)
        hm = h - dh / 2.0
        v = V0 * math.exp(hm / (2.0 * SCALE_H))
        dt = dh / v
        we, wn = _wind_at(samples, hm)
        drift_e += we * dt
        drift_n += wn * dt
        dur += dt
        h -= dh

    lat0 = math.radians(lat)
    return {
        "lat": lat + drift_n / _M_PER_DEG,
        "lon": lon + drift_e / (_M_PER_DEG * math.cos(lat0)),
        "drift_m": math.hypot(drift_e, drift_n),
        "dur_s": dur,
    }


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

    # Record a flight-path point for the map card at most this often (seconds).
    # A ~3 h flight at this rate stays well under memory concern; the map URL
    # is separately downsampled to <= 100 points.
    PATH_MIN_INTERVAL = 30

    # A burst sonde silent this long (seconds) is presumed down - post the
    # last-heard position with a search radius.
    LANDING_SILENCE = 600

    # Re-login after this long (seconds); atproto access JWTs last ~2 h, and
    # createSession is rate-limited per account, so sessions are reused.
    SESSION_MAX_AGE = 3000

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

        # Cached atproto session ({"did", "accessJwt", ...}) + creation time.
        self._session = None
        self._session_time = 0.0

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
                "landing_notified": _id in self.notified
                and "landing" in self.notified[_id],
                "track": GenericTrack(max_elements=20),
                # Coarse flight path for the map card (the GenericTrack above
                # only keeps 20 elements for rate averaging).
                "path": [
                    (
                        telemetry["lat"],
                        telemetry["lon"],
                        telemetry["alt"],
                        _telem_time(telemetry),
                    )
                ],
                "path_time": time.time(),
                "last_telem": telemetry,
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
        self.sondes[_id]["last_telem"] = telemetry
        if telemetry["alt"] > self.sondes[_id]["max_alt"]:
            self.sondes[_id]["max_alt"] = telemetry["alt"]

        if (time.time() - self.sondes[_id]["path_time"]) >= self.PATH_MIN_INTERVAL:
            self.sondes[_id]["path"].append(
                (
                    telemetry["lat"],
                    telemetry["lon"],
                    telemetry["alt"],
                    _telem_time(telemetry),
                )
            )
            self.sondes[_id]["path_time"] = time.time()
            if len(self.sondes[_id]["path"]) > 1000:
                self.sondes[_id]["path"] = _downsample(self.sondes[_id]["path"], 500)

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
        """Remove any sondes we haven't heard from recently, and post a
        last-heard ("balloon down") alert for burst sondes that have gone
        quiet - signal is usually lost below the local horizon, so the last
        position + an altitude-scaled search radius is the useful product."""
        _now = time.time()
        for _id in list(self.sondes.keys()):
            _sonde = self.sondes[_id]
            _age = _now - _sonde["last_time"]

            if (
                _sonde.get("burst_notified")
                and not _sonde.get("landing_notified")
                and _age > self.LANDING_SILENCE
            ):
                _sonde["landing_notified"] = True
                if self.burst_notifications:
                    self.log_info(
                        "Sonde %s silent for %.0f min after burst - posting last-heard position."
                        % (_id, _age / 60.0)
                    )
                    try:
                        self.post_landing(_sonde["last_telem"])
                        self._mark_notified(_id, "landing")
                    except Exception as e:
                        self.log_error("Error posting landing alert - %s" % str(e))

            if _age > self.SONDE_MAX_AGE:
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
            # No position - nothing to map, plain post only.
            _lines = [
                "🔒 Encrypted radiosonde detected: %s %s"
                % (self._type_str(telemetry), _id),
                "%s · telemetry is encrypted, no position available"
                % telemetry["freq"],
                "#radiosonde #sondehub #hamradio",
            ]
            self.post("\n".join(_lines))
            return

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

        _descending = telemetry.get("vel_v", 0.0) < -2.0
        _title = (
            "Radiosonde tracked in descent"
            if _descending
            else "Balloon launch detected"
        )
        _desc = "%s radiosonde %s heard on %s, %s." % (
            self._type_str(telemetry),
            strip_sonde_serial(_id),
            telemetry["freq"],
            "descending" if _descending else "ascending",
        )
        if _range:
            _desc = _desc[:-1] + ", %s." % _range
        _params = self._balloon_params(
            telemetry,
            title=_title,
            desc=_desc,
            alt_line="At %s m (%s ft)" % ("{:,}".format(int(telemetry["alt"])), _ft(telemetry["alt"])),
            track=self.sondes.get(_id, {}).get("path"),
        )

        # Remember this post so the later burst event replies into the same thread.
        _ref = self.post_card("\n".join(_lines), _params)
        if _ref:
            if _id in self.sondes:
                self.sondes[_id]["post_ref"] = _ref
            self._remember_post(_id, _ref)

    def post_burst(self, telemetry, sonde_state):
        _id = telemetry["id"]
        _max_alt = self.sondes[_id]["max_alt"]
        _descent = abs(sonde_state["ascent_rate"])
        _lines = [
            "💥 Balloon burst: %s %s" % (self._type_str(telemetry), _id),
            "Peak altitude %s m · now descending %.0f m/s"
            % ("{:,}".format(int(_max_alt)), _descent),
        ]
        _stat = telemetry["freq"]
        _range = self._range_line(telemetry)
        if _range:
            _stat += " · " + _range
        _lines.append(_stat)
        _lines.append("sondehub.org/%s" % strip_sonde_serial(_id))
        _lines.append("#radiosonde #sondehub")

        # Predict the landing point by falling from here through the winds we
        # measured on the way up (the sonde was a wind tracer on ascent). The
        # circle then sits on the PREDICTED landing, tight, instead of being a
        # bay-sized blob centred on the burst point.
        _path = self.sondes.get(_id, {}).get("path")
        _pred = predict_landing(_path, telemetry["lat"], telemetry["lon"], telemetry["alt"])
        if _pred:
            _center = (_pred["lat"], _pred["lon"])
            _drift_nm = _pred["drift_m"] / 1852.0
            # Uncertainty ~15% of the predicted drift (winds evolve since
            # ascent, descent rate is modelled), floored/capped to stay useful.
            _radius_nm = min(8.0, max(1.0, 0.15 * _drift_nm))
            _mins_to_ground = _pred["dur_s"] / 60.0
            _desc = (
                "%s radiosonde %s burst at %s ft. Predicted landing zone from "
                "the ascent winds is marked; the circle is the drift uncertainty."
                % (self._type_str(telemetry), strip_sonde_serial(_id), _ft(_max_alt))
            )
        else:
            # No usable ascent profile - fall back to a modest altitude-scaled
            # circle around the current position (still far tighter than before).
            _center = None
            _radius_nm = min(8.0, max(2.0, (telemetry["alt"] / 1000.0) * 0.25))
            _mins_to_ground = telemetry["alt"] / max(3.0, _descent) / 60.0
            _desc = (
                "%s radiosonde %s burst at %s ft and is descending on parachute. "
                "The circle is a rough landing-zone estimate."
                % (self._type_str(telemetry), strip_sonde_serial(_id), _ft(_max_alt))
            )
        _params = self._balloon_params(
            telemetry,
            title="Balloon burst at %s ft" % _ft(_max_alt),
            desc=_desc,
            alt_line="Burst %s m (%s ft)" % ("{:,}".format(int(_max_alt)), _ft(_max_alt)),
            until="Landing expected ~%.0f min" % _mins_to_ground,
            track=_path,
            radius_nm=_radius_nm,
            center=_center,
        )

        # Thread the burst under this sonde's discovery post (from memory, or the
        # persisted store if the discovery happened before an auto_rx restart).
        _ref = self.sondes.get(_id, {}).get("post_ref") or self._recall_post(_id)
        _reply = {"root": _ref, "parent": _ref} if _ref else None
        self.post_card("\n".join(_lines), _params, reply=_reply)

    def post_landing(self, telemetry):
        _id = telemetry["id"]
        _alt_m = telemetry["alt"]
        _lines = [
            "🛬 Balloon down: %s %s" % (self._type_str(telemetry), _id),
            "Signal lost at %s m - likely below the horizon or on the ground"
            % "{:,}".format(int(_alt_m)),
        ]
        _range = self._range_line(telemetry)
        if _range:
            _lines.append(_range)
        _lines.append("sondehub.org/%s" % strip_sonde_serial(_id))
        _lines.append("#radiosonde #sondehub")

        # By now the sonde is low, so a descent prediction from the last-heard
        # altitude is short-range and tight. Fall back to an altitude-scaled
        # search circle if the ascent profile is too thin.
        _last_local = datetime.datetime.now(STATION_TZ).strftime("%H:%M %Z")
        _path = self.sondes.get(_id, {}).get("path")
        _pred = predict_landing(_path, telemetry["lat"], telemetry["lon"], _alt_m)
        if _pred:
            _center = (_pred["lat"], _pred["lon"])
            _radius_nm = min(6.0, max(0.5, 0.15 * (_pred["drift_m"] / 1852.0)))
            _desc = (
                "%s radiosonde %s - signal lost. Estimated landing/search area "
                "from the last-heard position and the ascent winds is marked."
                % (self._type_str(telemetry), strip_sonde_serial(_id))
            )
        else:
            _center = None
            _radius_nm = min(10.0, max(1.0, (_alt_m / 1000.0) * 1.0))
            _desc = (
                "%s radiosonde %s - last position before signal loss. "
                "The circle is a search-area estimate from the last-heard altitude."
                % (self._type_str(telemetry), strip_sonde_serial(_id))
            )
        _params = self._balloon_params(
            telemetry,
            title="Balloon down - last heard at %s ft" % _ft(_alt_m),
            desc=_desc,
            alt_line="Last heard %s m (%s ft)" % ("{:,}".format(int(_alt_m)), _ft(_alt_m)),
            until="Last heard %s" % _last_local,
            track=_path,
            radius_nm=_radius_nm,
            center=_center,
        )

        _ref = self.sondes.get(_id, {}).get("post_ref") or self._recall_post(_id)
        _reply = {"root": _ref, "parent": _ref} if _ref else None
        self.post_card("\n".join(_lines), _params, reply=_reply)

    # -------------------------------------------------------------- map cards

    def _balloon_params(
        self, telemetry, title, desc, alt_line=None, until=None, track=None,
        radius_nm=None, center=None,
    ):
        """Query params for the nerdymark.com balloon map page + PNG (the two
        endpoints get the IDENTICAL query string). The event marker + any
        radius circle sit at `center` (lat, lon) when given - e.g. a predicted
        landing point offset from the track's end - otherwise at the telemetry
        position. `desc` must stay URL-free: the server strips URL-like tokens
        (including "sondehub.org")."""
        _lat, _lon = center if center else (telemetry["lat"], telemetry["lon"])
        _p = {
            "kind": "balloon",
            "lat": "%.4f" % _lat,
            "lon": "%.4f" % _lon,
        }
        if radius_nm:
            _p["radius_nm"] = "%.1f" % min(500.0, max(0.05, radius_nm))
        _p["id"] = strip_sonde_serial(telemetry["id"])[:40]
        _p["title"] = title[:120]
        _p["desc"] = desc[:600]
        if alt_line:
            _p["alt"] = alt_line[:60]
        if until:
            _p["until"] = until[:60]

        # Fit the flight track into the remaining URL budget (thin it until the
        # whole query fits), so a long flight never overflows the proxy's URL
        # limit. Reserve headroom for the bsky backlink params added later.
        if track and len(track) >= 2:
            _budget = MAX_QUERY_CHARS - len(urlencode(_p)) - len("&track=") - 60
            _val = _fit_track(track, _budget)
            if _val:
                _p["track"] = _val
        return _p

    def _backlink_ok(self):
        _h = (self.handle or "").strip().lower()
        return _h == BACKLINK_DOMAIN or _h.endswith("." + BACKLINK_DOMAIN)

    def _fetch_map(self, query_string):
        """GET the map PNG. Returns (png_bytes, "ok"), (None, "badreq") on a
        400 (our params are malformed), or (None, "unavail") on 429/5xx/
        network trouble. Never retried - the render service has a shared
        daily budget and negative-caches failures."""
        try:
            _r = requests.get(MAP_PNG_URL + "?" + query_string, timeout=MAP_TIMEOUT)
            if _r.status_code == 400:
                return None, "badreq"
            if _r.ok and _r.headers.get("content-type", "").startswith("image/"):
                return _r.content, "ok"
            self.log_error(
                "Map render unavailable (HTTP %s) - card will have no thumbnail."
                % _r.status_code
            )
            return None, "unavail"
        except Exception as e:
            self.log_error(
                "Map render fetch failed (%s) - card will have no thumbnail." % str(e)
            )
            return None, "unavail"

    def post_card(self, text, params, reply=None):
        """Post with a balloon-map link card. Bluesky cards are client-built:
        fetch map.png (same query string as the page URL), upload it as the
        card thumb, create the post. When the handle is on the backlink
        allowlist we pre-generate the post's TID rkey ourselves and bake
        bsky=<rkey> into the page URL, so the page's "View on Bluesky" link
        points at the very post carrying it. Failure ladder (an alert must
        never be lost to the map service): map.png 400 -> plain post;
        429/5xx/network -> card without thumb; anything else -> plain post."""
        _rkey = None
        if self._backlink_ok():
            _rkey = _tid()
            params = dict(params, bsky=_rkey, bsky_handle=self.handle)
        _qs = urlencode(params)
        _page_url = MAP_PAGE_URL + "?" + _qs

        try:
            _png, _status = self._fetch_map(_qs)
            if _status == "badreq":
                self.log_error(
                    "Map render rejected our params (HTTP 400) - posting without card."
                )
                return self.post(text, reply=reply)

            _thumb = None
            if _png:
                try:
                    _thumb = self._upload_blob(_png, "image/png")
                except Exception as e:
                    self.log_error("Card thumbnail upload failed - %s" % str(e))

            _external = {
                "uri": _page_url,
                "title": "🎈 %s - %s" % (params.get("id", ""), params.get("title", "")),
                "description": params.get("desc", ""),
            }
            if _thumb:
                _external["thumb"] = _thumb
            _embed = {"$type": "app.bsky.embed.external", "external": _external}
            return self.post(text, reply=reply, embed=_embed, rkey=_rkey)
        except Exception as e:
            self.log_error("Map card build failed (%s) - posting without card." % str(e))
            return self.post(text, reply=reply)

    # ------------------------------------------------------------------ xrpc

    def _get_session(self, force=False):
        """Return a cached atproto session, logging in only when there is
        none, it has aged out, or force=True. createSession is rate-limited
        per account, so it must not run per post."""
        if (
            not force
            and self._session
            and (time.time() - self._session_time) < self.SESSION_MAX_AGE
        ):
            return self._session
        _r = requests.post(
            self.pds_url + "/xrpc/com.atproto.server.createSession",
            json={"identifier": self.handle, "password": self.app_password},
            timeout=30,
        )
        _r.raise_for_status()
        self._session = _r.json()
        self._session_time = time.time()
        return self._session

    def _upload_blob(self, data, mime):
        _session = self._get_session()
        _r = requests.post(
            self.pds_url + "/xrpc/com.atproto.repo.uploadBlob",
            headers={
                "Authorization": "Bearer " + _session["accessJwt"],
                "Content-Type": mime,
            },
            data=data,
            timeout=30,
        )
        if _r.status_code in (400, 401):
            _session = self._get_session(force=True)
            _r = requests.post(
                self.pds_url + "/xrpc/com.atproto.repo.uploadBlob",
                headers={
                    "Authorization": "Bearer " + _session["accessJwt"],
                    "Content-Type": mime,
                },
                data=data,
                timeout=30,
            )
        _r.raise_for_status()
        return _r.json()["blob"]

    def post(self, text, reply=None, embed=None, rkey=None):
        """Post text to Bluesky. `reply`, if given, is an atproto reply ref
        ({"root": {...}, "parent": {...}}) that threads this post under another.
        `embed` is an optional app.bsky.embed.* dict (e.g. an external link
        card); `rkey` an optional pre-generated TID record key. Returns the
        created post's {"uri","cid"} (a strong ref usable to reply to it
        later), or None on failure. Failures are logged, never raised - a
        Bluesky outage must not affect telemetry processing."""
        _since_last = time.time() - self.last_post_time
        if _since_last < self.MIN_POST_INTERVAL:
            time.sleep(self.MIN_POST_INTERVAL - _since_last)

        try:
            _record = {
                "$type": "app.bsky.feed.post",
                "text": text,
                "facets": _facets(text),
                "createdAt": datetime.datetime.now(datetime.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
            }
            if reply:
                _record["reply"] = reply
            if embed:
                _record["embed"] = embed

            def _create(_session):
                _body = {
                    "repo": _session["did"],
                    "collection": "app.bsky.feed.post",
                    "record": _record,
                }
                if rkey:
                    _body["rkey"] = rkey
                return requests.post(
                    self.pds_url + "/xrpc/com.atproto.repo.createRecord",
                    headers={"Authorization": "Bearer " + _session["accessJwt"]},
                    json=_body,
                    timeout=30,
                )

            _resp = _create(self._get_session())
            if _resp.status_code in (400, 401):
                # Likely an expired access token - one fresh login, one retry.
                _resp = _create(self._get_session(force=True))
            _resp.raise_for_status()
            _resp = _resp.json()
            self.last_post_time = time.time()
            self.log_info("Posted: %s" % text.splitlines()[0])
            if _resp.get("uri") and _resp.get("cid"):
                return {"uri": _resp["uri"], "cid": _resp["cid"]}
        except Exception as e:
            self.log_error("Error posting to Bluesky - %s" % str(e))
        return None

    # ------------------------------------------------------------ persistence

    def _load_notified(self):
        try:
            with open(self.notified_file, "r") as f:
                _data = json.load(f)
            _cutoff = time.time() - self.NOTIFIED_TTL
            # Keep an entry while any of its event *timestamps* is fresh. Non-numeric
            # values (e.g. the "discovery_post" ref dict) are ignored for the cutoff.
            out = {}
            for _id, _events in _data.items():
                _ts = [v for v in _events.values() if isinstance(v, (int, float))]
                if _ts and max(_ts) > _cutoff:
                    out[_id] = _events
            return out
        except FileNotFoundError:
            return {}
        except Exception as e:
            self.log_error("Could not read notified-sondes file - %s" % str(e))
            return {}

    def _save_notified(self):
        try:
            with open(self.notified_file, "w") as f:
                json.dump(self.notified, f)
        except Exception as e:
            self.log_error("Could not write notified-sondes file - %s" % str(e))

    def _mark_notified(self, _id, event):
        if _id not in self.notified:
            self.notified[_id] = {}
        self.notified[_id][event] = time.time()
        self._save_notified()

    def _remember_post(self, _id, ref):
        """Persist a sonde's discovery-post ref so a later burst reply threads to
        it even across an auto_rx restart."""
        if _id not in self.notified:
            self.notified[_id] = {}
        self.notified[_id]["discovery_post"] = ref
        self._save_notified()

    def _recall_post(self, _id):
        return self.notified.get(_id, {}).get("discovery_post")

    # ----------------------------------------------------------------- logging

    def log_debug(self, line):
        logging.debug("Bluesky - %s" % line)

    def log_info(self, line):
        logging.info("Bluesky - %s" % line)

    def log_error(self, line):
        logging.error("Bluesky - %s" % line)


if __name__ == "__main__":
    # Test post: python -m autorx.bluesky <handle> <app_password>
    # Posts a synthetic burst alert with a full map card (flight track +
    # landing-zone circle) to verify the nerdymark.com card flow end-to-end.
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
    _telem = {
        "id": "TEST0000001",
        "lat": 37.9013,
        "lon": -121.6521,
        "alt": 9514.0,
        "type": "RS41",
        "freq": "404.200 MHz",
    }
    _params = _bsky._balloon_params(
        _telem,
        title="Balloon burst at 31,214 ft (test)",
        desc="radiosonde_auto_rx map-card TEST post. Not a real flight.",
        alt_line="Burst 9,514 m (31,214 ft)",
        until="Landing expected ~25 min",
        track=[
            (37.7358, -122.2219),
            (37.7702, -122.1050),
            (37.8021, -121.9810),
            (37.8555, -121.8102),
            (37.9013, -121.6521),
        ],
        radius_nm=3.0,
    )
    _bsky.post_card(
        "💥 Balloon burst (map-card TEST): RS41 TEST0000001\n"
        "Peak altitude 9,514 m · now descending 12 m/s\n"
        "404.200 MHz\n#radiosonde #sondehub",
        _params,
    )
    _bsky.close()
