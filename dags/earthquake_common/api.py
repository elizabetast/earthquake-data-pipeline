"""USGS FDSN API: UTC, ограниченные запросы, валидация и разбиение окон."""
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"
MAX_EVENTS = 20000


def iso(value):
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def validate(payload):
    if not isinstance(payload, dict) or payload.get("type") != "FeatureCollection":
        raise ValueError("USGS: ожидался GeoJSON FeatureCollection")
    features = payload.get("features")
    if not isinstance(features, list):
        raise ValueError("USGS: отсутствует features")
    for feature in features:
        if not isinstance(feature, dict) or not feature.get("id"):
            raise ValueError("USGS: событие без id")
        props = feature.get("properties") or {}
        if not isinstance(props.get("updated"), (int, float)):
            raise ValueError("USGS: событие без времени обновления")
    return features


def request(params):
    url = ENDPOINT + "?" + urlencode(params)
    req = Request(url, headers={"User-Agent": "LR3-Earthquake-Educational-Pipeline/1.0"})
    for attempt in range(3):
        try:
            with urlopen(req, timeout=60) as response:
                # При отсутствии событий USGS может вернуть HTTP 204 без тела.
                payload = ({"type": "FeatureCollection", "features": [], "_http_status": 204}
                           if response.status == 204 else json.load(response))
            validate(payload)
            return url, payload, datetime.now(timezone.utc)
        except (URLError, TimeoutError) as exc:
            if isinstance(exc, HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 2:
                raise
            logging.getLogger(__name__).warning("USGS: повтор HTTP-запроса после %s", exc)
            time.sleep(2 ** (attempt + 1))



def fetch_window(start, end, updated_after=None, requester=request):
    params = {"format": "geojson", "starttime": iso(start), "endtime": iso(end),
              "orderby": "time-asc", "limit": MAX_EVENTS}
    if updated_after is not None:
        params["updatedafter"] = iso(updated_after)
    url, payload, fetched_at = requester(params)
    features = validate(payload)
    if len(features) >= MAX_EVENTS:
        if end - start <= timedelta(seconds=1):
            raise ValueError("USGS: слишком много событий в секундном окне")
        middle = start + (end - start) / 2
        # Границы включительные, дубли на границе устраняет UNIQUE в STG.
        yield from fetch_window(start, middle, updated_after, requester)
        yield from fetch_window(middle, end, updated_after, requester)
    else:
        yield url, params, payload, fetched_at


def fetch_range(start, end, updated_after=None):
    # Первичную историю дробим по дням, изменения обычно умещаются в один ответ.
    cursor = start
    step = end - start if updated_after else timedelta(days=1)
    while cursor < end:
        boundary = min(cursor + step, end)
        yield from fetch_window(cursor, boundary, updated_after)
        cursor = boundary
