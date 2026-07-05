"""Weather advisory over two chained Open-Meteo APIs — no API key required.

    cities.csv ─▶ geocode ─▶ forecast ─▶ advice ─▶ briefing (reduce)
                  (lookup)   (stale 3h)  (index)

Real APIs, both public and keyless:
  - Open-Meteo geocoding (city name -> lat/lon)
  - Open-Meteo forecast (lat/lon -> daily temps + precipitation)

Run it out of the box:

    uv run python examples/weather_advisory/weather_advisory.py

`forecast` carries stale_after="3h": a cached forecast older than three hours
re-fetches on the next run. If the numbers changed, downstream advice recomputes;
if they're identical, the old generation just has its clock refreshed.
"""

import json
import urllib.parse
import urllib.request

from rubedo import Filtered, describe, pipeline, run, step

GEOCODE = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST = "https://api.open-meteo.com/v1/forecast"


def _get(url: str, params: dict):
    q = urllib.parse.urlencode(params)
    with urllib.request.urlopen(f"{url}?{q}", timeout=15) as r:
        return json.load(r)


@step(name="geocode", version="1", retries=3, retry_delay=1, rate_limit="60/min")
def geocode(row: dict) -> dict:
    """City name -> coordinates. Unknown cities decline the lane."""
    hits = _get(GEOCODE, {"name": row["city"], "count": 1}).get("results")
    if not hits:
        return Filtered(f"no such place: {row['city']!r}")
    h = hits[0]
    return {"city": h["name"], "country": h.get("country", ""),
            "lat": h["latitude"], "lon": h["longitude"]}


@step(
    name="forecast",
    version="1",
    depends_on=["geocode"],
    stale_after="3h",  # weather goes stale — re-fetch past this TTL
    retries=3,
    rate_limit="60/min",
)
def forecast(geocode: dict) -> dict:
    """Coordinates -> tomorrow's high/low and precipitation."""
    data = _get(FORECAST, {
        "latitude": geocode["lat"], "longitude": geocode["lon"],
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "auto", "forecast_days": 1,
    })
    daily = data["daily"]
    return {
        "city": geocode["city"], "country": geocode["country"],
        "tmax": daily["temperature_2m_max"][0],
        "tmin": daily["temperature_2m_min"][0],
        "precip": daily["precipitation_sum"][0],
    }


@step(name="advice", version="1", depends_on=["forecast"], index=["outlook"])
def advice(forecast: dict) -> dict:
    """Turn the numbers into a one-word outlook and a suggestion."""
    if forecast["precip"] >= 1:
        outlook, tip = "wet", "take an umbrella"
    elif forecast["tmax"] >= 28:
        outlook, tip = "hot", "stay hydrated"
    elif forecast["tmax"] <= 5:
        outlook, tip = "cold", "bundle up"
    else:
        outlook, tip = "mild", "enjoy it"
    return {**forecast, "outlook": outlook, "tip": tip}


@step(name="briefing", version="1", depends_on=["advice"], shape="reduce")
def briefing(advice: dict) -> str:
    """Fan every city's advice into one morning briefing."""
    rows = sorted(advice.values(), key=lambda a: a["tmax"], reverse=True)
    lines = [
        f"{a['city']:<12} {a['tmax']:>5.1f}°C / {a['tmin']:>5.1f}°C  "
        f"[{a['outlook']}] — {a['tip']}"
        for a in rows
    ]
    return "Morning weather briefing:\n" + "\n".join(lines)


def make_pipeline():
    from rubedo import CsvSource
    import os

    return pipeline(
        id="weather-advisory",
        name="Weather Advisory",
        source=CsvSource(os.path.join(os.path.dirname(__file__), "cities.csv"), key="city"),
        steps=[geocode, forecast, advice, briefing],
    )


def main():
    pipe = make_pipeline()
    print(describe(pipe))
    print()
    summary = run(pipe)
    print(f"created={summary.created_count} reused={summary.reused_count} "
          f"filtered={summary.filtered_count}")


if __name__ == "__main__":
    main()
