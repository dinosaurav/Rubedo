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

import os
from rubedo import Filtered, pipeline


GEOCODE = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST = "https://api.open-meteo.com/v1/forecast"


def _get(url: str, params: dict):
    q = urllib.parse.urlencode(params)
    with urllib.request.urlopen(f"{url}?{q}", timeout=15) as r:
        return json.load(r)


p = pipeline(name="weather-advisory")


@p.step
def cities():
    import csv
    with open(os.path.join(os.path.dirname(__file__), "cities.csv")) as f:
        for row in csv.DictReader(f):
            yield row


@p.step(retries=3, retry_delay=1, rate_limit="60/min")
def geocode(cities: dict) -> dict | Filtered:
    """City name -> coordinates. Unknown cities decline the lane."""
    row = cities
    hits = _get(GEOCODE, {"name": row["city"], "count": 1}).get("results")
    if not hits:
        return Filtered(f"no such place: {row['city']!r}")
    h = hits[0]
    return {"city": h["name"], "country": h.get("country", ""),
            "lat": h["latitude"], "lon": h["longitude"]}


@p.step(
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


@p.step
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


@p.step(shape="reduce")
def briefing(advice: dict) -> str:
    """Fan every city's advice into one morning briefing."""
    rows = sorted(advice.values(), key=lambda a: a["tmax"], reverse=True)
    lines = [
        f"{a['city']:<12} {a['tmax']:>5.1f}°C / {a['tmin']:>5.1f}°C  "
        f"[{a['outlook']}] — {a['tip']}"
        for a in rows
    ]
    return "Morning weather briefing:\n" + "\n".join(lines)


def main():
    print(p.describe())
    print()
    summary = p.run()
    print(
        f"created={summary.created_count} reused={summary.reused_count} "
        f"filtered={summary.filtered_count}"
    )
    print("\n--- Final Output (briefing) ---")
    import json
    print(json.dumps(summary.output_for("briefing"), indent=2, default=str))


if __name__ == "__main__":
    main()
