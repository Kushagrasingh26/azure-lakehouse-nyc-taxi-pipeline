import os
import random
from datetime import datetime, timedelta

import pandas as pd


def _rand_dt(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def generate_sample_csv(out_path: str, n_rows: int = 50_000) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 31)

    zones = [f"Zone_{i:03d}" for i in range(1, 101)]
    vendors = [1, 2]
    rate_codes = [1, 2, 3, 4, 5, 6]

    rows = []
    for _ in range(n_rows):
        pickup = _rand_dt(start, end)
        duration_min = random.randint(1, 60)
        dropoff = pickup + timedelta(minutes=duration_min)

        trip_distance = round(max(0.1, random.gauss(3.2, 2.1)), 2)
        fare_amount = round(max(2.5, trip_distance * random.uniform(1.8, 3.5)), 2)
        tip_amount = round(max(0.0, random.gauss(1.0, 2.0)), 2)
        total_amount = round(fare_amount + tip_amount + random.uniform(0.0, 2.0), 2)

        rows.append(
            {
                "vendor_id": random.choice(vendors),
                "tpep_pickup_datetime": pickup.isoformat(sep=" "),
                "tpep_dropoff_datetime": dropoff.isoformat(sep=" "),
                "passenger_count": random.randint(1, 4),
                "trip_distance": trip_distance,
                "pu_zone": random.choice(zones),
                "do_zone": random.choice(zones),
                "rate_code_id": random.choice(rate_codes),
                "payment_type": random.choice([1, 2, 3, 4]),
                "fare_amount": fare_amount,
                "tip_amount": tip_amount,
                "total_amount": total_amount,
            }
        )

    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    print(f"✅ Generated sample CSV: {out_path} (rows={len(df)})")


if __name__ == "__main__":
    generate_sample_csv("data/sample_nyc_taxi.csv", n_rows=50_000)
