import os
import random
import uuid
from datetime import datetime, timedelta
import tempfile

try:
    import pandas as pd
except Exception as e:
    raise RuntimeError("Missing dependency 'pandas'. Install with 'pip install pandas'") from e

try:
    from faker import Faker
except Exception as e:
    raise RuntimeError("Missing dependency 'faker'. Install with 'pip install faker'") from e

# MinIO is imported lazily inside upload_to_minio to allow generating events without MinIO installed


def generate_events_for_date(process_date: str, n_rows: int = 1_000_000) -> pd.DataFrame:
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    base_date = datetime.strptime(process_date, "%Y-%m-%d")
    start = datetime.combine(base_date, datetime.min.time())
    end = start + timedelta(days=1)

    data = []
    for _ in range(n_rows):
        event_id = str(uuid.uuid4())
        user_id = random.randint(1, 100_000)
        event_type = random.choices(["view", "click", "purchase"], weights=[0.7, 0.2, 0.1])[0]
        product_id = random.randint(1, 5_000)
        timestamp = fake.date_time_between_dates(datetime_start=start, datetime_end=end)
        amount = 0.0
        if event_type == "purchase":
            amount = round(random.uniform(5.0, 500.0), 2)

        data.append(
            {
                "event_id": event_id,
                "user_id": user_id,
                "event_type": event_type,
                "product_id": product_id,
                "event_timestamp": timestamp.isoformat(),
                "amount": amount,
            }
        )

    df = pd.DataFrame(data)
    return df


def upload_to_minio(df: pd.DataFrame, process_date: str):
    """Write DataFrame to a temp CSV and upload to MinIO, cleaning up the temp file."""
    with tempfile.NamedTemporaryFile(prefix=f"events_{process_date}_", suffix=".csv", delete=False) as tmp:
        csv_path = tmp.name
    try:
        df.to_csv(csv_path, index=False)

        try:
            from minio import Minio
        except Exception as e:
            raise RuntimeError("Missing dependency 'minio'. Install with 'pip install minio'") from e

        client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )

        bucket = "bronze"
        object_name = f"raw/events/date={process_date}/events_{process_date}.csv"

        # create bucket if it doesn't exist
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
        except Exception:
            # some MinIO versions/permissions may raise; allow fput_object to handle error
            pass

        client.fput_object(bucket_name=bucket, object_name=object_name, file_path=csv_path)
    finally:
        try:
            os.remove(csv_path)
        except Exception:
            pass


def main(process_date: str):
    df = generate_events_for_date(process_date)
    upload_to_minio(df, process_date)


if __name__ == "__main__":
    date_str = os.environ.get("PROCESS_DATE", datetime.today().strftime("%Y-%m-%d"))
    main(date_str)
