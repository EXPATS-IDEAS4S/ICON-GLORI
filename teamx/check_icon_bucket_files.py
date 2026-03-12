import io
from datetime import datetime, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import xarray as xr

from credentials_buckets import (
    S3_ACCESS_KEY,
    S3_BUCKET_ICON,
    S3_ENDPOINT_URL,
    S3_SECRET_ACCESS_KEY,
)


# Settings
START_DATE = "2025-04-01"
END_DATE = "2025-09-30"
BASENAME = "merged_SYNMSG_BT_CL_IR10.8_CLCT"
INIT_HOURS = ["00"]
OUTPUT_REPORT = "icon_bucket_check_report.txt"
FULL_LOAD = False
EXPECTED_VARS = ["SYNMSG_BT_CL_IR10.8", "CLCT"]
EXPECTED_TIMESTAMPS = 48  # 48 hourly steps


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(
            connect_timeout=10,
            read_timeout=120,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def iter_dates(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_filename(basename, day, init_hour):
    return f"{basename}_{day:%Y%m%d}_{init_hour}.nc"


def check_exists_and_openable(s3, bucket, key, full_load=False,
                              expected_vars=None, expected_timestamps=None):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        if error_code in {"NoSuchKey", "404"}:
            return "missing", f"missing: {key}"
        return "error", f"s3-error: {key} ({error_code})"

    try:
        payload = obj["Body"].read()
    except Exception as exc:  # noqa: BLE001
        return "corrupt", f"read-failed: {key} ({exc})"

    issues = []
    try:
        ds = xr.open_dataset(io.BytesIO(payload))
        try:
            _ = dict(ds.dims)
            data_vars = list(ds.data_vars)
            if full_load:
                ds.load()

            if expected_vars:
                missing_vars = [v for v in expected_vars if v not in data_vars]
                if missing_vars:
                    issues.append(f"missing-vars:{missing_vars}")

            if expected_timestamps is not None:
                n_times = ds.dims.get("time", None)
                if n_times is None:
                    issues.append("no-time-dim")
                elif n_times != expected_timestamps:
                    issues.append(f"timestamps:{n_times}/{expected_timestamps}")

        finally:
            ds.close()
    except Exception as exc:  # noqa: BLE001
        return "corrupt", f"open-failed: {key} ({exc})"

    if issues:
        return "incomplete", f"incomplete: {key} ({', '.join(issues)})"
    return "ok", f"ok: {key}"


def main():
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d").date()
    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")

    s3 = build_s3_client()

    report_lines = []
    counts = {"ok": 0, "missing": 0, "corrupt": 0, "error": 0, "incomplete": 0}

    for day in iter_dates(start_date, end_date):
        for init_hour in INIT_HOURS:
            key = build_filename(BASENAME, day, init_hour)
            status, line = check_exists_and_openable(
                s3=s3,
                bucket=S3_BUCKET_ICON,
                key=key,
                full_load=FULL_LOAD,
                expected_vars=EXPECTED_VARS,
                expected_timestamps=EXPECTED_TIMESTAMPS,
            )
            counts[status] += 1
            print(line)
            report_lines.append(line)

    summary = (
        f"summary: ok={counts['ok']} missing={counts['missing']} "
        f"incomplete={counts['incomplete']} corrupt={counts['corrupt']} error={counts['error']}"
    )
    print(summary)
    report_lines.append(summary)

    if OUTPUT_REPORT:
        with open(OUTPUT_REPORT, "w", encoding="utf-8") as handle:
            handle.write("\n".join(report_lines) + "\n")
        print(f"report-saved: {OUTPUT_REPORT}")


if __name__ == "__main__":
    main()