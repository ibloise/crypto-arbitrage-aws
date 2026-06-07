from typing import Any, Iterable

from .contracts import tick_to_kinesis_record


def publish_ticks(
    client: Any,
    stream_name: str,
    ticks: Iterable[dict[str, Any]],
    max_attempts: int = 3,
) -> int:
    records = [tick_to_kinesis_record(tick) for tick in ticks]

    for index in range(0, len(records), 500):
        pending = records[index:index + 500]
        for _attempt in range(max_attempts):
            response = client.put_records(StreamName=stream_name, Records=pending)
            failed_count = response.get("FailedRecordCount", 0)
            if not failed_count:
                break

            results = response.get("Records", [])
            pending = [
                record
                for record, result in zip(pending, results)
                if result.get("ErrorCode")
            ]
            if len(pending) != failed_count:
                raise RuntimeError(f"Kinesis rejected {failed_count} records")
        else:
            raise RuntimeError(f"Kinesis rejected {len(pending)} records after retries")

    return len(records)
