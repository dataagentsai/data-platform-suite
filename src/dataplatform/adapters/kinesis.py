"""stream_bus on Amazon Kinesis Data Streams (cloud layer, dbs/cloud/aws.yaml). Needs boto3; the client is passed
in, so tests use a local stand-in (moto) and the platform passes one built from the job's role.

Kinesis assigns shard ids and sequence numbers itself. A sequence number is a large decimal that only grows within
a shard, so it is the reader's position as is (StreamRecord.seq). The arrival time the world declares travels inside
the record, beside the data, because Kinesis stamps its own arrival time.
"""
from __future__ import annotations

import json
import time
from typing import Any

from dataplatform.source_ports import Row, StreamRecord

PUT_BATCH = 500        # PutRecords limit per call
MAX_BACKOFF_S = 2.0    # a shard takes 1,000 records or 1 MB a second; throttled entries are retried after a pause


class KinesisStream:
    def __init__(self, client: Any, name_prefix: str = ""):
        self.k, self.name_prefix = client, name_prefix

    def _name(self, stream: str) -> str:
        return f"{self.name_prefix}{stream}"

    def append(self, stream: str, records: list[tuple[str, str, Row]]) -> None:
        # One key's records keep their order: they share a shard, and PutRecords keeps request order per shard
        # only among successful entries, so a partial failure is retried from the first failed entry onwards.
        pending = [{"PartitionKey": key, "Data": json.dumps({"arrived_at": at, "data": data}, separators=(",", ":"))
                    .encode()} for key, at, data in records]
        pause = 0.0
        while pending:
            batch, pending = pending[:PUT_BATCH], pending[PUT_BATCH:]
            out = self.k.put_records(StreamName=self._name(stream), Records=batch)
            if out.get("FailedRecordCount"):
                first = next(i for i, r in enumerate(out["Records"]) if "ErrorCode" in r)
                pending = batch[first:] + pending
                pause = min(MAX_BACKOFF_S, max(0.05, pause * 2))
                time.sleep(pause)
            else:
                pause = 0.0

    def shards(self, stream: str) -> list[str]:
        out: list[str] = []
        kwargs: dict[str, Any] = {"StreamName": self._name(stream)}
        while True:
            page = self.k.list_shards(**kwargs)
            out += [s["ShardId"] for s in page["Shards"]]
            if not page.get("NextToken"):
                return sorted(out)
            kwargs = {"NextToken": page["NextToken"]}

    def read(self, stream: str, shard: str, after_seq: int, limit: int) -> list[StreamRecord]:
        start: dict[str, Any] = {"ShardIteratorType": "TRIM_HORIZON"}
        if after_seq > 0:
            start = {"ShardIteratorType": "AFTER_SEQUENCE_NUMBER", "StartingSequenceNumber": str(after_seq)}
        it = self.k.get_shard_iterator(StreamName=self._name(stream), ShardId=shard, **start)["ShardIterator"]
        out: list[StreamRecord] = []
        while it and len(out) < limit:
            page = self.k.get_records(ShardIterator=it, Limit=min(10_000, limit - len(out)))
            for r in page["Records"]:
                body = json.loads(r["Data"])
                out.append(StreamRecord(shard, int(r["SequenceNumber"]), body["arrived_at"], body["data"]))
            if not page["Records"] or page.get("MillisBehindLatest", 0) == 0:
                break
            it = page.get("NextShardIterator")
        return out[:limit]
