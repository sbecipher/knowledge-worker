from datetime import datetime, timezone

import pyarrow.parquet as pq

from price_lake import PRICE_EOD_COLUMNS, canonical_price_eod_rows, write_price_eod_parquet


def test_canonical_price_eod_rows_preserve_instrument_and_optional_universe_key() -> None:
    rows = canonical_price_eod_rows(
        [
            {
                "date": "2026-04-02",
                "ticker": "AA",
                "instrument": "AA.N",
                "close": 64.08,
                "price_52wk_high_flg_1d": True,
            }
        ],
        context={
            "bar_granularity": "day",
            "requested_period": "day",
            "effective_start_date": "2026-04-02",
            "effective_end_date": "2026-04-02",
            "provider": "lseg",
            "source_dataset": "prices",
            "workflow_id": "wf-123",
            "workflow_run_id": "run-123",
            "request_id": "req-123",
        },
        processed_at=datetime(2026, 4, 2, tzinfo=timezone.utc),
    )

    assert list(rows[0]) == PRICE_EOD_COLUMNS
    assert rows[0]["instrument"] == "AA.N"
    assert rows[0]["universe_key"] is None
    assert rows[0]["granularity"] == "day"
    assert rows[0]["price_52wk_high_flag_1d"] is True
    assert rows[0]["record_hash"]


def test_write_price_eod_parquet_uses_canonical_schema(tmp_path) -> None:
    rows = canonical_price_eod_rows(
        [{"date": "2026-04-02", "ticker": "AA", "instrument": "AA.N"}],
        context={
            "bar_granularity": "day",
            "effective_start_date": "2026-04-02",
            "effective_end_date": "2026-04-02",
            "source_dataset": "prices",
        },
        processed_at=datetime(2026, 4, 2, tzinfo=timezone.utc),
    )
    path = tmp_path / "part-00000-test.snappy.parquet"

    write_price_eod_parquet(path, rows)

    parquet_file = pq.ParquetFile(path)
    assert parquet_file.schema_arrow.names == PRICE_EOD_COLUMNS
    assert parquet_file.metadata.num_rows == 1
