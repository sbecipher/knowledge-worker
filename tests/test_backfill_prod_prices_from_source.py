from scripts.backfill_prod_prices_from_source import (
    SourcePriceObjectInfo,
    _flatten_prod_rows,
    destination_object_path,
    output_workflow_id,
    parse_source_object_path,
)


def test_parse_source_object_path_extracts_partitions_and_source_workflow_id() -> None:
    info = parse_source_object_path(
        "source/prices/granularity=day/date=2026-04-07/ticker=AA/wf-123.ndjson",
        "12345",
    )

    assert info == SourcePriceObjectInfo(
        object_path="source/prices/granularity=day/date=2026-04-07/ticker=AA/wf-123.ndjson",
        ticker="AA",
        date="2026-04-07",
        granularity="day",
        source_workflow_id="wf-123",
        generation="12345",
    )


def test_output_workflow_id_modes_always_differ_from_source_workflow_id() -> None:
    info = SourcePriceObjectInfo(
        object_path="source/prices/granularity=day/date=2026-04-07/ticker=AA/wf-123.ndjson",
        ticker="AA",
        date="2026-04-07",
        granularity="day",
        source_workflow_id="wf-123",
        generation="12345",
    )

    derived = output_workflow_id(
        object_info=info,
        mode="derived",
        run_workflow_id="run-123",
        explicit_workflow_id=None,
        suffix="prod",
    )
    suffixed = output_workflow_id(
        object_info=info,
        mode="suffix",
        run_workflow_id="run-123",
        explicit_workflow_id=None,
        suffix="prod",
    )
    run_based = output_workflow_id(
        object_info=info,
        mode="run",
        run_workflow_id="prices_prod_backfill_20260409T120000Z",
        explicit_workflow_id=None,
        suffix="prod",
    )
    explicit = output_workflow_id(
        object_info=info,
        mode="explicit",
        run_workflow_id="run-123",
        explicit_workflow_id="manual-backfill",
        suffix="prod",
    )

    assert derived.startswith("prices_prod_")
    assert suffixed == "wf-123__prod"
    assert run_based.startswith("prices_prod_backfill_20260409T120000Z__")
    assert explicit.startswith("manual-backfill__")
    assert all(value != "wf-123" for value in [derived, suffixed, run_based, explicit])


def test_destination_object_path_uses_prod_prices_layout() -> None:
    info = SourcePriceObjectInfo(
        object_path="source/prices/granularity=day/date=2026-04-07/ticker=AA/wf-123.ndjson",
        ticker="AA",
        date="2026-04-07",
        granularity="day",
        source_workflow_id="wf-123",
        generation="12345",
    )

    path = destination_object_path(object_info=info, workflow_id="wf-123__prod", gcs_prefix="")

    assert path == "prod/prices/granularity=day/date=2026-04-07/ticker=AA/wf-123__prod.ndjson"


def test_flatten_prod_rows_uses_source_prices_rows_and_new_workflow_id() -> None:
    info = SourcePriceObjectInfo(
        object_path="source/prices/granularity=day/date=2026-04-07/ticker=AA/wf-123.ndjson",
        ticker="AA",
        date="2026-04-07",
        granularity="day",
        source_workflow_id="wf-123",
        generation="12345",
    )
    source_rows = [
        {
            "ticker": "AA",
            "requested_period": "day",
            "as_of_date": "2026-04-07",
            "effective_start_date": "2026-04-07",
            "effective_end_date": "2026-04-07",
            "bar_granularity": "day",
            "universe_key": "mmh5r1",
            "workflow_id": "wf-123",
            "workflow_run_id": "run-123",
            "request_id": "req-123",
            "source_system": "marketio",
            "provider": "lseg",
            "frequency": "daily",
            "source": "lseg",
            "ric": "AA.N",
            "primary_ric": "AA.N",
            "cik_number": "0001675149",
            "organization_id": "4295904304",
            "date": "2026-04-07",
            "instrument": "AA.N",
            "fields": {
                "TR.CLOSEPRICE": 64.08,
                "TR.TotalReturn1D": 3.22164948453608,
            },
        }
    ]

    rows = _flatten_prod_rows(
        source_rows=source_rows,
        object_info=info,
        workflow_id="wf-123__prod",
        request_id="req-backfill",
        universe_key=None,
    )

    assert rows == [
        {
            "ticker": "AA",
            "requested_period": "day",
            "as_of_date": "2026-04-07",
            "effective_start_date": "2026-04-07",
            "effective_end_date": "2026-04-07",
            "bar_granularity": "day",
            "universe_key": "mmh5r1",
            "workflow_id": "wf-123__prod",
            "workflow_run_id": "wf-123__prod",
            "request_id": "req-backfill",
            "source_system": "source_prices_backfill",
            "provider": "lseg",
            "frequency": "daily",
            "ric": "AA.N",
            "primary_ric": "AA.N",
            "organization_id": "4295904304",
            "cik_number": "0001675149",
            "source_uri": None,
            "source_object_path": "source/prices/granularity=day/date=2026-04-07/ticker=AA/wf-123.ndjson",
            "source_dataset": "prices",
            "transform_name": "prices_prod_transform",
            "transform_version": "v1",
            "date": "2026-04-07",
            "instrument": "AA.N",
            "open": None,
            "close": 64.08,
            "high": None,
            "low": None,
            "volume": None,
            "avg_monthly_volume_3m": None,
            "avg_daily_val_traded_52w": None,
            "price_52wk_high": None,
            "price_52wk_low": None,
            "total_return_52wk": None,
            "total_return_1d": 3.22164948453608,
            "rel_price_pct_chg_1d": None,
            "price_pct_chg_1d": None,
            "price_avg_pct_diff_2d": None,
            "price_pct_chg_120d": None,
            "price_pct_chg_13w": None,
            "price_pct_chg_26w": None,
            "price_pct_chg_52w": None,
            "price_pct_chg_2d": None,
            "price_pct_chg_wtd": None,
            "price_pct_chg_qtd": None,
            "price_net_chg_2d": None,
            "price_net_chg_10d": None,
            "price_net_chg_20d": None,
            "price_net_chg_30d": None,
            "price_net_chg_50d": None,
            "price_net_chg_90d": None,
            "price_mo_volatility_dly": None,
            "price_mo_volatility_t12m": None,
            "price_target_mean": None,
            "price_to_mean_price_target": None,
            "fwd_pto_eps_smart_est": None,
            "pto_eps_mean_est": None,
            "peg_smart": None,
            "pto_ebt_smart_est": None,
            "price_mo_region_rank": None,
            "price_to_book_value_per_shr": None,
            "pto_bps_smart_est": None,
            "pto_cpx_smart_est": None,
            "price_avg_2d": None,
            "price_avg_5d": None,
            "price_avg_10d": None,
            "price_avg_30d": None,
            "price_50_day_average": None,
            "price_avg_100d": None,
            "price_150_day_average": None,
            "price_200_day_average": None,
            "price_52wk_high_flg_1d": None,
            "price_52wk_high_flg_5d": None,
            "price_52wk_low_flg_1d": None,
            "price_52wk_low_flg_5d": None,
            "price_chg": 67.30164948453609,
            "price_pct_chg": 0.0322164948453608,
        }
    ]
