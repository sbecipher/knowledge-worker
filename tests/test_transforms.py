from transforms.fundamentals import prod_fundamentals_data
from transforms.prices import prod_prices_data


def test_prod_fundamentals_data_pivots_long_form_rows() -> None:
    payload = {
        "data": [
            {
                "instrument": "AA.N",
                "name": "TR.F.TotRevenue",
                "description": "Total Revenue",
                "period_start_date": "2025-01-01",
                "period_end_date": "2025-03-31",
                "financial_period_absolute": "FY2025Q1",
                "std_income_statement_all": 1000.0,
            },
            {
                "instrument": "AA.N",
                "name": "TR.F.NetIncome",
                "description": "Net Income",
                "period_start_date": "2025-01-01",
                "period_end_date": "2025-03-31",
                "financial_period_absolute": "FY2025Q1",
                "std_income_statement_all": 120.0,
            },
        ]
    }

    flattened = prod_fundamentals_data(payload)
    assert flattened == [
        {
            "instrument": "AA.N",
            "financial_period_absolute": "FY2025Q1",
            "period_start_date": "2025-01-01",
            "period_end_date": "2025-03-31",
            "is_tot_revenue": 1000.0,
            "is_net_income": 120.0,
        }
    ]


def test_prod_fundamentals_data_merges_statement_families() -> None:
    payload = {
        "ric": "AA.N",
        "data": [
            {
                "instrument": "AA.N",
                "statement": "income_statement",
                "name": "TR.F.TotRevenue",
                "period_start_date": "2025-01-01",
                "period_end_date": "2025-03-31",
                "financial_period_absolute": "FY2025Q1",
                "std_income_statement_all": 1000.0,
            },
            {
                "instrument": "AA.N",
                "statement": "balance_sheet",
                "name": "TR.F.TotalAssets",
                "period_start_date": "2025-01-01",
                "period_end_date": "2025-03-31",
                "financial_period_absolute": "FY2025Q1",
                "std_balance_sheet_all": 5000.0,
            },
            {
                "instrument": "AA.N",
                "statement": "cashflow_statement",
                "name": "TR.F.CashFromOperatingActivities",
                "period_start_date": "2025-01-01",
                "period_end_date": "2025-03-31",
                "financial_period_absolute": "FY2025Q1",
                "std_cash_flow_all": 200.0,
            },
        ],
    }

    flattened = prod_fundamentals_data(payload)
    assert flattened == [
        {
            "instrument": "AA.N",
            "financial_period_absolute": "FY2025Q1",
            "period_start_date": "2025-01-01",
            "period_end_date": "2025-03-31",
            "is_tot_revenue": 1000.0,
            "bs_total_assets": 5000.0,
            "cf_cash_from_operating_activities": 200.0,
        }
    ]


def test_prod_prices_data_maps_title_only_lseg_headers() -> None:
    payload = {
        "fields": [
            "TR.TotalReturn52Wk",
            "TR.PriceToMeanPriceTarget",
            "TR.FwdPtoEPSSmartEst",
            "TR.PtoEPSMeanEst",
            "TR.PEGSmart",
            "TR.PtoEBTSmartEst",
            "TR.PtoBPSSmartEst",
            "TR.PtoCPXSmartEst",
        ],
        "data": [
            {
                "date": "2026-03-02",
                "instrument": "AA",
                "52 Week Total Return|52 Week Total Return": 107.143680593659,
                "Price To Price Target Mean|Price To Price Target Mean": 1.02386526393291,
                "Price / EPS (SmartEstimate ®)|Price / EPS (SmartEstimate ®)": 13.8708204088082,
                "Price / EPS (Mean Estimate)|Price / EPS (Mean Estimate)": 24.1406850409126,
                "Price / Earnings To Growth Ratio (SmartEstimate ®)|Price / Earnings To Growth Ratio (SmartEstimate ®)": 0.728670001066448,
                "Price / EBITDA (SmartEstimate ®)|Price / EBITDA (SmartEstimate ®)": 6.74698365817598,
                "Price / Book Value Per Share (SmartEstimate ®)|Price / Book Value Per Share (SmartEstimate ®)": 2.32672050415182,
                "Price / CAPEX (SmartEstimate ®)|Price / CAPEX (SmartEstimate ®)": 24.2955027550283,
            }
        ],
    }

    flattened = prod_prices_data(payload)
    assert len(flattened) == 1
    row = flattened[0]
    assert row["total_return_52wk"] == 107.143680593659
    assert row["price_to_mean_price_target"] == 1.02386526393291
    assert row["fwd_pto_eps_smart_est"] == 13.8708204088082
    assert row["pto_eps_mean_est"] == 24.1406850409126
    assert row["peg_smart"] == 0.728670001066448
    assert row["pto_ebt_smart_est"] == 6.74698365817598
    assert row["pto_bps_smart_est"] == 2.32672050415182
    assert row["pto_cpx_smart_est"] == 24.2955027550283


def test_prod_prices_data_computes_price_change_fields() -> None:
    payload = {
        "fields": ["TR.CLOSEPRICE", "TR.TotalReturn1D"],
        "data": [
            {
                "date": "2026-03-02",
                "instrument": "AA",
                "TR.CLOSEPRICE|Close Price": 64.08,
                "TR.TotalReturn1D|Daily Total Return": 3.22164948453608,
            }
        ],
    }

    flattened = prod_prices_data(payload)
    row = flattened[0]
    assert row["close"] == 64.08
    assert row["total_return_1d"] == 3.22164948453608
    assert row["price_chg"] == 67.30164948453608
    assert row["price_pct_chg"] == 0.0322164948453608
