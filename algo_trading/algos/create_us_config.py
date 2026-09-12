# -*- coding: utf-8 -*-
"""
algo_trading/algos/create_us_config.py
──────────────────────────────────────
Generates the token_ref_us.xlsx configuration spreadsheet for U-Exchange underlyings.
"""

import os
import polars as pl
from algo_trading.algos.polars_excel import write_polars_sheets_to_excel


def generate_us_config(output_path: str = "algo_trading/algos/token_ref_us.xlsx") -> str:
    # 1. stock_config
    stock_config = pl.DataFrame({
        "Parameter": [
            "capital_allowed",
            "percent_of_capital_utilization",
            "margin_per_stock",
            "debounce_counter_threshold",
            "order_pending_counter_threshold",
            "stoploss_threshold",
            "hedge_threshold",
            "strike_choice_CE",
            "strike_choice_PE",
            "live_balance_lower_limit",
        ],
        "Value": [
            "100000.0",
            "1.0",
            "25000.0",
            "3",
            "5",
            "0.15",
            "1.5",
            "ATM",
            "ATM",
            "10000.0",
        ],
    })

    # 2. Stock_list (Underlying candidates)
    stock_list = pl.DataFrame({
        "Stock": ["SPY", "QQQ", "AAPL", "MSFT", "TSLA", "NVDA"],
        "Capital_share": [0.30, 0.25, 0.15, 0.10, 0.10, 0.10],
        "Max_lots_per_order": [5, 5, 10, 10, 5, 5],
        "Strike_dist_CE": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "Strike_dist_PE": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "Tradable_stock": ["Yes", "Yes", "Yes", "Yes", "Yes", "Yes"],
    })

    # 3. cap_config
    cap_config = pl.DataFrame({
        "Symbol": ["SPY", "QQQ", "AAPL", "MSFT", "TSLA", "NVDA"],
        "tradable": [1, 1, 1, 1, 1, 1],
        "minimum_lots_to_buy": [1, 1, 1, 1, 1, 1],
        "maximum_lots_to_buy": [5, 5, 10, 10, 5, 5],
        "preference": [1, 2, 3, 4, 5, 6],
        "lower_price_limit": [0.10, 0.10, 0.05, 0.05, 0.10, 0.10],
        "upper_price_limit": [50.0, 50.0, 30.0, 30.0, 50.0, 50.0],
    })

    sheets = {
        "stock_config": stock_config,
        "Stock_list": stock_list,
        "cap_config": cap_config,
    }

    return write_polars_sheets_to_excel(sheets_dict=sheets, output_path=output_path)


if __name__ == "__main__":
    out = generate_us_config()
    print(f"Generated US config spreadsheet at: {out}")
