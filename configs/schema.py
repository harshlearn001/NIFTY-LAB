#!/usr/bin/env python3
# -*- coding: utf-8 -*-

EQUITY_SCHEMA = {
    "DATE": "datetime64[ns]",
    "OPEN": "float",
    "HIGH": "float",
    "LOW": "float",
    "CLOSE": "float",
}

FUTURES_CONT_SCHEMA = {
    "SYMBOL": "object",
    "TRADE_DATE": "datetime64[ns]",
    "EXP_DATE": "datetime64[ns]",
    "OPEN": "float",
    "HIGH": "float",
    "LOW": "float",
    "CLOSE": "float",
    "VOLUME": "float",
    "OI": "float",
}

OPTIONS_DAILY_SCHEMA = {
    "INSTRUMENT": "object",
    "SYMBOL": "object",
    "EXP_DATE": "datetime64[ns]",
    "STR_PRICE": "float",
    "OPT_TYPE": "object",
    "OPEN_PRICE": "float",
    "HI_PRICE": "float",
    "LO_PRICE": "float",
    "CLOSE_PRICE": "float",
    "OPEN_INT": "float",
    "TRD_QTY": "float",
    "NO_OF_CONT": "float",
    "NO_OF_TRADE": "float",
    "NOTION_VAL": "float",
    "PR_VAL": "float",
    "TRADE_DATE": "datetime64[ns]",
}
