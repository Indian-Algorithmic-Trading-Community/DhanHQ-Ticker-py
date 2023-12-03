import aiohttp
import asyncio
import logging
import polars as pl
from _typeshed import Incomplete
from datetime import datetime as dtdt
from polars import DataFrame
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Literal, Optional

class AttrDict(dict):
    def __getattr__(self, attr): ...
    def __setattr__(self, attr, value) -> None: ...
    def __delattr__(self, attr) -> None: ...
    def __dir__(self): ...

class Callback:
    callback: Incomplete
    is_async: Incomplete
    loop: Incomplete
    pool: Incomplete
    def __init__(
        self,
        callback: Callable,
        loop: Optional[asyncio.AbstractEventLoop] = ...,
        pool: Optional[Literal["thread", "process"]] = ...,
        max_workers: int = ...,
    ) -> None: ...
    def __del__(self) -> None: ...
    def __delete__(self) -> None: ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...
    def __aexit__(self) -> None: ...
    async def __call__(self, obj) -> None: ...

class JSONFormatter(logging.Formatter):
    def __init__(self) -> None: ...
    def format(self, record): ...
    def getTimestamp(self, created): ...

class LoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs): ...

class Utils:
    @staticmethod
    def is_windows() -> bool: ...
    @staticmethod
    def is_linux() -> bool: ...
    @staticmethod
    def is_mac() -> bool: ...
    @staticmethod
    def nbytes(frame: Any) -> int: ...
    @staticmethod
    def generate_tab_id(length) -> int: ...
    @staticmethod
    def json_stringify(
        dictionary: Dict[str, Any], return_urlencoded: bool = ...
    ) -> str: ...
    @staticmethod
    async def on_request_start(
        session: aiohttp.ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: aiohttp.TraceRequestStartParams,
    ) -> None: ...
    @staticmethod
    async def on_request_end(
        session: aiohttp.ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: aiohttp.TraceRequestEndParams,
    ) -> None: ...
    @staticmethod
    async def on_request_exception(
        session: aiohttp.ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: aiohttp.TraceRequestExceptionParams,
    ) -> None: ...
    @staticmethod
    async def generate_async_client_session(
        base_url: str,
        connector: aiohttp.TCPConnector,
        headers: Dict[str, str | Any],
        timeout: aiohttp.ClientTimeout,
        raise_for_status: bool,
        trust_env: bool,
        trace_configs: Optional[List[aiohttp.TraceConfig]] = ...,
        cookie_jar: Optional[aiohttp.CookieJar] = ...,
    ) -> aiohttp.ClientSession: ...
    @staticmethod
    def datetime_to_julian(date_time: dtdt) -> int: ...
    @staticmethod
    def julian_to_datetime(time_stamp: int) -> dtdt: ...
    @staticmethod
    def get_byte_format_length(byte_format: str) -> int: ...
    @staticmethod
    def get_dhan_imei_no(
        user_agent: str = ...,
        plugins_installed: int = ...,
        device_height: int = ...,
        device_width: int = ...,
        device_pixel_depth: int = ...,
    ) -> str: ...
    @staticmethod
    def get_instrument_type(
        is_index: bool = ...,
        is_equity: bool = ...,
        is_futidx: bool = ...,
        is_futstk: bool = ...,
        is_futcom: bool = ...,
        is_futcur: bool = ...,
        is_optidx: bool = ...,
        is_optstk: bool = ...,
        is_optcur: bool = ...,
        is_optfut: bool = ...,
    ) -> str: ...
    @staticmethod
    def get_dhan_trading_symbol(
        inst: str,
        exchange: Literal["NSE", "BSE", "MCX"],
        is_index: bool = ...,
        is_equity: bool = ...,
        is_futidx: bool = ...,
        is_futstk: bool = ...,
        is_futcom: bool = ...,
        is_futcur: bool = ...,
        is_optidx: bool = ...,
        is_optstk: bool = ...,
        is_optcur: bool = ...,
        is_optfut: bool = ...,
        expiry_date: Optional[str] = ...,
        strike: Optional[int | float] = ...,
        opt_type: Optional[Literal["CE", "PE"]] = ...,
    ) -> Optional[str | None]: ...
    @staticmethod
    def get_instrument_details(
        instrument: str,
        exchange: Literal["NSE", "BSE", "MCX"],
        is_index: bool = ...,
        is_equity: bool = ...,
        is_futidx: bool = ...,
        is_futstk: bool = ...,
        is_futcom: bool = ...,
        is_futcur: bool = ...,
        is_optidx: bool = ...,
        is_optstk: bool = ...,
        is_optcur: bool = ...,
        is_optfut: bool = ...,
        expiry_date: Optional[str] = ...,
        strike: Optional[int | float] = ...,
        opt_type: Optional[Literal["CE", "PE"]] = ...,
        pretty_print: bool = ...,
    ) -> Optional[DataFrame]: ...
    @staticmethod
    def get_instruments_details(
        instruments: List[str],
        exchange: Literal["NSE", "BSE", "MCX"],
        is_index: bool = ...,
        is_equity: bool = ...,
        is_futidx: bool = ...,
        is_futstk: bool = ...,
        is_futcom: bool = ...,
        is_futcur: bool = ...,
        is_optidx: bool = ...,
        is_optstk: bool = ...,
        is_optcur: bool = ...,
        is_optfut: bool = ...,
        expiry_dates: Optional[List[str]] = ...,
        strikes: Optional[List[int | float]] = ...,
        opt_types: Optional[List[Literal["CE", "PE"]]] = ...,
        pretty_print: bool = ...,
    ) -> Optional[DataFrame]: ...
    @staticmethod
    def get_expiry_dates(
        inst: str, exchange: Literal["NSE", "BSE", "MCX"], of_fut: bool = ...
    ) -> DataFrame: ...
    @staticmethod
    def get_option_chain(
        inst: str,
        exchange: Literal["NSE", "BSE", "MCX"],
        expiry_date: Optional[str] = ...,
    ) -> pl.DataFrame: ...
    @staticmethod
    def fetch_and_save_latest_dhan_master_scrip_feather() -> None: ...
    @staticmethod
    def get_parsed_instruments_attrdict(
        instruments: List[str] | List[Dict[str, Any]] | DataFrame
    ) -> List[AttrDict]: ...

@staticmethod
def get_logger(name, filename, level=...): ...
@staticmethod
def setup_signal_handlers(loop) -> None: ...
@staticmethod
def is_windows() -> bool: ...
@staticmethod
def is_linux() -> bool: ...
@staticmethod
def is_mac() -> bool: ...
@staticmethod
def nbytes(frame: Any) -> int: ...
@staticmethod
def generate_tab_id(length) -> int: ...
@staticmethod
def json_stringify(
    dictionary: Dict[str, Any], return_urlencoded: bool = ...
) -> str: ...
@staticmethod
async def on_request_start(
    session: aiohttp.ClientSession,
    trace_config_ctx: SimpleNamespace,
    params: aiohttp.TraceRequestStartParams,
) -> None: ...
@staticmethod
async def on_request_end(
    session: aiohttp.ClientSession,
    trace_config_ctx: SimpleNamespace,
    params: aiohttp.TraceRequestEndParams,
) -> None: ...
@staticmethod
async def on_request_exception(
    session: aiohttp.ClientSession,
    trace_config_ctx: SimpleNamespace,
    params: aiohttp.TraceRequestExceptionParams,
) -> None: ...
@staticmethod
async def generate_async_client_session(
    base_url: str,
    connector: aiohttp.TCPConnector,
    headers: Dict[str, str | Any],
    timeout: aiohttp.ClientTimeout,
    raise_for_status: bool,
    trust_env: bool,
    trace_configs: Optional[List[aiohttp.TraceConfig]] = ...,
    cookie_jar: Optional[aiohttp.CookieJar] = ...,
) -> aiohttp.ClientSession: ...
@staticmethod
def datetime_to_julian(date_time: dtdt) -> int: ...
@staticmethod
def julian_to_datetime(time_stamp: int) -> dtdt: ...
@staticmethod
def get_byte_format_length(byte_format: str) -> int: ...
@staticmethod
def get_dhan_imei_no(
    user_agent: str = ...,
    plugins_installed: int = ...,
    device_height: int = ...,
    device_width: int = ...,
    device_pixel_depth: int = ...,
) -> str: ...
@staticmethod
def get_instrument_type(
    is_index: bool = ...,
    is_equity: bool = ...,
    is_futidx: bool = ...,
    is_futstk: bool = ...,
    is_futcom: bool = ...,
    is_futcur: bool = ...,
    is_optidx: bool = ...,
    is_optstk: bool = ...,
    is_optcur: bool = ...,
    is_optfut: bool = ...,
) -> str: ...
@staticmethod
def get_dhan_trading_symbol(
    inst: str,
    exchange: Literal["NSE", "BSE", "MCX"],
    is_index: bool = ...,
    is_equity: bool = ...,
    is_futidx: bool = ...,
    is_futstk: bool = ...,
    is_futcom: bool = ...,
    is_futcur: bool = ...,
    is_optidx: bool = ...,
    is_optstk: bool = ...,
    is_optcur: bool = ...,
    is_optfut: bool = ...,
    expiry_date: Optional[str] = ...,
    strike: Optional[int | float] = ...,
    opt_type: Optional[Literal["CE", "PE"]] = ...,
) -> Optional[str | None]: ...
@staticmethod
def get_instrument_details(
    instrument: str,
    exchange: Literal["NSE", "BSE", "MCX"],
    is_index: bool = ...,
    is_equity: bool = ...,
    is_futidx: bool = ...,
    is_futstk: bool = ...,
    is_futcom: bool = ...,
    is_futcur: bool = ...,
    is_optidx: bool = ...,
    is_optstk: bool = ...,
    is_optcur: bool = ...,
    is_optfut: bool = ...,
    expiry_date: Optional[str] = ...,
    strike: Optional[int | float] = ...,
    opt_type: Optional[Literal["CE", "PE"]] = ...,
    pretty_print: bool = ...,
) -> Optional[DataFrame]: ...
@staticmethod
def get_instruments_details(
    instruments: List[str],
    exchange: Literal["NSE", "BSE", "MCX"],
    is_index: bool = ...,
    is_equity: bool = ...,
    is_futidx: bool = ...,
    is_futstk: bool = ...,
    is_futcom: bool = ...,
    is_futcur: bool = ...,
    is_optidx: bool = ...,
    is_optstk: bool = ...,
    is_optcur: bool = ...,
    is_optfut: bool = ...,
    expiry_dates: Optional[List[str]] = ...,
    strikes: Optional[List[int | float]] = ...,
    opt_types: Optional[List[Literal["CE", "PE"]]] = ...,
    pretty_print: bool = ...,
) -> Optional[DataFrame]: ...
@staticmethod
def get_expiry_dates(
    inst: str, exchange: Literal["NSE", "BSE", "MCX"], of_fut: bool = ...
) -> DataFrame: ...
@staticmethod
def get_option_chain(
    inst: str,
    exchange: Literal["NSE", "BSE", "MCX"],
    expiry_date: Optional[str] = ...,
) -> pl.DataFrame: ...
@staticmethod
def fetch_and_save_latest_dhan_master_scrip_feather() -> None: ...
@staticmethod
def get_parsed_instruments_attrdict(
    instruments: List[str] | List[Dict[str, Any]] | DataFrame
) -> List[AttrDict]: ...
