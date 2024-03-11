import os, sys, signal, json, asyncio, aiohttp, platform, logging, polars as pl
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Literal,
    Tuple,
)
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
from pathlib import Path
from random import randint
from struct import calcsize
from polars import DataFrame
from types import SimpleNamespace
from urllib.parse import quote_plus
from multiprocessing import cpu_count
from datetime import datetime as dtdt
from logging.handlers import RotatingFileHandler

try:
    from signal import SIGABRT, SIGINT, SIGTERM, SIGHUP

    SIGNALS = (SIGABRT, SIGINT, SIGTERM, SIGHUP)
except (ImportError, ModuleNotFoundError):
    from signal import SIGABRT, SIGINT, SIGTERM

    SIGNALS = (SIGABRT, SIGINT, SIGTERM)

# LOGGING_FORMAT: str = "[%(levelname)s]|[%(asctime)s]|[%(threadName)s]|[%(taskName)s]|[%(name)s::%(module)s::%(funcName)s::%(lineno)d]|=> %(message)s"

LOGGING_FORMAT: str = "[%(levelname)s]|[%(asctime)s]|[%(name)s::%(module)s::%(funcName)s::%(lineno)d]|=> %(message)s"

MASTER_SCRIP_CSV_URL: str = "https://images.dhan.co/api-data/api-scrip-master.csv"
MASTER_SCRIP_FEATHER: str = "master_scrip.feather"

MASTER_SCRIP_COLUMNS: List[str] = [
    "SEM_EXM_EXCH_ID",
    " SEM_SEGMENT",
    "SEM_SMST_SECURITY_ID",
    "SEM_INSTRUMENT_NAME",
    "SEM_EXPIRY_CODE",
    "SEM_TRADING_SYMBOL",
    "SEM_LOT_UNITS",
    "SEM_CUSTOM_SYMBOL",
    "SEM_EXPIRY_DATE",
    "SEM_STRIKE_PRICE",
    "SEM_OPTION_TYPE",
    "SEM_TICK_SIZE",
    "SEM_EXPIRY_FLAG",
    "KEY",
    "EXPIRY_DATE_STR",
    "EXPIRY_TIME_STR",
    "EXPIRY_DATE",
    "EXPIRY_TIME",
]

MASTER_SCRIP_INSTRUMENT_COLUMNS: List[str] = [
    "SEM_EXM_EXCH_ID",
    " SEM_SEGMENT",
    "SEM_SMST_SECURITY_ID",
    "SEM_INSTRUMENT_NAME",
    "SEM_TRADING_SYMBOL",
    "SEM_CUSTOM_SYMBOL",
    "KEY",
    "SEM_LOT_UNITS",
    "SEM_STRIKE_PRICE",
    "SEM_OPTION_TYPE",
    "SEM_TICK_SIZE",
    "EXPIRY_DATE_STR",
    "SEM_EXPIRY_FLAG",
]

MASTER_SCRIP_INSTRUMENT_COLUMNS_RENAME_TO: Dict[str, str] = {
    "SEM_EXM_EXCH_ID": "Exchange",
    " SEM_SEGMENT": "Segment",
    "SEM_SMST_SECURITY_ID": "SecurityId",
    "SEM_INSTRUMENT_NAME": "InstrumentType",
    "SEM_TRADING_SYMBOL": "TradingSymbol",
    "SEM_CUSTOM_SYMBOL": "DisplayName",
    "KEY": "DhanUniqueKey",
    "SEM_LOT_UNITS": "LotUnits",
    "SEM_STRIKE_PRICE": "StrikePrice",
    "SEM_OPTION_TYPE": "OptionType",
    "SEM_TICK_SIZE": "TickSize",
    "EXPIRY_DATE_STR": "ExpiryDate",
    "SEM_EXPIRY_FLAG": "ExpiryType",
}

OPTION_CHAIN_COLUMNS: List[str] = [
    "SEM_TRADING_SYMBOL",
    "SEM_SMST_SECURITY_ID",
    "SEM_STRIKE_PRICE",
    "SEM_OPTION_TYPE",
    "SEM_LOT_UNITS",
    "SEM_TICK_SIZE",
    "EXPIRY_DATE_STR",
    "SEM_EXPIRY_FLAG",
    "SEM_EXM_EXCH_ID",
    " SEM_SEGMENT",
    "SEM_INSTRUMENT_NAME",
    "SEM_CUSTOM_SYMBOL",
    "KEY",
]

DEFAULT_INSTRUMENT_DICT: Dict[str, Any] = {
    "Exchange": "",
    "Segment": "",
    "Token": None,
    "TradingSymbol": "",
    "DisplayName": "",
    "DhanUniqueKey": "",
}

PRICE_UPDATE_DICT: Dict[str, Any] = {
    "Ltp": 0.0,
    "Open": 0.0,
    "High": 0.0,
    "Low": 0.0,
    "Close": 0.0,
    "PrevClose": 0.0,
    "Volume": 0,
    "AveragePrice": 0.0,
    "OpenInterest": 0,
    "PrevOpenInterest": 0,
    "OpenInterestChg": 0,
    "OpenInterestChgPrcnt": 0.0,
    "LastTradedQty": 0,
    "LastTradedTime": "",
    "LastUpdatedTime": "",
    "Change": 0.0,
    "ChangePrcnt": 0.0,
    "BestAskPrice": 0.0,
    "BestAskQty": 0,
    "BestBidPrice": 0.0,
    "BestBidQty": 0,
    "TotalAskQty": 0,
    "TotalBidQty": 0,
    "TotalBidAskQty": 0,
    "TotalBidQtyPrcnt": 0.0,
    "TotalAskQtyPrcnt": 0.0,
    "Week52High": 0.0,
    "Week52Low": 0.0,
    "LowerCircuit": 0.0,
    "UpperCircuit": 0.0,
}

ORDER_PACKET_DEFAULT_KEYS: Tuple[str, ...] = (
    "msg_code",
    "msg_len",
    "exchange",
    "segment",
    "source",
    "security_id",
    "client_id",
    "exch_order_no",
    "order_no",
    "product",
    "txn_type",
    "order_type",
    "validity",
    "disc_quantity",
    "dq_qty_rem",
    "remaining_quantity",
    "quantity",
    "traded_qty",
    "price",
    "trigger_price",
    "serial_no",
    "traded_price",
    "avg_traded_price",
    "algo_ord_no",
    "strategy_id",
    "off_mkt_flag",
    "order_date_time",
    "exch_order_time",
    "last_updated_time",
    "remarks",
    "mkt_type",
    "reason_description",
    "leg_no",
    "mkt_pro_flag",
    "mkt_pro_value",
    "participant_type",
    "settlor",
    "gtcflag",
    "encash_flag",
    "pan_no",
    "group_id",
    "instrument",
    "symbol",
    "product_name",
    "status",
    "lot_size",
    "fsltrail",
    "fsltickvalue",
    "fprtickvalue",
    "strike_price",
    "expiry_date",
    "opt_type",
    "display_name",
    "isin",
    "series",
    "good_till_days_date",
    "sintrumenttype",
    "ref_ltp",
    "tick_size",
    "algo_id",
    "splatform",
    "schannel",
    "multiplier",
)


__all__ = (
    "Utils",
    "is_mac",
    "nbytes",
    "Callback",
    "is_linux",
    "AttrDict",
    "get_logger",
    "is_windows",
    "LoggerAdapter",
    "JSONFormatter",
    "json_stringify",
    "on_request_end",
    "strip_nullbytes",
    "generate_tab_id",
    "get_dhan_imei_no",
    "on_request_start",
    "get_expiry_dates",
    "get_option_chain",
    "datetime_to_julian",
    "julian_to_datetime",
    "get_instrument_type",
    "on_request_exception",
    "setup_signal_handlers",
    "get_byte_format_length",
    "get_instrument_details",
    "get_instruments_details",
    "get_dhan_trading_symbol",
    "generate_async_client_session",
    "get_parsed_instruments_attrdict",
    "fetch_and_save_latest_dhan_master_scrip_feather",
)
log = logging.getLogger(__name__)


class AttrDict(dict):
    __slots__ = ()

    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(attr) from None

    def __setattr__(self, attr, value):
        self[attr] = value

    def __delattr__(self, attr):
        try:
            del self[attr]
        except KeyError:
            raise AttributeError(attr) from None

    def __dir__(self):
        return list(self) + dir(type(self))


class Callback:
    def __init__(
        self,
        callback: Callable,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        pool: Optional[Literal["thread", "process"]] = None,
        max_workers: int = cpu_count(),
    ) -> None:
        self.callback = callback
        self.is_async = asyncio.iscoroutinefunction(callback)
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.pool = (
            ThreadPoolExecutor(max_workers=max_workers)
            if isinstance(pool, str) and pool == "thread"
            else ProcessPoolExecutor(max_workers=max_workers)
            if isinstance(pool, str) and pool == "process"
            else None
        )

    def __shutdown_pool(self) -> None:
        if self.pool is not None:
            self.pool.shutdown(wait=False, cancel_futures=True)

    def __del__(self) -> None:
        self.__shutdown_pool()

    def __delete__(self) -> None:
        self.__shutdown_pool()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.__shutdown_pool()

    def __aexit__(self) -> None:
        self.__shutdown_pool()

    async def __call__(self, obj):
        if self.callback is None:
            return
        elif self.is_async:
            await self.callback(obj)
        else:
            await self.loop.run_in_executor(self.pool, self.callback, obj)


class JSONFormatter(logging.Formatter):
    """
    Render logs as JSON.

    To add details to a log record, store them in a ``event_data``
    custom attribute. This dict is merged into the event.

    """

    def __init__(self):
        pass  # override logging.Formatter constructor

    def format(self, record):
        event = {
            "timestamp": self.getTimestamp(record.created),
            "message": record.getMessage(),
            "level": record.levelname,
            "logger": record.name,
        }
        event_data = getattr(record, "event_data", None)
        if event_data:
            event.update(event_data)
        if record.exc_info:
            event["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            event["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(event)

    def getTimestamp(self, created):
        return dtdt.utcfromtimestamp(created).isoformat()


class LoggerAdapter(logging.LoggerAdapter):
    """Add connection ID and client IP address to websockets logs."""

    def process(self, msg, kwargs):
        try:
            websocket = kwargs["extra"]["websocket"]
        except KeyError:
            return msg, kwargs
            kwargs["extra"]["event_data"] = {
                "connection_id": str(websocket.id),
                "remote_addr": websocket.request_headers.get("X-Forwarded-For"),
            }
        return msg, kwargs


class Utils:
    @staticmethod
    def is_windows() -> bool:
        return (
            os.name == "nt"
            and sys.platform == "win32"
            and platform.system() == "Windows"
        )  # noqa E501  # noqa E501

    @staticmethod
    def is_linux() -> bool:
        return (
            os.name == "posix"
            and platform.system() == "Linux"
            and sys.platform in {"linux", "linux2"}
        )  # noqa: E501

    @staticmethod
    def is_mac() -> bool:
        return (
            os.name == "posix"
            and sys.platform == "darwin"
            and platform.system() == "Darwin"
        )  # noqa E501  # noqa E501

    @staticmethod
    def nbytes(frame: Any) -> int:
        if isinstance(frame, (bytes, bytearray)):
            return len(frame)
        else:
            try:
                return frame.nbytes
            except AttributeError:
                return len(frame)

    @staticmethod
    def strip_nullbytes(s: str | bytes) -> str | bytes:
        _rc, rw = (b"\x00", b"") if isinstance(s, bytes) else ("\x00", "")
        return s.lstrip(_rc).rstrip(_rc).strip(_rc).strip().replace(_rc, rw)

    @staticmethod
    def generate_tab_id(length) -> int:
        return randint(10 ** (length - 1), (10**length) - 1)

    @staticmethod
    def json_stringify(
        dictionary: Dict[str, Any],
        return_urlencoded: bool = False,
    ) -> str:
        payload_serialized = json.dumps(dictionary, separators=(",", ":"))
        return (
            quote_plus(payload_serialized) if return_urlencoded else payload_serialized
        )

    @staticmethod
    async def on_request_start(
        session: aiohttp.ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: aiohttp.TraceRequestStartParams,
    ) -> None:
        trace_config_ctx.start = asyncio.get_running_loop().time()
        log.info(
            "[Request Hook]|=> Initiated HTTP.%s Request To Url: %s | With Header: %s |",
            params.method,
            params.url,
            json.dumps(dict(params.headers)),
        )
        print("\n")

    @staticmethod
    async def on_request_end(
        session: aiohttp.ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: aiohttp.TraceRequestEndParams,
    ) -> None:
        elapsed = asyncio.get_running_loop().time() - trace_config_ctx.start
        elapsed_msg = f"{elapsed:.3f} Seconds"
        text = await params.response.text()
        if len(text) > 500:
            text = f"{text[:500]}...Truncated to 500 Characters."
        log.info(
            "[Response Hook]|=> The HTTP.%s Request To Url: %s | Completed In %s | Response Status: %s %s | Response Header: %s | Response Content: %s |",
            params.method,
            params.url,
            elapsed_msg,
            params.response.status,
            params.response.reason,
            json.dumps(dict(params.headers)),
            text,
        )
        print("\n")

    @staticmethod
    async def on_request_exception(
        session: aiohttp.ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: aiohttp.TraceRequestExceptionParams,
    ) -> None:
        log.info(
            "[Request Exception Hook]|=> The HTTP.%s Request To Url: %s | Request Header: %s | Failed With Exception: %s |",
            params.method,
            params.url,
            json.dumps(dict(params.headers)),
            str(params.exception),
        )
        print("\n")

    @staticmethod
    async def generate_async_client_session(
        base_url: str,
        connector: aiohttp.TCPConnector,
        headers: Dict[str, str | Any],
        timeout: aiohttp.ClientTimeout,
        raise_for_status: bool,
        trust_env: bool,
        trace_configs: Optional[List[aiohttp.TraceConfig]] = None,
        cookie_jar: Optional[aiohttp.CookieJar] = None,
    ) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            base_url=base_url,
            connector=connector,
            headers=headers,
            timeout=timeout,
            cookie_jar=cookie_jar,
            raise_for_status=raise_for_status,
            trust_env=trust_env,
            trace_configs=trace_configs,
        )

    @staticmethod
    def datetime_to_julian(date_time: dtdt) -> int:
        return int(
            date_time.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        ) - int(
            dtdt(
                year=1980, month=1, day=1, hour=5, minute=30, second=0, microsecond=0
            ).timestamp()
        )

    @staticmethod
    def julian_to_datetime(time_stamp: int) -> dtdt:
        return dtdt.fromtimestamp(
            time_stamp
            + int(
                dtdt(
                    year=1980,
                    month=1,
                    day=1,
                    hour=5,
                    minute=30,
                    second=0,
                    microsecond=0,
                ).timestamp()
            )
        )

    @staticmethod
    def get_byte_format_length(byte_format: str) -> int:
        return calcsize(f"<{byte_format}")

    @staticmethod
    def get_dhan_imei_no(
        user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        plugins_installed: int = 0,
        device_height: int = 1329,
        device_width: int = 2056,
        device_pixel_depth: int = 30,
    ) -> str:
        return "".join(filter(str.isdigit, user_agent)) + "".join(
            map(
                str,
                [
                    plugins_installed,
                    device_height,
                    device_width,
                    device_pixel_depth,
                    Utils.generate_tab_id(6),
                ],
            )
        )

    @staticmethod
    def get_instrument_type(
        is_index: bool = False,
        is_equity: bool = False,
        is_futidx: bool = False,
        is_futstk: bool = False,
        is_futcom: bool = False,
        is_futcur: bool = False,
        is_optidx: bool = False,
        is_optstk: bool = False,
        is_optcur: bool = False,
        is_optfut: bool = False,
    ) -> str:
        return (
            "INDEX"
            if is_index
            else "EQUITY"
            if is_equity
            else "FUTIDX"
            if is_futidx
            else "FUTSTK"
            if is_futstk
            else "FUTCUR"
            if is_futcur
            else "FUTCOM"
            if is_futcom
            else "OPTIDX"
            if is_optidx
            else "OPTSTK"
            if is_optstk
            else "OPTCUR"
            if is_optcur
            else "OPTFUT"
            if is_optfut
            else ""
        )

    @staticmethod
    def get_dhan_trading_symbol(
        inst: str,
        exchange: Literal["NSE", "BSE", "MCX"],
        is_index: bool = False,
        is_equity: bool = False,
        is_futidx: bool = False,
        is_futstk: bool = False,
        is_futcom: bool = False,
        is_futcur: bool = False,
        is_optidx: bool = False,
        is_optstk: bool = False,
        is_optcur: bool = False,
        is_optfut: bool = False,
        expiry_date: Optional[str] = None,
        strike: Optional[int | float] = None,
        opt_type: Optional[Literal["CE", "PE"]] = None,
    ) -> Optional[str | None]:
        if is_index or is_equity:
            return inst
        if expiry_date is not None and not (is_index or is_equity):
            expiry = dtdt.strptime(expiry_date, "%Y-%m-%d")
            expiry_format = (
                "%d%b%Y"
                if exchange in {"BSE", "MCX"}
                and (is_futcur or is_optcur or is_optfut or is_futcom)
                else "%b%Y"
            )
            opt_or_fut = (
                opt_type
                if opt_type is not None
                and (is_optidx or is_optstk or is_optcur or is_optfut)
                else "FUT"
                if (is_futidx or is_futstk or is_futcom or is_futcur)
                else ""
            )
            sem_trading_symbol = "-".join(
                map(
                    str,
                    filter(
                        None.__ne__,
                        [
                            inst.upper(),
                            expiry.strftime(expiry_format),
                            strike,
                            opt_or_fut,
                        ],
                    ),
                )
            )
            return sem_trading_symbol
        raise ValueError(
            "Please Provide Correct Inputs, Check All Arguments and Keyword Arguments"
        )

    @staticmethod
    def get_instrument_details(
        instrument: str,
        exchange: Literal["NSE", "BSE", "MCX"],
        is_index: bool = False,
        is_equity: bool = False,
        is_futidx: bool = False,
        is_futstk: bool = False,
        is_futcom: bool = False,
        is_futcur: bool = False,
        is_optidx: bool = False,
        is_optstk: bool = False,
        is_optcur: bool = False,
        is_optfut: bool = False,
        expiry_date: Optional[str] = None,
        strike: Optional[int | float] = None,
        opt_type: Optional[Literal["CE", "PE"]] = None,
        pretty_print: bool = False,
    ) -> Optional[DataFrame]:
        inst_type = Utils.get_instrument_type(
            is_index=is_index,
            is_equity=is_equity,
            is_futidx=is_futidx,
            is_futstk=is_futstk,
            is_futcom=is_futcom,
            is_futcur=is_futcur,
            is_optidx=is_optidx,
            is_optstk=is_optstk,
            is_optcur=is_optcur,
            is_optfut=is_optfut,
        )
        sem_trading_symbol = Utils.get_dhan_trading_symbol(
            instrument,
            exchange,
            is_index=is_index,
            is_equity=is_equity,
            is_futidx=is_futidx,
            is_futstk=is_futstk,
            is_futcom=is_futcom,
            is_futcur=is_futcur,
            is_optidx=is_optidx,
            is_optstk=is_optstk,
            is_optcur=is_optcur,
            is_optfut=is_optfut,
            expiry_date=expiry_date,
            strike=strike,
            opt_type=opt_type,
        )
        print(sem_trading_symbol)
        if len(inst_type) >= 5 and sem_trading_symbol is not None:
            condition = (
                (pl.col("SEM_EXM_EXCH_ID") == exchange.upper())
                & (pl.col("SEM_TRADING_SYMBOL") == sem_trading_symbol)
                & (pl.col("SEM_INSTRUMENT_NAME") == inst_type)
            )
            if expiry_date is not None:
                condition = condition & (pl.col("EXPIRY_DATE_STR") == expiry_date)
            if strike is not None:
                condition = condition & (pl.col("SEM_STRIKE_PRICE") == strike)
            if opt_type is not None:
                condition = condition & (pl.col("SEM_OPTION_TYPE") == opt_type)
            df = (
                pl.scan_ipc(MASTER_SCRIP_FEATHER)
                .filter(condition)
                .select(MASTER_SCRIP_INSTRUMENT_COLUMNS)
                .collect()
                .rename(MASTER_SCRIP_INSTRUMENT_COLUMNS_RENAME_TO)
            )
            df = df[
                :,
                [
                    not (s.null_count() == df.height or all(s.str.contains("NA")))
                    for s in df
                ],
            ]
            if pretty_print:
                with pl.Config() as cfg:
                    cfg.set_tbl_cols(2)
                    cfg.set_tbl_rows(len(df.columns))
                    for i in range(len(df)):
                        print(df[i,].melt())
            return df

    @staticmethod
    def get_instruments_details(
        instruments: List[str],
        exchange: Literal["NSE", "BSE", "MCX"],
        is_index: bool = False,
        is_equity: bool = False,
        is_futidx: bool = False,
        is_futstk: bool = False,
        is_futcom: bool = False,
        is_futcur: bool = False,
        is_optidx: bool = False,
        is_optstk: bool = False,
        is_optcur: bool = False,
        is_optfut: bool = False,
        expiry_dates: Optional[List[str]] = None,
        strikes: Optional[List[int | float]] = None,
        opt_types: Optional[List[Literal["CE", "PE"]]] = None,
        pretty_print: bool = False,
    ) -> Optional[DataFrame]:
        inst_type = Utils.get_instrument_type(
            is_index=is_index,
            is_equity=is_equity,
            is_futidx=is_futidx,
            is_futstk=is_futstk,
            is_futcom=is_futcom,
            is_futcur=is_futcur,
            is_optidx=is_optidx,
            is_optstk=is_optstk,
            is_optcur=is_optcur,
            is_optfut=is_optfut,
        )
        if (
            is_futidx or is_futstk or is_futcom or is_futcur
        ) and expiry_dates is not None:
            if len(instruments) != len(expiry_dates):
                raise ValueError(
                    "Length of `instruments` List and `expiry_date` should be equal"
                )
            else:
                zipped_instruments = zip(instruments, expiry_dates)
                sem_trading_symbols = [
                    _sem_trading_symbol
                    for _sem_trading_symbol in [
                        Utils.get_dhan_trading_symbol(
                            inst,
                            exchange,
                            is_index=is_index,
                            is_equity=is_equity,
                            is_futidx=is_futidx,
                            is_futstk=is_futstk,
                            is_futcom=is_futcom,
                            is_futcur=is_futcur,
                            is_optidx=is_optidx,
                            is_optstk=is_optstk,
                            is_optcur=is_optcur,
                            is_optfut=is_optfut,
                            expiry_date=expiry_date,
                        )
                        for inst, expiry_date in zipped_instruments
                    ]
                    if _sem_trading_symbol is not None
                ]
        elif (
            (is_optidx or is_optstk or is_optcur or is_optfut)
            and expiry_dates is not None
            and strikes is not None
            and opt_types is not None
        ):
            if len(instruments) != len(expiry_dates) != len(strikes) != len(opt_types):
                raise ValueError(
                    "Length of `instruments`, `expiry_date`, `strike`, and `opt_type` List should be equal"
                )
            else:
                zipped_instruments = zip(instruments, expiry_dates, strikes, opt_types)
                sem_trading_symbols = [
                    _sem_trading_symbol
                    for _sem_trading_symbol in [
                        Utils.get_dhan_trading_symbol(
                            inst,
                            exchange,
                            is_index=is_index,
                            is_equity=is_equity,
                            is_futidx=is_futidx,
                            is_futstk=is_futstk,
                            is_futcom=is_futcom,
                            is_futcur=is_futcur,
                            is_optidx=is_optidx,
                            is_optstk=is_optstk,
                            is_optcur=is_optcur,
                            is_optfut=is_optfut,
                            expiry_date=expiry_date,
                            strike=strike,
                            opt_type=opt_type,
                        )
                        for inst, expiry_date, strike, opt_type in zipped_instruments
                    ]
                    if _sem_trading_symbol is not None
                ]
        else:
            sem_trading_symbols = [
                _sem_trading_symbol
                for _sem_trading_symbol in [
                    Utils.get_dhan_trading_symbol(
                        inst,
                        exchange,
                        is_index=is_index,
                        is_equity=is_equity,
                        is_futidx=is_futidx,
                        is_futstk=is_futstk,
                        is_futcom=is_futcom,
                        is_futcur=is_futcur,
                        is_optidx=is_optidx,
                        is_optstk=is_optstk,
                        is_optcur=is_optcur,
                        is_optfut=is_optfut,
                    )
                    for inst in instruments
                ]
                if _sem_trading_symbol is not None
            ]
        if len(inst_type) >= 5 and len(sem_trading_symbols) >= 2:
            condition = (
                (pl.col("SEM_EXM_EXCH_ID") == exchange.upper())
                & pl.any_horizontal(
                    (pl.col("SEM_TRADING_SYMBOL") == sem_trading_symbol)
                    for sem_trading_symbol in sem_trading_symbols
                )
                & (pl.col("SEM_INSTRUMENT_NAME") == inst_type)
            )
            if expiry_dates is not None:
                condition = condition & pl.any_horizontal(
                    (pl.col("EXPIRY_DATE_STR") == expiry_date)
                    for expiry_date in set(expiry_dates)
                )
            if strikes is not None:
                condition = condition & pl.any_horizontal(
                    (pl.col("SEM_STRIKE_PRICE") == strike) for strike in set(strikes)
                )
            if opt_types is not None:
                condition = condition & pl.any_horizontal(
                    (pl.col("SEM_OPTION_TYPE") == opt_type)
                    for opt_type in set(opt_types)
                )
            df = (
                pl.scan_ipc(MASTER_SCRIP_FEATHER)
                .filter(condition)
                .select(MASTER_SCRIP_INSTRUMENT_COLUMNS)
                .collect()
                .rename(MASTER_SCRIP_INSTRUMENT_COLUMNS_RENAME_TO)
            )
            df = df[
                :,
                [
                    not (s.null_count() == df.height or all(s.str.contains("NA")))
                    for s in df
                ],
            ]
            if pretty_print:
                with pl.Config() as cfg:
                    cfg.set_tbl_cols(2)
                    cfg.set_tbl_rows(len(df.columns))
                    for i in range(len(df)):
                        print(df[i,].melt())
            return df

    @staticmethod
    def get_expiry_dates(
        inst: str,
        exchange: Literal["NSE", "BSE", "MCX"],
        of_fut: bool = False,
    ) -> DataFrame:
        if of_fut:
            condition = (
                (pl.col("SEM_EXM_EXCH_ID") == exchange.upper())
                & (pl.col("SEM_INSTRUMENT_NAME").str.starts_with("FUT"))
                & (pl.col("SEM_TRADING_SYMBOL").str.starts_with(f"{inst.upper()}-"))
                & (pl.col("SEM_TRADING_SYMBOL").str.ends_with("-FUT"))
                & (pl.col("SEM_CUSTOM_SYMBOL").str.ends_with("FUT"))
            )
        else:
            condition = (
                (pl.col("SEM_EXM_EXCH_ID") == exchange.upper())
                & (pl.col("SEM_INSTRUMENT_NAME").str.starts_with("OPT"))
                & (pl.col("SEM_TRADING_SYMBOL").str.starts_with(f"{inst.upper()}-"))
                & (
                    (pl.col("SEM_TRADING_SYMBOL").str.ends_with("-CE"))
                    | (pl.col("SEM_TRADING_SYMBOL").str.ends_with("-PE"))
                )
                & (
                    (pl.col("SEM_CUSTOM_SYMBOL").str.ends_with("CALL"))
                    | (pl.col("SEM_CUSTOM_SYMBOL").str.ends_with("PUT"))
                )
                & (
                    (pl.col("SEM_OPTION_TYPE") == "CE")
                    | (pl.col("SEM_OPTION_TYPE") == "PE")
                )
            )
        return (
            pl.scan_ipc(MASTER_SCRIP_FEATHER)
            .filter(condition)
            .sort("EXPIRY_DATE")
            .select(
                "EXPIRY_DATE_STR",
                "SEM_EXPIRY_FLAG",
            )
            .unique(maintain_order=True)
            .collect()
            .rename(
                {
                    "EXPIRY_DATE_STR": "ExpiryDate",
                    "SEM_EXPIRY_FLAG": "ExpiryType",
                }
            )
        )

    @staticmethod
    def get_option_chain(
        inst: str,
        exchange: Literal["NSE", "BSE", "MCX"],
        expiry_date: Optional[str] = None,
    ) -> pl.DataFrame:
        get_all_expiries = Utils.get_expiry_dates(inst, exchange)
        return (
            pl.scan_ipc(MASTER_SCRIP_FEATHER)
            .filter(
                (pl.col("SEM_EXM_EXCH_ID") == exchange.upper())
                & (pl.col("SEM_INSTRUMENT_NAME").str.starts_with("OPT"))
                & (pl.col("SEM_TRADING_SYMBOL").str.starts_with(f"{inst.upper()}-"))
                & (
                    (pl.col("SEM_TRADING_SYMBOL").str.ends_with("-CE"))
                    | (pl.col("SEM_TRADING_SYMBOL").str.ends_with("-PE"))
                )
                & (
                    (pl.col("SEM_CUSTOM_SYMBOL").str.ends_with("CALL"))
                    | (pl.col("SEM_CUSTOM_SYMBOL").str.ends_with("PUT"))
                )
                & (
                    (pl.col("SEM_OPTION_TYPE") == "CE")
                    | (pl.col("SEM_OPTION_TYPE") == "PE")
                )
                & (
                    pl.col("EXPIRY_DATE_STR")
                    == (
                        expiry_date
                        if (
                            expiry_date is not None
                            and expiry_date in get_all_expiries["ExpiryDate"]
                        )
                        else get_all_expiries["ExpiryDate"][0]
                    )
                )
            )
            .select(OPTION_CHAIN_COLUMNS)
            .sort("SEM_STRIKE_PRICE")
            .collect()
            .rename(MASTER_SCRIP_INSTRUMENT_COLUMNS_RENAME_TO)
        )

    @staticmethod
    def fetch_and_save_latest_dhan_master_scrip_feather():
        master_scrip_feather = Path(MASTER_SCRIP_FEATHER)
        if not master_scrip_feather.exists() or (
            master_scrip_feather.exists()
            and (
                dtdt.today().replace(hour=8, minute=30, second=0, microsecond=0)
                >= dtdt.fromtimestamp(os.path.getmtime(master_scrip_feather))
            )
        ):
            log.info("Proceeding With A Fresh Download of Master Scrip Data File")
            pl.read_csv(MASTER_SCRIP_CSV_URL, truncate_ragged_lines=True).select(
                "*",
                pl.concat_str(
                    [
                        pl.col("SEM_SMST_SECURITY_ID").cast(pl.Utf8),
                        " SEM_SEGMENT",
                        "SEM_EXM_EXCH_ID",
                    ],
                    separator="-",
                ).alias("KEY"),
                pl.col("SEM_EXPIRY_DATE")
                .str.split_exact(" ", 1)
                .struct.rename_fields(["EXPIRY_DATE_STR", "EXPIRY_TIME_STR"])
                .alias("fields"),
            ).unnest("fields").select(
                "*",
                pl.col("EXPIRY_DATE_STR").str.to_date().alias("EXPIRY_DATE"),
                pl.col("EXPIRY_TIME_STR").str.to_time().alias("EXPIRY_TIME"),
            ).write_ipc(MASTER_SCRIP_FEATHER)
            log.info("Master Scrip Data Downloaded And Saved As A Feather File.")

    @staticmethod
    def get_parsed_instruments_attrdict(
        instruments: List[str] | List[Dict[str, Any]] | DataFrame,
    ) -> List[AttrDict]:
        if isinstance(instruments, list):
            return [
                AttrDict(
                    instrument
                    | {
                        "PriceUpdates": AttrDict(
                            PRICE_UPDATE_DICT
                            | {"MarketDepth": AttrDict({"Buy": None, "Sell": None})}
                        ),
                        "OrderUpdates": None,
                    }
                )
                for instrument in pl.scan_ipc(MASTER_SCRIP_FEATHER)
                .filter(
                    pl.any_horizontal(
                        (pl.col("KEY") == instrument["DhanUniqueKey"])
                        for instrument in instruments
                        if isinstance(instrument, dict)
                    )
                    if isinstance(instruments[0], dict)
                    else pl.any_horizontal(
                        (pl.col("KEY") == instrument)
                        for instrument in instruments
                        if isinstance(instrument, str)
                    )
                )
                .collect()
                .rename(MASTER_SCRIP_INSTRUMENT_COLUMNS_RENAME_TO)
                .to_dicts()
                if isinstance(instrument, dict)
            ]
        return [
            AttrDict(
                instrument
                | {
                    "PriceUpdates": AttrDict(
                        PRICE_UPDATE_DICT
                        | {"MarketDepth": AttrDict({"Buy": None, "Sell": None})}
                    ),
                    "OrderUpdates": None,
                }
            )
            for instrument in instruments.to_dicts()
            if isinstance(instrument, dict)
        ]


@staticmethod
def get_logger(name, filename, level=logging.WARNING):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    stream = logging.StreamHandler()
    stream.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logger.addHandler(stream)

    fh = RotatingFileHandler(filename, maxBytes=10 * 1024 * 1024, backupCount=10)
    fh.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logger.addHandler(fh)
    logger.propagate = False
    return logger


@staticmethod
def setup_signal_handlers(loop):
    """
    This must be run from the loop in the main thread
    """

    def handle_stop_signals(*args):
        raise SystemExit

    if sys.platform.startswith("win"):
        # NOTE: asyncio loop.add_signal_handler() not supported on windows
        for sig in SIGNALS:
            signal.signal(sig, handle_stop_signals)
    else:
        for sig in SIGNALS:
            loop.add_signal_handler(sig, handle_stop_signals)


is_mac = Utils.is_mac
nbytes = Utils.nbytes
strip_nullbytes = Utils.strip_nullbytes
is_linux = Utils.is_linux
is_windows = Utils.is_windows
json_stringify = Utils.json_stringify
on_request_end = Utils.on_request_end
generate_tab_id = Utils.generate_tab_id
get_dhan_imei_no = Utils.get_dhan_imei_no
get_expiry_dates = Utils.get_expiry_dates
get_option_chain = Utils.get_option_chain
on_request_start = Utils.on_request_start
datetime_to_julian = Utils.datetime_to_julian
julian_to_datetime = Utils.julian_to_datetime
get_instrument_type = Utils.get_instrument_type
on_request_exception = Utils.on_request_exception
get_instrument_details = Utils.get_instrument_details
get_byte_format_length = Utils.get_byte_format_length
get_dhan_trading_symbol = Utils.get_dhan_trading_symbol
get_instruments_details = Utils.get_instruments_details
fetch_and_save_latest_dhan_master_scrip_feather = (
    Utils.fetch_and_save_latest_dhan_master_scrip_feather
)
get_parsed_instruments_attrdict = Utils.get_parsed_instruments_attrdict
generate_async_client_session = Utils.generate_async_client_session
