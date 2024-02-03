from __future__ import annotations
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Literal,
    NoReturn,
    Set,
    Tuple,
)
import os, sys, contextlib, json, queue, asyncio, aiohttp, logging, socket, websockets, websockets.client, signal
from time import sleep
from operator import add
from polars import DataFrame
from threading import Thread
from asyncio import TaskGroup
from Crypto.Cipher import AES
from binascii import unhexlify
from websockets.typing import Data
from urllib.parse import quote_plus
from datetime import timedelta as td
import websockets.exceptions as wsexc
from datetime import datetime as dtdt
from base64 import b64encode, b64decode
from hashlib import sha512, pbkdf2_hmac
from aiohttp.resolver import AsyncResolver
from Crypto.Util.Padding import pad, unpad
from websockets.exceptions import WebSocketException
from websockets.client import WebSocketClientProtocol
from struct import pack, unpack, pack_into, unpack_from, calcsize
from .utils import (
    AttrDict,
    Callback,
    get_logger,
    is_windows,
    LoggerAdapter,
    LOGGING_FORMAT,
    json_stringify,
    on_request_end,
    strip_nullbytes,
    generate_tab_id,
    get_dhan_imei_no,
    on_request_start,
    julian_to_datetime,
    on_request_exception,
    get_byte_format_length,
    ORDER_PACKET_DEFAULT_KEYS,
    generate_async_client_session,
    get_parsed_instruments_attrdict,
    fetch_and_save_latest_dhan_master_scrip_feather,
)

if not is_windows() and sys.version_info >= (3, 8):
    try:
        import uvloop
    except (ImportError, ModuleNotFoundError):
        os.system(f"{sys.executable} -m pip install uvloop")
        import uvloop

    from signal import SIGABRT, SIGINT, SIGTERM, SIGHUP

    SIGNALS = (SIGABRT, SIGINT, SIGTERM, SIGHUP)
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    from signal import SIGABRT, SIGINT, SIGTERM

    SIGNALS = (SIGABRT, SIGINT, SIGTERM)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

__all__ = ["DhanTicker"]

log = logging.getLogger(__name__)


class DhanTicker:
    PING_INTERVAL: float = 30.0
    MAXDELAY: int = 5
    MAXRETRIES: int = 10
    CONNECT_TIMEOUT: int = 30
    CLOSE_TIMEOUT: int = 30

    SUBSCRIBE: str = "SUBSCRIBE"
    UNSUBSCRIBE: str = "UNSUBSCRIBE"
    FULL: str = "FULL"
    QUOTE: str = "QUOTE"

    BROKER_CODE: str = "DHN1804"
    APP_ID: str = "DH_WEB"
    APP_VERSION: str = "v1.0.0.15"
    ROLE: str = "Admin"
    SOURCE: str = "W"
    WEB_VERSION: str = "Chrome Browser"
    PASS_TYPE: str = "DI"
    SALT: str = "nahd"

    PBKDF2_HMAC_HASH = "sha1"
    PASS_TEXT: str = "DHAN"
    SALT_HEX: str = "498960e491150a0fc0f21822a147fd62"
    IV_HEX: str = "320ef7705d1030f0a1a55b3dcf676cb8"
    KEY_SIZE: int = 4
    ITERATIONS: int = 1000

    USER_AGENT: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    LOGIN_ACCEPT: str = "application/json, text/plain, */*"
    LOGIN_CONTENT_TYPE: str = "application/x-www-form-urlencoded"
    LOGIN_AUTHORISATION: str = "Token {jwt_token}"
    PING_PAYLOAD: bytes = b"\x00"
    URL: AttrDict = AttrDict(
        master_scrip_url="https://images.dhan.co/api-data/api-scrip-master.csv",
        login=AttrDict(
            root="https://login-api.dhan.co",
            login="/login/login",
            jwt_token="/jwt/token",
            login_otp_auth="/login/LoginOtpAuth",
            create_session="/login/createSession",
            simplified_login="/login/simplifiedLogin",
            login_generate_otp="/login/LoginGenerateOtp",
            login_feed="wss://qr-login.dhan.co",
            origin="https://login.dhan.co",
            referer="https://login.dhan.co/",
        ),
        web=AttrDict(
            version="https://web-api.dhan.co/user/getVersion",
            origin="https://web.dhan.co",
            price_feed="wss://price-feed-web.dhan.co",
            order_feed="wss://order-feed.dhan.co",
            live_scanner_feed="wss://live-scanner-feed.dhan.co",
        ),
        tv=AttrDict(
            origin="https://tv.dhan.co",
            price_feed="wss://price-feed-tv.dhan.co",
        ),
        otw=AttrDict(
            origin="https://options-trader.dhan.co",
            price_feed="wss://price-feed-otw.dhan.co",
        ),
    )
    RETURN_TYPE: Set[str] = {"bytes", "json", "string"}
    MARKET_STATUS_BY_CODE: Dict[int, str] = {
        1: "PREOPEN",
        3: "OPEN",
        5: "POSTCLOSE",
        6: "CLOSE",
    }
    BYTE_FORMAT: AttrDict = AttrDict(
        initial_data="B2I2B",
        open_packet="bh30s50s500s50s50s10s10s",
        get_market_status="bh30s50s",
        sub_unsub="bh30s50sbib20s20s",
        oi="I",
        ltp="fHIf3I",
        mdepth="2I2H2f2I2H2f2I2H2f2I2H2f2I2H2f",
        ohlc="4f",
        index="5fI",
        top_bid="4I2f",
        heart_beat="bh30s50s",
        market_status="3sf",
        prev_close="2f",
        ckt_limit="2f",
        week52_hl="2f",
        order_update="2h6scc12s30s30s30scc10s5sx5i2dh2x3dhc20s20s20s24s4s300sxhcxdc30scc12s3xi10s15s20s15si4d20s5s40s15s3s20s50s3x2d20s20s10x20s10xd",
    )
    ON_TICK: str = "ON_TICK"
    ON_ORDER_UPDATE: str = "ON_ORDER_UPDATE"
    CLOUDFLARE_DNS: List[str] = ["1.1.1.1", "1.0.0.1"]
    GOOGLE_DNS: List[str] = ["8.8.8.8", "8.8.4.4"]
    DNS_CACHE_SECONDS: int = 22500

    @staticmethod
    def get_signal_r_seg(exchange: str, segment: str) -> int:
        if not segment:
            return -1
        if segment == "I":
            return 0
        if exchange == "NSE":
            match segment:
                case "E":
                    return 1
                case "D":
                    return 2
                case "C":
                    return 3
                case "M":
                    return 10
        elif exchange == "BSE":
            match segment:
                case "E":
                    return 4
                case "C":
                    return 7
                case "D":
                    return 8
                case "M":
                    return 9
                case "I":
                    return 0
        elif exchange == "MCX" and segment == "M":
            return 5
        elif exchange == "NCDEX" and segment == "M":
            return 6
        elif exchange == "IDX" and segment == "I":
            return 0
        elif exchange == "ICEX" and segment == "M":
            return 11
        return -1

    @staticmethod
    def market_status_by_code(status_code: int) -> Optional[str]:
        return DhanTicker.MARKET_STATUS_BY_CODE.get(status_code)

    @staticmethod
    def get_seg_key_n_decimal_points(
        idx: str,
        sec_id: str,
    ) -> AttrDict:
        tempkey, seg, tofix = "", "", 2
        match idx:
            case 0:
                tempkey, seg, tofix = f"{sec_id}-I-IDX", "I", 2
            case 1:
                tempkey, seg, tofix = f"{sec_id}-E-NSE", "E", 2
            case 2:
                tempkey, seg, tofix = f"{sec_id}-D-NSE", "D", 2
            case 3:
                tempkey, seg, tofix = f"{sec_id}-C-NSE", "C", 4
            case 4:
                tempkey, seg, tofix = f"{sec_id}-E-BSE", "", 2
            case 5:
                tempkey, seg, tofix = f"{sec_id}-M-MCX", "M", 2
            case 6:
                tempkey, seg, tofix = f"{sec_id}-M-NCDEX", "M", 2
            case 7:
                tempkey, seg, tofix = f"{sec_id}-C-BSE", "C", 4
            case 8:
                tempkey, seg, tofix = f"{sec_id}-D-BSE", "D", 4
        return AttrDict(Segment=seg, DhanUniqueKey=tempkey, ToFix=tofix)

    @staticmethod
    def process_order_packet(packet: bytes) -> Dict[str, Any]:
        order_response = {
            k: (
                int(v)
                if isinstance(v, str) and v.isdigit()
                else 1
                if k == "lot_size" and (k is None or k == 0)
                else v.replace(" ", "T")
                if k.endswith("_time")
                else v / 100
                if k == "tick_size"
                else v
            )
            for k, v in {
                k: (strip_nullbytes(v.decode("utf-8")) if isinstance(v, bytes) else v)
                for k, v in zip(
                    ORDER_PACKET_DEFAULT_KEYS,
                    DhanTicker._unpack(
                        DhanTicker.BYTE_FORMAT.order_update,
                        packet,
                    ),
                )
            }.items()
        }

        for k in {"remaining_quantity", "traded_qty", "quantity"}:
            order_response[f"disp_{k}"] = (
                order_response[k] // order_response["lot_size"]
            )

        order_response["remarks_1"] = order_response["splatform"]
        order_response["order_label"] = (
            "Basket"
            if "BSKT" in order_response["remarks_1"]
            or "STRGBSK" in order_response["remarks_1"]
            else "Forever"
            if "FOVR" in order_response["remarks_1"]
            else "SIP"
            if "SIP" in order_response["remarks_1"]
            else "TV"
            if "TV_TERMINAL" in order_response["remarks_1"]
            else "Iceberg"
            if "IBERG" in order_response["remarks_1"]
            else ""
        )
        order_response["DhanUniqueKey"] = "-".join(
            map(
                lambda k: str(order_response[k]), ("security_id", "segment", "exchange")
            )
        )
        log.info("Received Order Update: %s", order_response)
        return order_response

    @staticmethod
    def _split_price_packets(frame: bytes) -> List[bytes]:
        packets = []
        while len(frame) > 0:
            (packet_length,) = DhanTicker._unpack("B", frame, 9, 10)
            if packet_length < 11:
                break
            _frame, frame = frame[:packet_length], frame[packet_length:]
            packets.append(_frame)
        return packets

    @staticmethod
    def _split_order_packets(frame: bytes) -> List[bytes]:
        packets = []
        while len(frame) > 0:
            (packet_length,) = DhanTicker._unpack("h", frame, 2, 4)
            _frame, frame = frame[:packet_length], frame[packet_length:]
            packets.append(_frame)
        return packets

    @staticmethod
    def on_open_send_this_packet(
        id: str,
        token: str,
        client_id: str,
        broker_code: str,
        prev_day: bool = False,
    ) -> bytes:
        return DhanTicker._pack(
            41,
            703,
            id.encode("utf-8"),
            token.encode("utf-8"),
            DhanTicker.getWSHashApi(client_id, prev_day=prev_day).encode("utf-8"),
            broker_code.encode("utf-8"),
            client_id.encode("utf-8"),
            b"W",
            b"C",
            byte_format=DhanTicker.BYTE_FORMAT.open_packet,
        )

    @staticmethod
    def send_getmarketstatus_frame(id: str, token: str) -> bytes:
        return DhanTicker._pack(
            35,
            83,
            id.encode("utf-8"),
            token.encode("utf-8"),
            byte_format=DhanTicker.BYTE_FORMAT.get_market_status,
            buffer_len=83,
        )

    @staticmethod
    def send_heartbeat_frame(id: str, token: str) -> bytes:
        return DhanTicker._pack(
            14,
            83,
            id.encode("utf-8"),
            token.encode("utf-8"),
            byte_format=DhanTicker.BYTE_FORMAT.heart_beat,
            buffer_len=83,
        )

    @staticmethod
    def get_ws_send_frames_code(
        instrument: AttrDict,
        message_type: str = SUBSCRIBE,
        mode: str = QUOTE,
    ) -> int:
        match message_type, mode:
            case DhanTicker.SUBSCRIBE, DhanTicker.QUOTE:
                return 24 if instrument.Segment == "I" else 12
            case DhanTicker.UNSUBSCRIBE, DhanTicker.QUOTE:
                return 13
            case DhanTicker.SUBSCRIBE, DhanTicker.FULL:
                return 23
            case DhanTicker.UNSUBSCRIBE, DhanTicker.FULL:
                return 25
            case _:
                return 12
                # (
                #     (24 if instrument.Segment == "I" else 12)
                #     if mode == DhanTicker.QUOTE
                #     else 23
                # )
                # if message_type == DhanTicker.SUBSCRIBE
                # else (13 if mode == DhanTicker.QUOTE else 25)

    @staticmethod
    def get_ws_send_frames(
        id: str,
        token: str,
        instruments: List[AttrDict],
        message_type: str = SUBSCRIBE,
        mode: str = QUOTE,
    ) -> List[bytes]:
        quote_packets = [
            DhanTicker._pack(
                DhanTicker.get_ws_send_frames_code(
                    instrument, message_type, mode=DhanTicker.QUOTE
                ),
                129,
                id.encode("utf-8"),
                token.encode("utf-8"),
                DhanTicker.get_signal_r_seg(instrument.Exchange, instrument.Segment),
                -1,
                1,
                "abc".encode("utf-8"),
                str(instrument.SecurityId).encode("utf-8"),
                byte_format=DhanTicker.BYTE_FORMAT.sub_unsub,
                buffer_len=129,
            )
            for instrument in instruments
        ]
        if mode is DhanTicker.QUOTE:
            return quote_packets
        else:
            full_packet = [
                DhanTicker._pack(
                    DhanTicker.get_ws_send_frames_code(instrument, message_type, mode),
                    129,
                    id.encode("utf-8"),
                    token.encode("utf-8"),
                    DhanTicker.get_signal_r_seg(
                        instrument.Exchange, instrument.Segment
                    ),
                    -1,
                    1,
                    "abc".encode("utf-8"),
                    str(instrument.SecurityId).encode("utf-8"),
                    byte_format=DhanTicker.BYTE_FORMAT.sub_unsub,
                    buffer_len=129,
                )
                for instrument in instruments
            ]
            return [q_f for qf in zip(quote_packets, full_packet) for q_f in qf]

    @staticmethod
    def update_oi(packet: bytes) -> int:
        (oi,) = DhanTicker._unpack(DhanTicker.BYTE_FORMAT.oi, packet)
        return oi

    @staticmethod
    def ltp_bind(
        seg_key: AttrDict, packet: bytes
    ) -> Tuple[float, int, int, float, int, dtdt, dtdt]:
        dp = seg_key.ToFix
        ltp, ltq, volume, avg_price, oi, ltt, lut = DhanTicker._unpack(
            DhanTicker.BYTE_FORMAT.ltp, packet
        )
        ltp, avg_price = round(ltp, dp), round(avg_price, dp)
        return (
            ltp,
            ltq,
            volume,
            avg_price,
            oi,
            julian_to_datetime(ltt),
            julian_to_datetime(lut),
        )

    @staticmethod
    def update_mbp(
        seg_key: AttrDict, packet: bytes
    ) -> Tuple[List[Dict[str, int | float]], List[Dict[str, int | float]]]:
        (
            bq0,
            sq0,
            bo0,
            so0,
            bp0,
            sp0,
            bq1,
            sq1,
            bo1,
            so1,
            bp1,
            sp1,
            bq2,
            sq2,
            bo2,
            so2,
            bp2,
            sp2,
            bq3,
            sq3,
            bo3,
            so3,
            bp3,
            sp3,
            bq4,
            sq4,
            bo4,
            so4,
            bp4,
            sp4,
        ) = DhanTicker._unpack(DhanTicker.BYTE_FORMAT.mdepth, packet)
        return (
            [
                {"Quantity": q, "Price": p, "Orders": o}
                for q, p, o in [
                    (bq0, bp0, bo0),
                    (bq1, bp1, bo1),
                    (bq2, bp2, bo2),
                    (bq3, bp3, bo3),
                    (bq4, bp4, bo4),
                ]
            ],
            [
                {"Quantity": q, "Price": p, "Orders": o}
                for q, p, o in [
                    (sq0, sp0, so0),
                    (sq1, sp1, so1),
                    (sq2, sp2, so2),
                    (sq3, sp3, so3),
                    (sq4, sp4, so4),
                ]
            ],
        )

    @staticmethod
    def update_ohlc(
        seg_key: AttrDict, packet: bytes
    ) -> Tuple[float, float, float, float]:
        dp = seg_key.ToFix
        open, close, high, low = DhanTicker._unpack(DhanTicker.BYTE_FORMAT.ohlc, packet)
        return round(open, dp), round(high, dp), round(low, dp), round(close, dp)

    @staticmethod
    def bind_index_bc(
        seg_key: AttrDict, packet: bytes
    ) -> Tuple[float, float, float, float, float, dtdt, float, float]:
        dp = seg_key.ToFix
        ltp, open, prev_close, high, low, ltt = DhanTicker._unpack(
            DhanTicker.BYTE_FORMAT.index, packet
        )
        ltp, open, prev_close, high, low, ltt = (
            round(ltp, dp),
            round(open, dp),
            round(prev_close, dp),
            round(high, dp),
            round(low, dp),
            julian_to_datetime(ltt),
        )
        change = 0.0 if prev_close <= 0 else round(ltp - prev_close, dp)
        change_prcnt = (
            0.0 if prev_close <= 0 else round(100 * (change / prev_close), dp)
        )
        return ltp, open, prev_close, high, low, ltt, change, change_prcnt

    @staticmethod
    def update_top_bid_ask(
        seg_key: AttrDict, packet: bytes
    ) -> Tuple[int, int, int, int, float, float, int, int, int]:
        dp, tbqp, tsqp = seg_key.ToFix, 0, 0
        tsq, tbq, bbq, baq, bbp, bap = DhanTicker._unpack(
            DhanTicker.BYTE_FORMAT.top_bid, packet
        )
        tbsq, bbp, bap = tsq + tbq, round(bbp, dp), round(bap, dp)
        with contextlib.suppress(ZeroDivisionError):
            tbqp = round((tbq / tbsq) * 100, dp)
            tsqp = round((tsq / tbsq) * 100, dp)
        return tsq, tbq, bbq, baq, bbp, bap, tbsq, tbqp, tsqp

    @staticmethod
    def update_mkt_status(
        seg_key: AttrDict, packet: bytes
    ) -> Tuple[str | bytes, float, str | None]:
        message, value = DhanTicker._unpack(
            DhanTicker.BYTE_FORMAT.market_status, packet
        )
        message, value = strip_nullbytes(message), round(value, seg_key.ToFix)
        return message, value, DhanTicker.market_status_by_code(int(value))

    @staticmethod
    def prev_close_bind(seg_key: AttrDict, packet: bytes) -> Tuple[float, float]:
        dp = seg_key.ToFix
        prev_close, prev_oi = DhanTicker._unpack(
            DhanTicker.BYTE_FORMAT.prev_close, packet
        )
        return round(prev_close, dp), round(prev_oi, dp)

    @staticmethod
    def update_ckt_limit(seg_key: AttrDict, packet: bytes) -> Tuple[float, float]:
        dp = seg_key.ToFix
        up_limit, lwr_limit = DhanTicker._unpack(
            DhanTicker.BYTE_FORMAT.ckt_limit, packet
        )
        return round(up_limit, dp), round(lwr_limit, dp)

    @staticmethod
    def update_52week_hl_bind(
        seg_key: AttrDict,
        packet: bytes,
    ) -> Tuple[float, float]:
        dp = seg_key.ToFix
        high_52_wk, low_52_wk = DhanTicker._unpack(
            DhanTicker.BYTE_FORMAT.week52_hl, packet
        )
        return round(high_52_wk, dp), round(low_52_wk, dp)

    @staticmethod
    def calc_chg_chgprcnt(
        seg_key: AttrDict, current: int | float, prev: int | float
    ) -> Tuple[int | float, int | float]:
        dp = seg_key.ToFix
        change = 0.0 if prev <= 0 else round(current - prev, dp)
        change_prcnt = 0.0 if prev <= 0 else round(100 * (change / prev), dp)
        return (
            (round(change, dp), round(change_prcnt, dp))
            if isinstance(current, float) and isinstance(prev, float)
            else (int(change), int(change_prcnt))
        )

    @staticmethod
    def _unpack(
        byte_format: str, buffer: bytes, offset: int = 0, until: int = 0
    ) -> Tuple[Any, ...]:
        if offset > 0 and until == 0:
            return unpack_from(f"<{byte_format}", buffer, offset=offset)
        if offset >= 0 and until > 0:
            return unpack(f"<{byte_format}", buffer[offset:until])
        return unpack(f"<{byte_format}", buffer)

    @staticmethod
    def _pack(*v, byte_format: str, buffer_len: int = 0) -> bytes:
        if buffer_len > 0 and buffer_len >= calcsize(f"<{byte_format}"):
            buffer = memoryview(bytearray(buffer_len))
            pack_into(f"<{byte_format}", buffer, 0, *v)
            return buffer.tobytes()
        return pack(f"<{byte_format}", *v)

    @staticmethod
    def generate_key(
        salt: bytes, password: bytes, key_size: int, iterations: int
    ) -> bytes:
        return pbkdf2_hmac(
            hash_name=DhanTicker.PBKDF2_HMAC_HASH,
            password=password,
            salt=salt,
            iterations=iterations,
            dklen=32 * key_size,
        )[:16]

    @staticmethod
    def encrypt_aes_cbc_256(text_to_cipher: str, key_bytes: bytes, iv: bytes) -> bytes:
        cipher = AES.new(key_bytes, AES.MODE_CBC, iv)
        return b64encode(
            cipher.encrypt(
                pad(
                    text_to_cipher.encode("utf-8"),
                    cipher.block_size
                    if cipher.block_size == AES.block_size
                    else AES.block_size,
                )
            )
        )

    @staticmethod
    def decrypt_aes_cbc_256(ciphered_text: str, key: bytes, iv: bytes) -> bytes:
        cipher = AES.new(key, AES.MODE_CBC, iv)
        return unpad(
            cipher.decrypt(b64decode(ciphered_text)),
            cipher.block_size
            if cipher.block_size == AES.block_size
            else AES.block_size,
        )

    @staticmethod
    def decode_user_data(
        ciphered_text: str,
        return_as: Literal["bytes", "dict", "str", "attrdict"] = "str",
    ) -> bytes | str | Dict[str, Any]:
        return_type = {"bytes", "dict", "str", "attrdict"}
        if return_as not in return_type:
            raise ValueError(
                "Wrong `return_as` param value, Valid ones are one of these `bytes`, `dict`, `attrdict` and `str`"
            )
        decrypted_data = DhanTicker.decrypt_aes_cbc_256(
            ciphered_text,
            DhanTicker.generate_key(
                unhexlify(DhanTicker.SALT_HEX),
                DhanTicker.PASS_TEXT.encode("utf-8"),
                DhanTicker.KEY_SIZE,
                DhanTicker.ITERATIONS,
            ),
            unhexlify(DhanTicker.IV_HEX),
        )
        return (
            json.loads(decrypted_data.decode("utf-8"), object_hook=AttrDict)
            if return_as == "attrdict"
            else json.loads(decrypted_data.decode("utf-8"))
            if return_as == "dict"
            else decrypted_data
            if return_as == "bytes"
            else decrypted_data.decode("utf-8")
        )

    @staticmethod
    def encrypt_data(text_to_cipher: str, return_as: str = "string") -> str | bytes:
        encrypted_data = DhanTicker.encrypt_aes_cbc_256(
            text_to_cipher,
            DhanTicker.generate_key(
                unhexlify(DhanTicker.SALT_HEX),
                DhanTicker.PASS_TEXT.encode("utf-8"),
                DhanTicker.KEY_SIZE,
                DhanTicker.ITERATIONS,
            ),
            unhexlify(DhanTicker.IV_HEX),
        )
        return (
            encrypted_data.decode("utf-8")
            if return_as.lower() == "string"
            else encrypted_data
        )

    @staticmethod
    def getWSHashApi(client_id: str, prev_day: bool = False) -> str:
        keyword = f"{DhanTicker.BROKER_CODE}-{client_id}-W-C-" + str(
            int(
                (
                    (
                        dtdt.now().replace(hour=0, minute=0, second=0, microsecond=0)
                        - td(days=1)
                    )
                    if prev_day
                    else dtdt.now().replace(hour=0, minute=0, second=0, microsecond=0)
                ).timestamp()
                - dtdt(
                    year=1980, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
                ).timestamp()
            )
        )
        return b64encode(sha512(keyword.encode("utf-8")).digest()).decode("utf-8")

    @staticmethod
    def default_callbacks() -> Dict[str, Optional[Callable]]:
        return dict.fromkeys(
            (DhanTicker.ON_TICK.lower(), DhanTicker.ON_ORDER_UPDATE.lower())
        )

    @staticmethod
    def _start_background_loop(loop: asyncio.AbstractEventLoop) -> Optional[NoReturn]:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            loop.run_until_complete(loop.shutdown_asyncgens())
            if loop.is_running():
                loop.stop()
            if not loop.is_closed():
                loop.close()

    def __aenter__(self) -> "DhanTicker":
        return self

    def __enter__(self) -> "DhanTicker":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._graceful_exit()

    def __del__(self) -> None:
        self._graceful_exit()

    def __delete__(self) -> None:
        self._graceful_exit()

    def __init__(
        self,
        userdata: Optional[str] = None,
        debug: bool = False,
        debug_verbose: bool = False,
        callbacks: Optional[Dict[str, Callback | Callable]] = None,
    ) -> None:
        fetch_and_save_latest_dhan_master_scrip_feather()
        self.debug = debug
        self.debug_verbose = debug_verbose
        self.log_level = (
            logging.INFO
            if self.debug
            else logging.DEBUG
            if self.debug_verbose
            else logging.WARNING
        )
        logging.getLogger("websockets").addHandler(logging.NullHandler())
        logging.getLogger("aiohttp.client").addHandler(logging.NullHandler())
        if self.debug:
            logging.basicConfig(format=LOGGING_FORMAT, level=self.log_level)
            logging.getLogger("aiohttp.client").propagate = False
            logging.getLogger("aiohttp.client").setLevel(self.log_level)
            logging.getLogger("websockets").propagate = False
            logging.getLogger("websockets").setLevel(logging.WARNING)
        if self.debug_verbose:
            logging.basicConfig(format=LOGGING_FORMAT, level=self.log_level)
            logging.getLogger("aiohttp.client").propagate = self.debug_verbose
            logging.getLogger("aiohttp.client").setLevel(self.log_level)
            logging.getLogger("websockets").propagate = self.debug_verbose
            logging.getLogger("websockets").setLevel(self.log_level)
        self.callbacks = callbacks
        self.on_tick = None
        self.on_order_update = None
        self.price_feed, self.order_feed = None, None
        self.order_feed_running, self.price_feed_running = False, False
        self.should_run = True
        self.stop_stream_queue = queue.Queue()
        self.price_feed_url = self.URL.tv.price_feed
        self.order_feed_url = self.URL.web.order_feed
        self.subscribed_tokens = AttrDict()
        self.market_status = AttrDict(
            {"Msg": None, "Value": None, "MarketStatus": None}
        )
        self.__initialize_loop()
        if self.callbacks is not None and isinstance(self.callbacks, dict):
            for k, v in self.callbacks.items():
                if k.lower() == DhanTicker.ON_TICK.lower():
                    self.on_tick = (
                        Callback(v, loop=self._loop) if isinstance(v, Callable) else v
                    )
                if k.lower() == DhanTicker.ON_ORDER_UPDATE.lower():
                    self.on_order_update = (
                        Callback(v, loop=self._loop) if isinstance(v, Callable) else v
                    )
        if userdata is not None and isinstance(userdata, str):
            self.login_with_userdata(userdata)
        else:
            self._initialize_session_params()

    def login_with_userdata(self, userdata: str) -> None:
        try:
            self.userdata = self.decode_user_data(userdata, return_as="attrdict")
        except Exception:
            log.info(
                "Failed To Decode The Provided `userdata`, Please Check And Try Again With A Valid `userdata`"
            )
            self._graceful_exit()
        else:
            self.__update_credentials_and_run_main()

    def login_with_credentials(self, login_id: str | int, password: str) -> None:
        future = asyncio.run_coroutine_threadsafe(
            self.__login_with_credentials(login_id, password),
            self._loop,
        )
        try:
            future.result(timeout=60.0)
        except TimeoutError:
            error_message = f"The Whole Login With Credentials & OTP Took Longer Than The Default Timeout To Wait For The Response, i.e. {float(60.0):.2f} Seconds, Cancelling The Task..."
            log.error(error_message)
            future.cancel()
        except Exception as exc:
            error_message = f"The Login With Credentials & OTP Method Call Has Ended Up With An Exception: {exc!r} {future.exception(1.0)}"
            log.exception(error_message)
        else:
            self.__update_credentials_and_run_main()

    def _graceful_exit(self) -> None:
        with contextlib.suppress(RuntimeError, RuntimeWarning):
            self.stop()
            if hasattr(self, "__reqsession") and self.__reqsession is not None:
                asyncio.run_coroutine_threadsafe(
                    asyncio.ensure_future(self.__reqsession.close()), self._loop
                )
                asyncio.run_coroutine_threadsafe(asyncio.sleep(0.001), self._loop)
            asyncio.run_coroutine_threadsafe(
                self._loop.shutdown_asyncgens(), self._loop
            )
            if self._loop.is_running():
                self._loop.stop()
            if not self._loop.is_closed():
                self._loop.close()

    def _handle_stop_signals(self, *args, **kwargs):
        try:
            self._graceful_exit()
        except Exception as err:
            log.error(str(err))
        else:
            exit()

    def __update_credentials_and_run_main(self) -> None:
        if isinstance(self.userdata, AttrDict):
            self.tab_id = generate_tab_id(6)
            self.entity_id = self.userdata.entity_id
            self.token_id = self.userdata.token_id
            self.price_feed_client_id = "".join(map(str, (self.entity_id, self.tab_id)))
            self.order_feed_client_id = self.entity_id
            self.price_feed_id = "-".join(
                (self.price_feed_client_id, self.SOURCE, self.BROKER_CODE)
            )
            self.order_feed_id = self.entity_id
            self.price_feed_token = f"{self.token_id}{self.tab_id}"
            self.order_feed_token = f"{self.token_id}{self.tab_id}"
            self.order_feed_url += f"?src={self.SOURCE}&id={self.order_feed_id}"
            self.run()

    def __initialize_loop(self) -> None:
        if is_windows():
            self._loop = asyncio.new_event_loop()
            with contextlib.suppress(ValueError):
                for sig in SIGNALS:
                    signal.signal(sig, self._handle_stop_signals)
        else:
            self._loop = uvloop.new_event_loop()
            with contextlib.suppress(ValueError):
                for sig in SIGNALS:
                    self._loop.add_signal_handler(sig, self._handle_stop_signals)  # noqa E501
        self._event_thread = Thread(
            target=self._start_background_loop,
            args=(self._loop,),
            name=f"{self.__class__.__name__}_event_thread",
            daemon=True,
        )
        self._event_thread.start()
        log.info("DhanTicker Event Loop has been initialized.")

    async def __initialize_session_params(self) -> None:
        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        trace_config.on_request_exception.append(on_request_exception)
        self.__reqsession = await generate_async_client_session(
            base_url=self.URL.login.root,
            connector=aiohttp.TCPConnector(
                limit=0,
                limit_per_host=0,
                ttl_dns_cache=self.DNS_CACHE_SECONDS,
                use_dns_cache=True,
                resolver=AsyncResolver(nameservers=self.CLOUDFLARE_DNS),
                family=socket.AF_INET,
            ),
            headers={
                "Accept": DhanTicker.LOGIN_ACCEPT,
                "Content-Type": DhanTicker.LOGIN_CONTENT_TYPE,
                "Origin": DhanTicker.URL.login.origin,
                "Referer": DhanTicker.URL.login.referer,
                "User-Agent": DhanTicker.USER_AGENT,
            },
            timeout=aiohttp.ClientTimeout(total=float(self.CONNECT_TIMEOUT)),
            raise_for_status=True,
            trust_env=True,
            trace_configs=[trace_config] if self.debug or self.debug_verbose else None,
            cookie_jar=aiohttp.CookieJar(),
        )
        await self.__reqsession.get("/")

    def _initialize_session_params(self) -> None:
        future = asyncio.run_coroutine_threadsafe(
            self.__initialize_session_params(),
            self._loop,
        )
        try:
            future.result(5.0)
        except TimeoutError:
            error_message = f"The Initialization of Async Client Session Took Longer Than The Default Timeout To Wait For The Response, i.e. {float(5.0):.2f} Seconds, Cancelling The Task..."
            log.error(error_message)
            future.cancel()
        except Exception as exc:
            error_message = f"The Initialization of Async Client Session Ended Up With An Exception: {exc!r} {future.exception(1.0)}"
            log.exception(error_message)

    def get_origin(self, endpoint: str, order_feed: bool = False):
        return (
            DhanTicker.URL.web.origin
            if order_feed
            else (
                DhanTicker.URL.tv.origin
                if "-tv" in self.price_feed_url
                else DhanTicker.URL.otw.origin
                if "-otw" in self.price_feed_url
                else DhanTicker.URL.web.origin
            )
        )

    async def get_ws_client(
        self,
        order_feed: bool = False,
    ) -> WebSocketClientProtocol:
        uri = self.order_feed_url if order_feed else self.price_feed_url
        return await websockets.client.connect(
            uri,
            logger=LoggerAdapter(
                get_logger("websockets.client", "websocket.log", level=self.log_level),
                None,
            )
            if self.debug or self.debug_verbose
            else None,
            origin=self.get_origin(uri, order_feed=order_feed),
            user_agent_header=self.USER_AGENT,
            open_timeout=self.CONNECT_TIMEOUT,
            ping_interval=self.PING_INTERVAL,
            ping_timeout=None,
            close_timeout=self.CLOSE_TIMEOUT,
        )

    async def __on_open(self, order_feed: bool = False) -> None:
        if not order_feed and self.price_feed is not None and self.price_feed_running:
            packet = self.on_open_send_this_packet(
                self.price_feed_id,
                self.price_feed_token,
                self.price_feed_client_id,
                self.BROKER_CODE,
            )
            log.info("Sending On Open Packet To Price Feed.")
            await self.price_feed.send(packet)
            log.info("On Open Packet Sent To Price Feed Successfully")
        if order_feed and self.order_feed is not None and self.order_feed_running:
            packet = self.on_open_send_this_packet(
                self.order_feed_id,
                self.order_feed_token,
                self.order_feed_client_id,
                self.BROKER_CODE,
            )
            log.info("Sending On Open Packet To Order Feed.")
            await self.order_feed.send(packet)
            log.info("On Open Packet Sent To Order Feed Successfully.")

    def run(self) -> None:
        try:
            if self._loop.is_running():
                self.run_future = asyncio.run_coroutine_threadsafe(
                    self.run_forever(), self._loop
                )
        except KeyboardInterrupt:
            log.info("Keyboard Interrupt Detected, Bye...")

    def stop(self) -> None:
        if self._loop.is_running():
            if self.run_future.running():
                self.run_future.cancel()
                while not self.run_future.done():
                    sleep(0.025)
            asyncio.run_coroutine_threadsafe(
                self.stop_ws(),
                self._loop,
            ).result()

    async def start_ws(self) -> None:
        await self.connect()

    async def stop_ws(self) -> None:
        self.should_run = False
        if self.stop_stream_queue.empty():
            self.stop_stream_queue.put_nowait({"should_stop": True})

    async def close(self, with_exceptions: bool = False) -> None:
        log_msg = "Facing An Error, " if with_exceptions else ""
        if self.order_feed and self.price_feed:
            await self.__unsubscribe(is_reconnect=not with_exceptions)
            await asyncio.sleep(0.25)
            async with TaskGroup() as tg:
                tasks = [
                    tg.create_task(self.price_feed.close()),
                    tg.create_task(self.order_feed.close()),
                ]
            [task.result() for task in tasks if task.done()]
            await asyncio.sleep(0.25)
            self.order_feed_running, self.price_feed_running = False, False
            self.price_feed, self.order_feed = None, None
            log_msg += "Websocket Disconnected For Price Feed Endpoint: %s And Order Feed Endpoint: %s"
            log.info(log_msg, self.price_feed_url, self.order_feed_url)
            return
        if self.order_feed and not self.price_feed:
            await self.order_feed.close()
            await asyncio.sleep(0.1)
            self.order_feed_running, self.order_feed = False, None
            log_msg += "Websocket Disconnected For Order Feed Endpoint: %s"
            log.info(log_msg, self.order_feed_url)
            return
        if self.price_feed and not self.order_feed:
            await self.__unsubscribe(is_reconnect=not with_exceptions)
            await asyncio.sleep(0.25)
            await self.price_feed.close()
            await asyncio.sleep(0.1)
            self.price_feed_running, self.price_feed = False, None
            log_msg += "Websocket Disconnected For Price Feed Endpoint: %s"
            log.info(log_msg, self.price_feed_url)
            return

    async def connect(self) -> None:
        log.info("Connecting To Websocket Endpoint: %s", self.price_feed_url)
        self.price_feed = await self.get_ws_client()
        log.info("Connecting To Websocket Endpoint: %s", self.order_feed_url)
        self.order_feed = await self.get_ws_client(order_feed=True)
        await asyncio.sleep(0.25)
        if self.price_feed is not None and isinstance(
            self.price_feed, WebSocketClientProtocol
        ):
            self.price_feed_running = True
            log.info(
                "Websocket Endpoint: %s Connected Successfully.", self.price_feed_url
            )
            await self.__on_open()
        if self.order_feed is not None and isinstance(
            self.order_feed, WebSocketClientProtocol
        ):
            self.order_feed_running = True
            log.info(
                "Websocket Endpoint: %s Connected Successfully.", self.order_feed_url
            )
            await self.__on_open(order_feed=True)

    async def run_forever(self) -> None:
        reconnect_sleep_time: float = 0.05
        while True:
            if not self.should_run:
                log.info("Order and Price Feeds Stopped Successfully.")
                return
            if not (self.order_feed_running and self.price_feed_running):
                log.info("Initializing Websocket Stream For Order and Price Feeds")
                await self.start_ws()
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.ping())
                    tg.create_task(self.consume())
                    tg.create_task(self.ping(order_feed=True))
                    tg.create_task(self.consume(order_feed=True))
            except* WebSocketException as wse:
                log.warning(
                    "Order or/and Price Feeds Websocket Has Met With An Exception, During Its Communication With Dhan Servers."
                )
                log.exception(wse, exc_info=True)
                await self.close(with_exceptions=True)
                await asyncio.sleep(reconnect_sleep_time)
                reconnect_sleep_time += 0.05
            except* Exception as exc:
                log.critical(
                    "An Exception Has Occured While Consuming The Stream From Order or/and Price Feeds. Please Check For Correction In The Callbacks If Any."
                )
                log.exception(exc, exc_info=True)
            finally:
                await asyncio.sleep(0.001)

    async def ping(self, order_feed: bool = False) -> None:
        while True:
            if not self.stop_stream_queue.empty():
                self.stop_stream_queue.get(timeout=1.0)
                await self.close()
                break
            try:
                async with asyncio.timeout(self.PING_INTERVAL - 5):
                    await asyncio.sleep(self.PING_INTERVAL)
            except asyncio.TimeoutError:
                with contextlib.suppress(
                    wsexc.ConnectionClosed, RuntimeError, RuntimeWarning
                ):
                    if order_feed and self.order_feed is not None:
                        await self.order_feed.ping(data=self.PING_PAYLOAD)
                        log.info("Order Feed Pinged")
                    if not order_feed and self.price_feed is not None:
                        await self.price_feed.ping(data=self.PING_PAYLOAD)
                        log.info("Price Feed Pinged")
                        await self.send_heartbeat_to_price_feed()
                        await self.price_feed.send(
                            self.send_getmarketstatus_frame(
                                self.price_feed_id, self.price_feed_token
                            )
                        )
                        log.info("Market Status Requested")
                    # pong_waiter = await self.price_feed.ping(data=self.PING_PAYLOAD)
                    # A future that will be completed when the corresponding pong is received. You can ignore it if you don’t intend to wait. The result of the future is the latency of the connection in seconds.
                    # Here we do not want to wait for the corresponding pong packet so commenting the below line.
                    # latency = await pong_waiter
            finally:
                await asyncio.sleep(0.001)

    async def consume(self, order_feed: bool = False) -> None:
        while True:
            data = None
            if not self.stop_stream_queue.empty():
                self.stop_stream_queue.get(timeout=1.0)
                await self.close()
                return
            try:
                async with asyncio.timeout(5.0):
                    if order_feed and self.order_feed is not None:
                        data = await self.order_feed.recv()
                    if not order_feed and self.price_feed is not None:
                        data = await self.price_feed.recv()
            except asyncio.TimeoutError:
                continue
            else:
                if data is not None:
                    await self.dispatch(data, order_feed=order_feed)
            finally:
                await asyncio.sleep(0.001)

    async def cast(
        self,
        to: Literal["ON_TICK", "ON_ORDER_UPDATE"],
        data: Data | Dict[str, Any] | List[AttrDict] | List[Dict[str, Any]],
    ):
        match to.upper():
            case "ON_TICK":
                log.info("Casting on `on_tick` callback.")
                if self.on_tick is not None and isinstance(self.on_tick, Callback):
                    log.info("Calling `on_tick` callback with data: %s", data)
                    await self.on_tick(data)
            case "ON_ORDER_UPDATE":
                log.info("Casting on `on_order_update` callback.")
                if self.on_order_update is not None and isinstance(
                    self.on_order_update, Callback
                ):
                    log.info("Calling `on_order_update` callback with data: %s", data)
                    await self.on_order_update(data)

    async def dispatch(self, data: Data, order_feed: bool = False) -> None:
        if order_feed and isinstance(data, bytes):
            await self.process_order_frame(data)
        if not order_feed and isinstance(data, bytes):
            await self.process_price_frame(data)

    async def process_order_frame(self, frame: Data) -> None:
        if isinstance(frame, bytes):
            order_update_datas = [
                AttrDict(self.process_order_packet(packet))
                for packet in self._split_order_packets(frame)
            ]
            async with TaskGroup() as tg:
                tasks = [
                    tg.create_task(
                        self.__set_order_updates(
                            order_update_datas,
                        )
                    ),
                    tg.create_task(
                        self.cast(
                            "ON_ORDER_UPDATE",
                            order_update_datas,
                        )
                    ),
                ]
            [task.result() for task in tasks if task.done()]

    async def __set_order_updates(
        self,
        order_update_datas: List[AttrDict],
    ) -> None:
        [
            self.subscribed_tokens.__setitem__(
                order_update_data["DhanUniqueKey"],
                get_parsed_instruments_attrdict([order_update_data["DhanUniqueKey"]])[
                    0
                ],
            )
            for order_update_data in order_update_datas
            if order_update_data["DhanUniqueKey"] not in self.subscribed_tokens
        ]
        [
            self.subscribed_tokens[order_update_data["DhanUniqueKey"]].__setitem__(
                "OrderUpdates",
                order_update_data,
            )
            for order_update_data in order_update_datas
        ]

    async def __subscribe(self) -> None:
        subscription_frames = add(
            *[
                self.get_ws_send_frames(
                    self.price_feed_id,
                    self.price_feed_token,
                    [i for i in self.subscribed_tokens.values() if i.Mode == mode],
                    mode=mode,
                )
                for mode in (DhanTicker.QUOTE, DhanTicker.FULL)
            ]
        )
        await self._subscribe(subscription_frames)
        log.info("Instruments Resubscribed Successfully.")

    async def _subscribe(
        self,
        subscription_frames: List[bytes],
    ) -> None:
        for i in range(1, 6):
            if not self.price_feed_running:
                sleep_duration = i + i * 0.1
                await asyncio.sleep(sleep_duration)
                log.error(
                    "Price Feed Websocket Is Not Running, Sleeping For %s Second, Then Will Retry Subscribing Instruments. Retry No.: %s",
                    str(sleep_duration),
                    str(i),
                )
        if self.price_feed is not None and self.price_feed_running:
            async with TaskGroup() as tg:
                tasks = [
                    tg.create_task(self.price_feed.send(subscription_frame))
                    for subscription_frame in subscription_frames
                ]
            [task.result() for task in tasks if task.done()]
            log.info("Instruments Subscription Packets Sent Successfully.")
        else:
            log.error("Price Feed Websocket Is Not Running, Hence Can't Subscribe.")

    def subscribe(
        self,
        instruments: List[str] | List[Dict[str, Any]] | DataFrame,
        mode: str = QUOTE,
    ) -> None:
        to_be_subscribed = get_parsed_instruments_attrdict(instruments)
        log.info("Instruments To Be Subscribed: %s", to_be_subscribed)
        subscription_frames = self.get_ws_send_frames(
            self.price_feed_id, self.price_feed_token, to_be_subscribed, mode=mode
        )
        future = asyncio.run_coroutine_threadsafe(
            self._subscribe(subscription_frames), self._loop
        )
        try:
            future.result(timeout=5.0)
        except TimeoutError:
            log.error(
                "The instruments subscription tasks took too long, cancelling the task..."
            )
            future.cancel()
        except Exception as exc:
            log.exception(
                "The instruments subscription tasks has raised an exception: %s",
                exc,
            )
        else:
            [inst.update({"Mode": mode}) for inst in to_be_subscribed]
            self.subscribed_tokens.update(
                {inst.DhanUniqueKey: inst for inst in to_be_subscribed}
            )
            log.info("Instruments Subscribed Successfully.")

    async def __unsubscribe(self, is_reconnect: bool = True) -> None:
        unsubscription_frames = add(
            *[
                self.get_ws_send_frames(
                    self.price_feed_id,
                    self.price_feed_token,
                    [i for i in self.subscribed_tokens.values() if i.Mode == mode],
                    message_type=DhanTicker.UNSUBSCRIBE,
                    mode=mode,
                )
                for mode in (DhanTicker.QUOTE, DhanTicker.FULL)
            ]
        )
        await self._unsubscribe(unsubscription_frames)
        if not is_reconnect:
            self.subscribed_tokens = AttrDict()
        log.info("Instruments Unsubscribed Successfully.")

    async def _unsubscribe(
        self,
        unsubscription_frames: List[bytes],
    ) -> None:
        for i in range(1, 6):
            if not self.price_feed_running:
                sleep_duration = i + i * 0.1
                await asyncio.sleep(sleep_duration)
                log.error(
                    "Price Feed Websocket Is Not Running, Sleeping For %s Second, Then Will Retry Unsubscribing Instruments. Retry No.: %s",
                    str(sleep_duration),
                    str(i),
                )
        if self.price_feed is not None and self.price_feed_running:
            async with TaskGroup() as tg:
                tasks = [
                    tg.create_task(self.price_feed.send(unsubscription_frame))
                    for unsubscription_frame in unsubscription_frames
                ]
            [task.result() for task in tasks if task.done()]
            log.info("Instruments Unsubscription Packets Sent Successfully.")
            return
        else:
            log.error("Price Feed Websocket Is Not Running, Hence Can't Unsubscribe.")

    def unsubscribe(
        self,
        instruments: List[str] | List[Dict[str, Any]] | DataFrame,
        mode: str = QUOTE,
    ) -> None:
        to_be_unsubscribed = [
            instrument
            for instrument in get_parsed_instruments_attrdict(instruments)
            if instrument.DhanUniqueKey in self.subscribed_tokens
        ]
        log.info("Instruments To Be Unsubscribed: %s", to_be_unsubscribed)
        unsubscription_frames = self.get_ws_send_frames(
            self.price_feed_id,
            self.price_feed_token,
            to_be_unsubscribed,
            message_type=DhanTicker.UNSUBSCRIBE,
            mode=mode,
        )
        future = asyncio.run_coroutine_threadsafe(
            self._subscribe(unsubscription_frames), self._loop
        )
        try:
            future.result(timeout=5.0)
        except TimeoutError:
            log.error(
                "The instruments unsubscription tasks took too long, cancelling the task..."
            )
            future.cancel()
        except Exception as exc:
            log.exception(
                "The instruments unsubscription tasks has raised an exception: %s",
                exc,
            )
        else:
            [
                self.subscribed_tokens.pop(inst.DhanUniqueKey)
                for inst in to_be_unsubscribed
            ]
            log.info("Instruments Unsubscribed Successfully.")

    async def send_heartbeat_to_price_feed(self) -> None:
        if self.price_feed is not None and self.price_feed_running:
            packet = self.send_heartbeat_frame(
                self.price_feed_id,
                self.price_feed_token,
            )
            await self.price_feed.send(packet)
            log.info("Heart Beat Packet Sent Successfully.")

    async def process_price_frame(self, frame: Data) -> None:
        if isinstance(frame, bytes):
            async with TaskGroup() as tg:
                tasks = [
                    tg.create_task(self.process_price_packet(packet))
                    for packet in DhanTicker._split_price_packets(frame)
                ]
            [task.result() for task in tasks if task.done()]

    async def process_price_packet(self, packet: bytes) -> None:
        initial_data_len = get_byte_format_length(DhanTicker.BYTE_FORMAT.initial_data)
        _packet, packet = packet[:initial_data_len], packet[initial_data_len:]
        (idx, sec_id, sec_id, packet_length, packet_type) = DhanTicker._unpack(
            DhanTicker.BYTE_FORMAT.initial_data, _packet
        )
        seg_key = DhanTicker.get_seg_key_n_decimal_points(idx, sec_id)
        key = seg_key.DhanUniqueKey
        if key not in self.subscribed_tokens:
            self.subscribed_tokens[key] = get_parsed_instruments_attrdict([key])[0]
        tradingsymbol = self.subscribed_tokens[key].TradingSymbol
        match packet_type:
            case 1:
                (
                    self.subscribed_tokens[key].PriceUpdates.Ltp,
                    self.subscribed_tokens[key].PriceUpdates.LastTradedQty,
                    self.subscribed_tokens[key].PriceUpdates.Volume,
                    self.subscribed_tokens[key].PriceUpdates.AveragePrice,
                    self.subscribed_tokens[key].PriceUpdates.OpenInterest,
                    self.subscribed_tokens[key].PriceUpdates.LastTradeTime,
                    self.subscribed_tokens[key].PriceUpdates.LastUpdatedTime,
                ) = DhanTicker.ltp_bind(seg_key, packet)
                if self.subscribed_tokens[key].PriceUpdates.OpenInterest == 4294967295:
                    self.subscribed_tokens[key].PriceUpdates.OpenInterest = 0
                (
                    self.subscribed_tokens[key].PriceUpdates.Change,
                    self.subscribed_tokens[key].PriceUpdates.ChangePrcnt,
                ) = self.calc_chg_chgprcnt(
                    seg_key,
                    self.subscribed_tokens[key].PriceUpdates.Ltp,
                    self.subscribed_tokens[key].PriceUpdates.PrevClose,
                )
                (
                    self.subscribed_tokens[key].PriceUpdates.OpenInterestChg,
                    self.subscribed_tokens[key].PriceUpdates.OpenInterestChgPrcnt,
                ) = self.calc_chg_chgprcnt(
                    seg_key,
                    self.subscribed_tokens[key].PriceUpdates.OpenInterest,
                    self.subscribed_tokens[key].PriceUpdates.PrevOpenInterest,
                )
                log.info(
                    "Updated %s, LTP Chnages & OHLC Deatils. Latest Data: %s",
                    tradingsymbol,
                    self.subscribed_tokens[key].PriceUpdates,
                )
            case 2:
                (
                    self.subscribed_tokens[key].PriceUpdates.MarketDepth.Buy,
                    self.subscribed_tokens[key].PriceUpdates.MarketDepth.Sell,
                ) = DhanTicker.update_mbp(seg_key, packet)
                log.info(
                    "Updated %s, Market Depth Deatils. Latest Data: %s",
                    tradingsymbol,
                    self.subscribed_tokens[key],
                )
            case 3:
                (
                    self.subscribed_tokens[key].PriceUpdates.Open,
                    self.subscribed_tokens[key].PriceUpdates.High,
                    self.subscribed_tokens[key].PriceUpdates.Low,
                    self.subscribed_tokens[key].PriceUpdates.Close,
                ) = DhanTicker.update_ohlc(seg_key, packet)
                log.info(
                    "Updated %s, Open, High, Low, Close. Latest Data: %s",
                    tradingsymbol,
                    self.subscribed_tokens[key].PriceUpdates,
                )
            case 5:
                (
                    self.subscribed_tokens[key].PriceUpdates.Ltp,
                    self.subscribed_tokens[key].PriceUpdates.Open,
                    self.subscribed_tokens[key].PriceUpdates.PrevClose,
                    self.subscribed_tokens[key].PriceUpdates.High,
                    self.subscribed_tokens[key].PriceUpdates.Low,
                    self.subscribed_tokens[key].PriceUpdates.LastTradeTime,
                    self.subscribed_tokens[key].PriceUpdates.Change,
                    self.subscribed_tokens[key].PriceUpdates.ChangePrcnt,
                ) = DhanTicker.bind_index_bc(seg_key, packet)
                log.info(
                    "Updated Index: %s, LTP & OHLC Deatils. Latest Data: %s",
                    tradingsymbol,
                    self.subscribed_tokens[key].PriceUpdates,
                )
            case 6:
                (
                    self.subscribed_tokens[key].PriceUpdates.TotalAskQty,
                    self.subscribed_tokens[key].PriceUpdates.TotalBidQty,
                    self.subscribed_tokens[key].PriceUpdates.BestBidQty,
                    self.subscribed_tokens[key].PriceUpdates.BestAskQty,
                    self.subscribed_tokens[key].PriceUpdates.BestBidPrice,
                    self.subscribed_tokens[key].PriceUpdates.BestAskPrice,
                    self.subscribed_tokens[key].PriceUpdates.TotalBidAskQty,
                    self.subscribed_tokens[key].PriceUpdates.TotalBidQtyPrcnt,
                    self.subscribed_tokens[key].PriceUpdates.TotalAskQtyPrcnt,
                ) = DhanTicker.update_top_bid_ask(seg_key, packet)
                log.info(
                    "Updated %s, Best Bid & Ask Deatils. Latest Data: %s",
                    tradingsymbol,
                    self.subscribed_tokens[key].PriceUpdates,
                )
            case 14:
                await self.send_heartbeat_to_price_feed()
            case 29:
                (
                    self.market_status.Msg,
                    self.market_status.Value,
                    self.market_status.MarketStatus,
                ) = DhanTicker.update_mkt_status(seg_key, packet)
                log.info("Updated Market Status. Latest Data: %s", self.market_status)
            case 32:
                (
                    self.subscribed_tokens[key].PriceUpdates.PrevClose,
                    self.subscribed_tokens[key].PriceUpdates.PrevOpenInterest,
                ) = DhanTicker.prev_close_bind(seg_key, packet)
                (
                    self.subscribed_tokens[key].PriceUpdates.Change,
                    self.subscribed_tokens[key].PriceUpdates.ChangePrcnt,
                ) = self.calc_chg_chgprcnt(
                    seg_key,
                    self.subscribed_tokens[key].PriceUpdates.Ltp,
                    self.subscribed_tokens[key].PriceUpdates.PrevClose,
                )
                (
                    self.subscribed_tokens[key].PriceUpdates.OpenInterestChg,
                    self.subscribed_tokens[key].PriceUpdates.OpenInterestChgPrcnt,
                ) = self.calc_chg_chgprcnt(
                    seg_key,
                    self.subscribed_tokens[key].PriceUpdates.OpenInterest,
                    self.subscribed_tokens[key].PriceUpdates.PrevOpenInterest,
                )
                log.info(
                    "Updated %s, Prev Close Value And Prev OI Value Deatils. Latest Data: %s",
                    tradingsymbol,
                    self.subscribed_tokens[key].PriceUpdates,
                )
            case 33:
                (
                    self.subscribed_tokens[key].PriceUpdates.UpperCircuit,
                    self.subscribed_tokens[key].PriceUpdates.LowerCircuit,
                ) = DhanTicker.update_ckt_limit(seg_key, packet)
                log.info(
                    "Updated %s, Upper & Lower Circuit Values. Latest Data: %s",
                    tradingsymbol,
                    self.subscribed_tokens[key].PriceUpdates,
                )
            case 36:
                (
                    self.subscribed_tokens[key].PriceUpdates.Week52High,
                    self.subscribed_tokens[key].PriceUpdates.Week52Low,
                ) = DhanTicker.update_52week_hl_bind(seg_key, packet)
                log.info(
                    "Updated %s, 52 Week High & Low Values. Latest Data: %s",
                    tradingsymbol,
                    self.subscribed_tokens[key].PriceUpdates,
                )
            case 37:
                (self.subscribed_tokens[key].PriceUpdates.OpenInterest,) = (
                    DhanTicker.update_oi(packet),
                )
                (
                    self.subscribed_tokens[key].PriceUpdates.OpenInterestChg,
                    self.subscribed_tokens[key].PriceUpdates.OpenInterestChgPrcnt,
                ) = self.calc_chg_chgprcnt(
                    seg_key,
                    self.subscribed_tokens[key].PriceUpdates.OpenInterest,
                    self.subscribed_tokens[key].PriceUpdates.PrevOpenInterest,
                )
                log.info(
                    "Updated %s, OI & Change in OI Details. Latest Data: %s",
                    tradingsymbol,
                    self.subscribed_tokens[key].PriceUpdates,
                )
        await self.cast("ON_TICK", self.subscribed_tokens[key])

    async def __fetch_jwt(
        self,
        login_id: str | int,
        params: Dict[str, str | int | float],
    ) -> Optional[str]:
        try:
            async with self.__reqsession.post(
                url=self.URL.login.jwt_token,
                data=json_stringify(params, return_urlencoded=True),
            ) as resp:
                jwt_token = await resp.text()
                if not self.debug or self.debug_verbose:
                    print(
                        f"The HTTP.{resp.method} Request To Url: {resp.url} | Response Status: {resp.status} {resp.reason} | Response Header: {json.dumps(dict(resp.headers))} | Response Content: {jwt_token} |"
                    )
        except aiohttp.ClientError as err:
            log.exception(
                "The Login JWT Token Request for UserId: %s, Failed With An Exception: %s",
                str(login_id),
                err,
            )
        else:
            return jwt_token

    async def __req_for_login(
        self,
        login_id: str | int,
        jwt_token: str,
        params: Dict[str, str | int | float],
        method: Literal["ask_pass", "with_password", "generate_otp", "otp_auth"],
    ) -> Optional[Dict[str, List[Dict[str, Any]] | str]]:
        if method not in {"ask_pass", "with_password", "generate_otp", "otp_auth"}:
            raise ValueError(
                "Method Parameter Was Not Correct, Valid Values Are `ask_pass`, `with_password`, `generate_otp` & `otp_auth`"
            )
        url, msg = (
            (
                self.URL.login.login,
                "AskPass" if method == "ask_pass" else "With Password",
            )
            if method in {"ask_pass", "with_password"}
            else (self.URL.login.login_generate_otp, "Generate OTP")
            if method == "generate_otp"
            else (self.URL.login.login_otp_auth, "OTP Auth")
            if method == "otp_auth"
            else (None, None)
        )

        if url is not None and msg is not None:
            try:
                async with self.__reqsession.post(
                    url,
                    data=quote_plus(self.encrypt_data(json_stringify(params))),
                    headers={
                        "Authorisation": self.LOGIN_AUTHORISATION.format(
                            jwt_token=jwt_token
                        )
                    },
                ) as resp:
                    try:
                        response = await resp.json(
                            content_type=resp.headers.get("Content-Type")
                        )
                    except json.JSONDecodeError as err:
                        log.exception(
                            "The Login %s Request for UserId: %s Succeded with Response: %s, However The response was not a JSON< Hence JSON Decoding Failed With An Exception: %s",
                            msg,
                            str(login_id),
                            await resp.text(),
                            err,
                        )
                        response = await resp.text()
                        if not self.debug or self.debug_verbose:
                            print(
                                f"The HTTP.{resp.method} Request To Url: {resp.url} | Response Status: {resp.status} {resp.reason} | Response Header: {json.dumps(dict(resp.headers))} | Response Content: {response} |"
                            )
                        return
            except aiohttp.ClientError as err:
                log.exception(
                    "The Login %s Request for UserId: %s, Failed With An Exception: %s",
                    msg,
                    str(login_id),
                    err,
                )
            else:
                if (
                    response is not None
                    and isinstance(response, dict)
                    and response.get("data") is not None
                ):
                    try:
                        data = self.decode_user_data(response["data"], return_as="dict")
                    except Exception as err:
                        log.exception(
                            "Decryption Of The Data Received: %s From Request For Login %s Failed With An Exception: %s",
                            response["data"],
                            msg,
                            err,
                        )
                        self._graceful_exit()
                    else:
                        log.info(
                            "Decryption Of The Data Received From Request For Login %s Succeded With Decrypted Response As: %s",
                            msg,
                            data,
                        )
                        if not self.debug or self.debug_verbose:
                            print(
                                f"Decryption Of The Data Received From Request For Login {msg} Succeded With Decrypted Response As: {data}"
                            )
                        print(isinstance(data, dict), type(data))
                        if isinstance(data, dict):
                            return data

    async def __login_with_credentials(
        self,
        login_id: str | int,
        password: str,
        imei_no: Optional[str] = None,
    ) -> None:
        imei_no = imei_no if imei_no is not None else get_dhan_imei_no()
        login_params = {
            "user_id": str(login_id),
            "pass": None,
            "imei_no": imei_no,
            "web_version": self.WEB_VERSION,
            "role": self.ROLE,
            "app_version": self.APP_VERSION,
            "app_id": self.APP_ID,
            "source": self.SOURCE,
        }
        log.info("Login Request JWT Token Params: %s", login_params)
        jwt_token = await self.__fetch_jwt(login_id, login_params)
        if jwt_token is not None and isinstance(jwt_token, str):
            login_params.update({"askpass": True})
            log.info("Login Request AskPass Params: %s", login_params)
            data = await self.__req_for_login(
                login_id, jwt_token, login_params, "ask_pass"
            )
            print(data)
            if (
                data is not None
                and isinstance(data, dict)
                and data.get("error_code") is not None
                and data.get("error_code") == "RS-2001"
                and data.get("status") is not None
                and data.get("status") == "success"
                and data.get("message") is not None
                and data.get("message") == "LOGIN WITH PASSWORD"
                and data.get("data") is not None
                and isinstance(data["data"], list)
            ):
                login_params.pop("askpass", None)
                login_params.update({"pass": password})
                log.info("Login Request With Password Params: %s", login_params)
                data = await self.__req_for_login(
                    login_id, jwt_token, login_params, "with_password"
                )
                if (
                    data is not None
                    and isinstance(data, dict)
                    and data.get("error_code") is not None
                    and data.get("error_code") == "RS-2001"
                    and data.get("status") is not None
                    and data.get("status") == "success"
                    and data.get("message") is not None
                    and data.get("message") == "LOGIN WITH PASSWORD SUCCESSFULLY"
                    and data.get("data") is not None
                    and isinstance(data["data"], list)
                ):
                    self.entity_id = data["data"][0]["entity_id"]
                    self.token_id = data["data"][0]["token_id"]
                    if data["data"][0]["validate_otp"]:
                        login_generate_otp_params = {
                            "email_mobile": str(login_id),
                            "entity_id": str(login_id),
                            "type": "new_user",
                            "device_id": imei_no,
                            "token_id": self.token_id,
                            "web_version": self.WEB_VERSION,
                            "reset_on": "mobile",
                            "source": self.SOURCE,
                        }
                        log.info(
                            "Login Request Generate OTP Params: %s",
                            login_generate_otp_params,
                        )
                        data = await self.__req_for_login(
                            login_id,
                            jwt_token,
                            login_generate_otp_params,
                            "generate_otp",
                        )
                        if (
                            data is not None
                            and isinstance(data, dict)
                            and data.get("error_code") is not None
                            and data.get("error_code") == "RS-2001"
                            and data.get("status") is not None
                            and data.get("status") == "success"
                            and data.get("message") is not None
                            and data.get("message") == "OTP SEND SUCCESSFULLY"
                            and data.get("data") is not None
                            and isinstance(data["data"], list)
                        ):
                            while True:
                                print(
                                    "Please Input The OTP Received On Your Mobile And Press Enter",
                                    end="\n" * 2,
                                )
                                otp = str(input("OTP: ")).lstrip().rstrip().strip()
                                if otp.isdigit() and len(otp) == 6:
                                    break
                                else:
                                    print(
                                        "Wrong OTP Input Has Been Provided, OTP Should Be 6 Digit. For Example: 123456. Please Try Again..",
                                        end="\n" * 2,
                                    )
                                    await asyncio.sleep(0.001)
                            login_otp_auth_params = {
                                "otp": otp,
                                "device_id": imei_no,
                                "entity_id": str(login_id),
                                "token_id": self.token_id,
                                "send_id": str(login_id),
                                "web_version": self.WEB_VERSION,
                                "source": self.SOURCE,
                            }
                            log.info(
                                "Login Request OTP Auth Params: %s",
                                login_otp_auth_params,
                            )
                            data = await self.__req_for_login(
                                login_id,
                                jwt_token,
                                login_otp_auth_params,
                                "otp_auth",
                            )
                            if (
                                data is not None
                                and isinstance(data, dict)
                                and data.get("error_code") is not None
                                and data.get("error_code") == "RS-2001"
                                and data.get("status") is not None
                                and data.get("status") == "success"
                                and data.get("message") is not None
                                and data.get("message")
                                == "LOGIN WITH PASSWORD SUCCESSFULLY"
                                and data.get("data") is not None
                                and isinstance(data["data"], list)
                            ):
                                self.userdata = AttrDict(data["data"][0])
