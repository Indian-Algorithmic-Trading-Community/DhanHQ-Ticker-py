from dhanhq_ticker_py import (
    get_option_chain,
    get_expiry_dates,
    get_instrument_details,
    get_instruments_details,
)

get_instrument_details("RELIANCE", "BSE", is_equity=True, pretty_print=True)

get_instrument_details(
    "USDINR",
    "BSE",
    is_optcur=True,
    expiry_date="2023-12-08",
    strike=82.5,
    opt_type="CE",
    pretty_print=True,
)

get_instruments_details(
    ["RELIANCE", "HDFCBANK"], "BSE", is_equity=True, pretty_print=True
)

get_instruments_details(["SENSEX", "BANKEX"], "BSE", is_index=True, pretty_print=True)

get_instruments_details(
    ["NIFTY", "BANKNIFTY", "FINNNIFTY", "MIDCPNIFTY"],
    "NSE",
    is_index=True,
    pretty_print=True,
)

print(get_expiry_dates("NIFTY", "NSE"))

print(get_option_chain("NIFTY", "NSE"))
print(get_option_chain("NIFTY", "NSE", "2023-12-14"))

print(get_expiry_dates("BANKNIFTY", "NSE"))

print(get_option_chain("BANKNIFTY", "NSE"))
print(get_option_chain("BANKNIFTY", "NSE", "2023-12-13"))


bnf_expiry, nf_expiry = [get_expiry_dates(idx, "NSE") for idx in {"BANKNIFTY", "NIFTY"}]

print(bnf_expiry, nf_expiry, sep="\n" * 2, end="\n" * 2)

nearest_bnf_expiry, nearest_nf_expiry = (
    bnf_expiry["ExpiryDate"][0],
    nf_expiry["ExpiryDate"][0],
)
print(nearest_bnf_expiry, nearest_nf_expiry, sep="\n" * 2, end="\n" * 2)

instruments = get_instruments_details(
    ["NIFTY", "BANKNIFTY"],
    "NSE",
    is_optidx=True,
    expiry_dates=[nearest_bnf_expiry, nearest_nf_expiry],
    strikes=[20000, 45000],
    opt_types=["CE", "PE"],
)
print(instruments, end="\n" * 2)

finnifty_all_nearest_expiry_options = get_option_chain("FINNIFTY", "NSE")
print(finnifty_all_nearest_expiry_options, end="\n" * 2)
