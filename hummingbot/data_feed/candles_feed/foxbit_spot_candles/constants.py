from bidict import bidict

from hummingbot.core.api_throttler.data_types import RateLimit

REST_URL = "https://api.foxbit.com.br/rest/v3"
REST_V2_URL = "https://api.foxbit.com.br/AP"
HEALTH_CHECK_ENDPOINT = "/system/time"
CANDLES_ENDPOINT = "/markets/{}/candlesticks"
WS_CANDLES_ENDPOINT = "SubscribeTicker"
INSTRUMENTS_ENDPOINT = "/GetInstruments"

WSS_URL = "wss://api.foxbit.com.br"

INTERVALS = bidict({
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "12h": "12h",
    "1d": "1d",
    "3d": "3d",
    "1w": "1w",
    "2w": "2w",
    "1M": "1M"
})

MAX_RECORDS = 500

RATE_LIMITS = [
    RateLimit(
        limit_id=CANDLES_ENDPOINT,
        limit=3,
        time_interval=2,
    ),
    RateLimit(
        limit_id=HEALTH_CHECK_ENDPOINT,
        limit=5,
        time_interval=1,
    ),
    RateLimit(
        limit_id=INSTRUMENTS_ENDPOINT,
        limit=750,
        time_interval=60,
    )
]
