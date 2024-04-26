import asyncio
import json
import logging
from typing import Optional

import numpy as np
from bidict import bidict

from hummingbot.core.network_iterator import NetworkStatus, safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.foxbit_spot_candles import constants as CONSTANTS
from hummingbot.logger import HummingbotLogger

_seq_nr: int = 0


def get_next_message_frame_sequence_number() -> int:
    """
    Returns next sequence number to be used into message frame for WS requests
    """
    global _seq_nr
    _seq_nr += 1
    return _seq_nr


class FoxbitSpotCandles(CandlesBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = CONSTANTS.MAX_RECORDS):
        self._trading_pair_instrument_id_map = bidict()
        super().__init__(trading_pair, interval, max_records)

    @property
    def name(self):
        return f"foxbit_{self._trading_pair}"

    @property
    def rest_url(self):
        return CONSTANTS.REST_URL

    @property
    def rest_v2_url(self):
        return CONSTANTS.REST_V2_URL

    @property
    def wss_url(self):
        return CONSTANTS.WSS_URL

    @property
    def health_check_url(self):
        return self.rest_url + CONSTANTS.HEALTH_CHECK_ENDPOINT

    @property
    def candles_url(self):
        return self.rest_url + CONSTANTS.CANDLES_ENDPOINT

    @property
    def instruments_url(self):
        return self.rest_v2_url + CONSTANTS.INSTRUMENTS_ENDPOINT

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS

    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(url=self.health_check_url,
                                             throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT)
        return NetworkStatus.CONNECTED

    async def initialize_trading_pair_instrument_id_map(self):
        rest_assistant = await self._api_factory.get_rest_assistant()
        exchange_info = await rest_assistant.execute_request(url=self.instruments_url,
                                                             throttler_limit_id=CONSTANTS.INSTRUMENTS_ENDPOINT)
        for symbol_data in exchange_info:
            self._trading_pair_instrument_id_map[symbol_data[
                "InstrumentId"]] = f"{symbol_data['Product1Symbol'].upper()}-{symbol_data['Product2Symbol'].upper()}"

    def trading_pair_instrument_id_map_ready(self):
        return len(self._trading_pair_instrument_id_map) > 0

    def get_exchange_trading_pair(self, hb_trading_pair: str) -> str:
        return hb_trading_pair.replace("-", "").lower()

    async def get_exchange_instrument_id(self, hb_trading_pair: str) -> str:
        if not self.trading_pair_instrument_id_map_ready():
            await self.initialize_trading_pair_instrument_id_map()
        return self._trading_pair_instrument_id_map.inverse.get(hb_trading_pair)

    async def fetch_candles(self,
                            start_time: Optional[int] = None,
                            end_time: Optional[int] = None,
                            limit: Optional[int] = CONSTANTS.MAX_RECORDS):
        rest_assistant = await self._api_factory.get_rest_assistant()
        params = {"interval": CONSTANTS.INTERVALS[self.interval], "limit": min(limit, CONSTANTS.MAX_RECORDS)}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        candles = await rest_assistant.execute_request(
            url=self.candles_url.format(self.get_exchange_trading_pair(self._trading_pair)),
            throttler_limit_id=CONSTANTS.CANDLES_ENDPOINT,
            params=params)
        new_hb_candles = []
        for candle in candles:
            timestamp = int(candle[0])
            open_price = float(candle[1])
            high_price = float(candle[2])
            low_price = float(candle[3])
            close_price = float(candle[4])
            base_volume = float(candle[6])
            quote_volume = float(candle[7])
            num_trades = int(candle[8])
            taker_buy_base_volume = float(candle[9])
            taker_buy_quote_volume = float(candle[10])
            new_hb_candles.append([timestamp, open_price, high_price, low_price, close_price, base_volume,
                                   quote_volume, num_trades, taker_buy_base_volume, taker_buy_quote_volume])
        return np.array(new_hb_candles).astype(float)

    async def fill_historical_candles(self):
        max_request_needed = (self._candles.maxlen // CONSTANTS.MAX_RECORDS) + 1
        requests_executed = 0
        while not self.ready:
            missing_records = self._candles.maxlen - len(self._candles)
            end_timestamp = int(self._candles[0][0])
            try:
                if requests_executed < max_request_needed:
                    # we have to add one more since, the last row is not going to be included
                    candles = await self.fetch_candles(end_time=end_timestamp, limit=missing_records + 1)
                    # we are computing again the quantity of records again since the websocket process is able to
                    # modify the deque and if we extend it, the new observations are going to be dropped.
                    missing_records = self._candles.maxlen - len(self._candles)
                    self._candles.extendleft(candles[-(missing_records + 1):-1][::-1])
                    requests_executed += 1
                else:
                    self.logger().error(f"There is no data available for the quantity of "
                                        f"candles requested for {self.name}.")
                    raise
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                self.logger().exception(
                    "Unexpected error occurred when getting historical candles. Retrying in 1 seconds...",
                )
                await self._sleep(1.0)

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the candles events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            instrument_id = await self.get_exchange_instrument_id(self._trading_pair)
            payload = {
                "Content-Type": "application/json",
                "User-Agent": "HBOT",
                "m": 2,
                "i": get_next_message_frame_sequence_number(),
                "n": CONSTANTS.WS_CANDLES_ENDPOINT,
                "o": json.dumps({"OMSId": 1,
                                 "InstrumentId": instrument_id,
                                 "Interval": self.get_seconds_from_interval(self.interval),
                                 "IncludeLastCount": 1}),
            }
            subscribe_candles_request: WSJSONRequest = WSJSONRequest(payload=payload)
            await ws.send(subscribe_candles_request)
            self.logger().info("Subscribed to public candles...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to public candles...",
                exc_info=True
            )
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            candle_data_list = json.loads(ws_response.data.get('o', '[]'))
            if len(candle_data_list) > 0:
                candle_data = candle_data_list[0]
                if isinstance(candle_data, list) and len(candle_data) >= 6:
                    timestamp = int(candle_data[0])
                    high = float(candle_data[1])
                    low = float(candle_data[2])
                    open = float(candle_data[3])
                    close = float(candle_data[4])
                    volume = float(candle_data[5])
                    # no data field
                    quote_asset_volume = 0
                    n_trades = 0
                    taker_buy_base_volume = 0
                    taker_buy_quote_volume = 0
                    if len(self._candles) == 0:
                        self._candles.append(np.array([timestamp, open, high, low, close, volume,
                                                       quote_asset_volume, n_trades, taker_buy_base_volume,
                                                       taker_buy_quote_volume]))
                        safe_ensure_future(self.fill_historical_candles())
                    elif timestamp > int(self._candles[-1][0]):
                        # TODO: validate also that the diff of timestamp == interval (issue with 1M interval).
                        self._candles.append(np.array([timestamp, open, high, low, close, volume,
                                                       quote_asset_volume, n_trades, taker_buy_base_volume,
                                                       taker_buy_quote_volume]))
                    elif timestamp == int(self._candles[-1][0]):
                        self._candles.pop()
                        self._candles.append(np.array([timestamp, open, high, low, close, volume,
                                                       quote_asset_volume, n_trades, taker_buy_base_volume,
                                                       taker_buy_quote_volume]))
