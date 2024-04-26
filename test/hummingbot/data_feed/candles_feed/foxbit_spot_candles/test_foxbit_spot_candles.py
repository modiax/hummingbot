import asyncio
import json
import re
import unittest
from typing import Awaitable
from unittest.mock import AsyncMock, MagicMock, patch
from aioresponses import aioresponses

from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.data_feed.candles_feed.foxbit_spot_candles import FoxbitSpotCandles, constants as CONSTANTS


class TestFoxbitSpotCandles(unittest.TestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDT"
        cls.interval = "1h"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = (cls.base_asset + cls.quote_asset).lower()

    def setUp(self) -> None:
        super().setUp()
        self.mocking_assistant = NetworkMockingAssistant()
        self.data_feed = FoxbitSpotCandles(trading_pair=self.trading_pair, interval=self.interval)
        self.log_records = []
        self.data_feed.logger().setLevel(1)
        self.data_feed.logger().addHandler(self)
        self.resume_test_event = asyncio.Event()

    def handle(self, record):
        self.log_records.append(record)

    def is_logged(self, log_level: str, message: str) -> bool:
        return any(
            record.levelname == log_level and record.getMessage() == message for
            record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def get_candles_rest_data_mock(self):
        data = [
            [
                "1672981200000",
                "127772.05150000",
                "128467.99980000",
                "127750.01000000",
                "128353.99990000",
                "1692918060000",
                "0.17080431",
                "21866.35948786",
                66,
                "0.12073605",
                "15466.34096391"
            ],
            [
                "1672984800000",
                "128353.99990000",
                "128353.99990000",
                "127922.00030000",
                "128339.99990000",
                "1692921660000",
                "0.12355465",
                "15851.30631056",
                45,
                "0.11030870",
                "14156.75206627"
            ],
            [
                "1672988400000",
                "127772.05150000",
                "128467.99980000",
                "127750.01000000",
                "128353.99990000",
                "1692918060000",
                "0.17080431",
                "21866.35948786",
                66,
                "0.12073605",
                "15466.34096391"
            ],
            [
                "1672992000000",
                "128353.99990000",
                "128353.99990000",
                "127922.00030000",
                "128339.99990000",
                "1692921660000",
                "0.12355465",
                "15851.30631056",
                45,
                "0.11030870",
                "14156.75206627"
            ]
        ]
        return data

    def get_candles_ws_data_mock_1(self):
        data = {"o": json.dumps([
            [
                123400000,
                2700.33,
                2701.268701,
                2687.01,
                2687.01,
                24.86100992,
                0,
                2870.95,
                1
            ],
            [
                123460000,
                2700.33,
                2701.268701,
                2687.01,
                2687.01,
                24.86100992,
                0,
                2870.95,
                1
            ],
        ])}
        return data

    def get_candles_ws_data_mock_2(self):
        data = {"o": json.dumps([
            [
                123460000,
                2700.33,
                2701.268701,
                2687.01,
                2687.01,
                24.86100992,
                0,
                2870.95,
                1
            ],
            [
                123460000,
                2700.33,
                2701.268701,
                2687.01,
                2687.01,
                24.86100992,
                0,
                2870.95,
                1
            ],
        ])}
        return data

    def get_instruments_rest_data_mock(self):
        data = [
            {
                "InstrumentId": 13,
                "Product1Symbol": "BTC",
                "Product2Symbol": "USDT",
            }]
        return data

    @aioresponses()
    def test_fetch_candles(self, mock_api: aioresponses):
        start_time = 1672981200000
        end_time = 1672992000000
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.CANDLES_ENDPOINT.format(self.ex_trading_pair)}"
        regex_url = re.compile(url + r"\?.*")
        data_mock = self.get_candles_rest_data_mock()
        mock_api.get(url=regex_url, body=json.dumps(data_mock))
        resp = self.async_run_with_timeout(self.data_feed.fetch_candles(start_time=start_time, end_time=end_time))
        self.assertEqual(resp.shape[0], len(data_mock))
        self.assertEqual(resp.shape[1], 10)

    def test_candles_empty(self):
        self.assertTrue(self.data_feed.candles_df.empty)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @aioresponses()
    def test_listen_for_subscriptions_subscribes_to_klines(self, ws_connect_mock, mock_api: aioresponses):
        url = f"{CONSTANTS.REST_V2_URL}{CONSTANTS.INSTRUMENTS_ENDPOINT}"
        regex_url = re.compile(url)
        data_mock = self.get_instruments_rest_data_mock()
        mock_api.get(url=regex_url, body=json.dumps(data_mock))

        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(self.get_candles_ws_data_mock_1()))

        self.listening_task = self.ev_loop.create_task(self.data_feed.listen_for_subscriptions())
        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)
        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(1, len(sent_subscription_messages))
        expected_kline_subscription = {'Content-Type': 'application/json', 'User-Agent': 'HBOT', 'i': 1, 'm': 2,
                                       'n': 'SubscribeTicker',
                                       'o': '{"OMSId": 1, "InstrumentId": 13, "Interval": 3600, "IncludeLastCount": 2}'}
        self.assertEqual(expected_kline_subscription, sent_subscription_messages[0])
        self.assertTrue(self.is_logged(
            "INFO",
            "Subscribed to public candles..."
        ))

    @patch("hummingbot.data_feed.candles_feed.foxbit_spot_candles.FoxbitSpotCandles._sleep")
    @patch("aiohttp.ClientSession.ws_connect")
    def test_listen_for_subscriptions_raises_cancel_exception(self, mock_ws, _: AsyncMock):
        mock_ws.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(self.data_feed.listen_for_subscriptions())
            self.async_run_with_timeout(self.listening_task)

    @patch("hummingbot.data_feed.candles_feed.foxbit_spot_candles.FoxbitSpotCandles._sleep")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_subscriptions_logs_exception_details(self, mock_ws, sleep_mock: AsyncMock):
        mock_ws.side_effect = Exception("TEST ERROR.")
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(
            asyncio.CancelledError())
        self.listening_task = self.ev_loop.create_task(self.data_feed.listen_for_subscriptions())
        self.async_run_with_timeout(self.resume_test_event.wait())
        self.assertTrue(
            self.is_logged(
                "ERROR",
                "Unexpected error occurred when listening to public klines. Retrying in 1 seconds..."))

    def test_subscribe_channels_raises_cancel_exception(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(self.data_feed._subscribe_channels(mock_ws))
            self.async_run_with_timeout(self.listening_task)

    def test_subscribe_channels_raises_exception_and_logs_error(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Test Error")
        with self.assertRaises(Exception):
            self.listening_task = self.ev_loop.create_task(self.data_feed._subscribe_channels(mock_ws))
            self.async_run_with_timeout(self.listening_task)
        self.assertTrue(
            self.is_logged("ERROR", "Unexpected error occurred subscribing to public candles...")
        )

    @patch("hummingbot.data_feed.candles_feed.foxbit_spot_candles.FoxbitSpotCandles.fill_historical_candles",
           new_callable=AsyncMock)
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_process_websocket_messages_empty_candle(self, ws_connect_mock, fill_historical_candles_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(self.get_candles_ws_data_mock_1()))
        self.listening_task = self.ev_loop.create_task(self.data_feed.listen_for_subscriptions())
        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)
        self.assertEqual(self.data_feed.candles_df.shape[0], 1)
        self.assertEqual(self.data_feed.candles_df.shape[1], 10)
        fill_historical_candles_mock.assert_called_once()

    @patch("hummingbot.data_feed.candles_feed.foxbit_spot_candles.FoxbitSpotCandles.fill_historical_candles",
           new_callable=AsyncMock)
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_process_websocket_messages_duplicated_candle_not_included(self, ws_connect_mock, fill_historical_candles):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        fill_historical_candles.return_value = None
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(self.get_candles_ws_data_mock_1()))
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(self.get_candles_ws_data_mock_1()))
        self.listening_task = self.ev_loop.create_task(self.data_feed.listen_for_subscriptions())
        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)
        self.assertEqual(self.data_feed.candles_df.shape[0], 1)
        self.assertEqual(self.data_feed.candles_df.shape[1], 10)

    @patch("hummingbot.data_feed.candles_feed.foxbit_spot_candles.FoxbitSpotCandles.fill_historical_candles")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_process_websocket_messages_with_two_valid_messages(self, ws_connect_mock, _):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(self.get_candles_ws_data_mock_1()))
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(self.get_candles_ws_data_mock_2()))
        self.listening_task = self.ev_loop.create_task(self.data_feed.listen_for_subscriptions())
        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value, timeout=2)
        self.assertEqual(self.data_feed.candles_df.shape[0], 2)
        self.assertEqual(self.data_feed.candles_df.shape[1], 10)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception
