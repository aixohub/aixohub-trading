"""
This file contains functions to stream data
Coded by Tyler Bowers
Github: https://github.com/tylerebowers/Schwab-API-Python
"""

import asyncio
import atexit
import json
import threading
from datetime import datetime, time
from time import sleep

import websockets
import websockets.exceptions


class SchwabStream:

    def __init__(self, schwabClient):
        """
        Initialize the stream object to stream data from Schwab Streamer
        :param client: Client object
        :type client: Client
        """
        self._websocket = None  # the websocket
        self._streamer_info = None  # streamer info from api call
        self._request_id = 0  # a counter for the request id
        self.active = False  # whether the stream is active
        self._thread = None  # the thread that runs the stream
        self._client = schwabClient  # so we can get streamer info
        self.verbose = schwabClient.verbose  # inherit the client's verbose setting
        self.subscriptions = {}  # a dictionary of subscriptions

        # register atexit to stop the stream (if active)
        def stop_atexit():
            if self.active:
                self.stop()

        atexit.register(stop_atexit)

    async def _start_streamer(self, receiver_func=print, *args, **kwargs):
        """
        Start the streamer
        :param receiver_func: function to call when data is received
        :type receiver_func: function
        """
        # get streamer info
        response = self._client.preferences()
        self._streamer_info = response.get('streamerInfo', None)[0]

        # start the stream
        while True:
            try:
                async with websockets.connect(self._streamer_info.get('streamerSocketUrl'),
                                              ping_interval=None) as self._websocket:
                    if self.verbose: print("Connected to streaming server.")

                    # send login payload
                    login_payload = self.basic_request(service="ADMIN", command="LOGIN",
                                                       parameters={"Authorization": self._client.access_token,
                                                                   "SchwabClientChannel": self._streamer_info.get(
                                                                       "schwabClientChannel"),
                                                                   "SchwabClientFunctionId": self._streamer_info.get(
                                                                       "schwabClientFunctionId")})
                    await self._websocket.send(json.dumps(login_payload))
                    receiver_func(await self._websocket.recv(), *args, **kwargs)
                    self.active = True

                    # send subscriptions
                    for service, subs in self.subscriptions.items():
                        reqs = []
                        for key, fields in subs.items():
                            reqs.append(self.basic_request(service=service, command="ADD", parameters={"keys": key,
                                                                                                       "fields": SchwabStream._list_to_string(
                                                                                                           fields)}))
                        if reqs:
                            await self._websocket.send(json.dumps({"requests": reqs}))
                            receiver_func(await self._websocket.recv(), *args, **kwargs)

                    # main listener loop
                    while True:
                        receiver_func(await self._websocket.recv(), *args, **kwargs)

            except Exception as e:
                self.active = False
                if e is websockets.exceptions.ConnectionClosedOK or str(
                        e) == "received 1000 (OK); then sent 1000 (OK)":  # catch logout request
                    if self.verbose: print("Stream has closed.")
                    break
                elif e is websockets.exceptions.ConnectionClosedError or str(
                        e) == "no close frame received or sent":  # catch no subscriptions kick
                    if self.verbose: print(f"Stream closed (likely no subscriptions): {e}")
                    break
                else:  # stream has quit unexpectedly, try to reconnect
                    if self.verbose: print(f"{e}")
                    if self.verbose: print("Connection lost to server, reconnecting...")

    def start(self, receiver=print, *args, **kwargs):
        """
        Start the stream
        :param receiver: function to call when data is received
        :type receiver: function
        """
        if not self.active:
            def _start_async():
                asyncio.run(self._start_streamer(receiver, *args, **kwargs))

            self._thread = threading.Thread(target=_start_async, daemon=False)
            self._thread.start()
            sleep(1)  # if the thread does not start in time then the main program may close before the streamer starts
        else:
            print("Stream already active.")

    def start_automatic(self, receiver=print, after_hours=False, pre_hours=False):
        """
        Start the stream automatically at market open and close, will NOT erase subscriptions
        :param receiver: function to call when data is received
        :type receiver: function
        :param after_hours: include after hours trading
        :type after_hours: bool
        :param pre_hours: include pre hours trading
        :type pre_hours: bool
        """
        start = time(9, 29, 0)  # market opens at 9:30
        end = time(16, 0, 0)  # market closes at 4:00
        if pre_hours:
            start = time(7, 59, 0)
        if after_hours:
            end = time(20, 0, 0)

        def checker():

            while True:
                in_hours = (start <= datetime.now().time() <= end) and (0 <= datetime.now().weekday() <= 4)
                if in_hours and not self.active:
                    self.start(receiver=receiver)
                elif not in_hours and self.active:
                    if self.verbose: print("Stopping Stream.")
                    self.stop(clear_subscriptions=False)
                sleep(60)

        threading.Thread(target=checker).start()

        if not start <= datetime.now().time() <= end:
            print("Stream was started outside of active hours and will launch when in hours.")

    def _record_request(self, request):
        """
        Record the request into self.subscriptions (for the event of crashes)
        :param request: request
        :type request: dict
        """

        def str_to_list(st):
            if type(st) is str:
                return st.split(",")
            elif type(st) is list:
                return st

        service = request.get("service", None)
        command = request.get("command", None)
        parameters = request.get("parameters", None)
        if parameters is not None:
            keys = str_to_list(parameters.get("keys", []))
            fields = str_to_list(parameters.get("fields", []))
            # add service to subscriptions if not already there
            if service not in self.subscriptions:
                self.subscriptions[service] = {}
            if command == "ADD":
                for key in keys:
                    if key not in self.subscriptions[service]:
                        self.subscriptions[service][key] = fields
                    else:
                        self.subscriptions[service][key] = list(set(fields) | set(self.subscriptions[service][key]))
            elif command == "SUBS":
                self.subscriptions[service] = {}
                for key in keys:
                    self.subscriptions[service][key] = fields
            elif command == "UNSUBS":
                for key in keys:
                    if key in self.subscriptions[service]:
                        self.subscriptions[service].pop(key)
            elif command == "VIEW":  # not sure if this is even working on Schwab's end :/
                for key in self.subscriptions[service].keys():
                    self.subscriptions[service][key] = fields

    def send(self, requests):
        """
        Send a request to the stream
        :param requests: list of requests or a single request
        :type requests: list | dict
        """

        # an async function to send the request
        async def _send(to_send):
            await self._websocket.send(to_send)

        # make sure requests is a list
        if type(requests) is not list:
            requests = [requests]

        # add requests to list of subscriptions
        for request in requests:
            self._record_request(request)

        # send the request if the stream is active, queue otherwise
        if self.active:
            to_send = json.dumps({"requests": requests})
            asyncio.run(_send(to_send))
        else:
            if self.verbose: print("Stream is not active, request queued.")

    def stop(self, clear_subscriptions=True):
        """
        Stop the stream
        :param clear_subscriptions: clear records
        :type clear_subscriptions: bool
        """
        if clear_subscriptions:
            self.subscriptions = {}
        self._request_id += 1
        self.send(self.basic_request(service="ADMIN", command="LOGOUT"))
        self.active = False

    def basic_request(self, service, command, parameters=None):
        """
        Create a basic request (all requests follow this format)
        :param service: service to use
        :type service: str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW"|"LOGIN"|"LOGOUT")
        :type command: str
        :param parameters: parameters to use
        :type parameters: dict
        :return: stream request
        :rtype: dict
        """
        # 如果_streamer_info为空，则获取偏好设置
        if self._streamer_info is None:
            response = self._client.preferences()
                # 从偏好设置中解析streamerInfo
            self._streamer_info = response.get('streamerInfo', None)[0]

        # 移除None参数
        # remove None parameters
        if parameters is not None:
            for key in parameters.keys():
                if parameters[key] is None: del parameters[key]

        # 如果_streamer_info不为空
        if self._streamer_info is not None:
            # 创建请求字典
            request = {"service": service.upper(),
                       "command": command.upper(),
                       "requestid": self._request_id,
                       "SchwabClientCustomerId": self._streamer_info.get("schwabClientCustomerId"),
                       "SchwabClientCorrelId": self._streamer_info.get("schwabClientCorrelId")}
            # 如果参数不为空且长度大于0，则添加到请求字典中
            if parameters is not None and len(parameters) > 0: request["parameters"] = parameters
            # 请求ID自增
            self._request_id += 1
            # 返回请求字典
            return request
        else:
            # 打印错误信息
            print("basic_request(): Could not use/get streamerInfo")
            # 返回None
            return None

    @staticmethod
    def _list_to_string(ls):
        """
        Convert a list to a string (e.g. [1, "B", 3] -> "1,B,3"), or passthrough if already a string
        :param ls: list to convert
        :type ls: list | str
        :return: converted string
        :rtype: str
        """
        if type(ls) is str:
            return ls
        elif type(ls) is list:
            return ",".join(map(str, ls))

    def level_one_equities(self, keys, fields, command="ADD"):
        """
        Level one equities
        :param keys: list of keys to use (e.g. ["AMD", "INTC"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("LEVELONE_EQUITIES", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                            "fields": SchwabStream._list_to_string(
                                                                                fields)})

    def level_one_options(self, keys, fields, command="ADD"):
        """
        Level one options, key format: [Underlying Symbol (6 characters including spaces) | Expiration (6 characters) | Call/Put (1 character) | Strike Price (5+3=8 characters)]
        :param keys: list of keys to use (e.g. ["GOOG  240809C00095000", "AAPL  240517P00190000"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("LEVELONE_OPTIONS", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                           "fields": SchwabStream._list_to_string(
                                                                               fields)})

    def level_one_futures(self, keys, fields, command="ADD"):
        """
        Level one futures, key format: '/' + 'root symbol' + 'month code' + 'year code'; month code is 1 character: (F: Jan, G: Feb, H: Mar, J: Apr, K: May, M: Jun, N: Jul, Q: Aug, U: Sep, V: Oct, X: Nov, Z: Dec), year code is 2 characters (i.e. 2024 = 24)
        :param keys: list of keys to use (e.g. ["/ESF24", "/GCG24"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("LEVELONE_FUTURES", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                           "fields": SchwabStream._list_to_string(
                                                                               fields)})

    def level_one_futures_options(self, keys, fields, command="ADD"):
        """
        Level one futures options, key format: '.' + '/' + 'root symbol' + 'month code' + 'year code' + 'Call/Put code' + 'Strike Price'
        :param keys: list of keys to use (e.g. ["./OZCZ23C565"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("LEVELONE_FUTURES_OPTIONS", command,
                                  parameters={"keys": SchwabStream._list_to_string(keys),
                                              "fields": SchwabStream._list_to_string(fields)})

    def level_one_forex(self, keys, fields, command="ADD"):
        """
        Level one forex, key format: 'from currency' + '/' + 'to currency'
        :param keys: list of keys to use (e.g. ["EUR/USD", "JPY/USD"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("LEVELONE_FOREX", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                         "fields": SchwabStream._list_to_string(
                                                                             fields)})

    def nyse_book(self, keys, fields, command="ADD"):
        """
        NYSE book orders
        :param keys: list of keys to use (e.g. ["NIO", "F"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("NYSE_BOOK", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                    "fields": SchwabStream._list_to_string(fields)})

    def nasdaq_book(self, keys, fields, command="ADD"):
        """
        NASDAQ book orders
        :param keys: list of keys to use (e.g. ["AMD", "CRWD"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("NASDAQ_BOOK", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                      "fields": SchwabStream._list_to_string(fields)})

    def options_book(self, keys, fields, command="ADD"):
        """
        Options book orders
        :param keys: list of keys to use (e.g. ["GOOG  240809C00095000", "AAPL  240517P00190000"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("OPTIONS_BOOK", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                       "fields": SchwabStream._list_to_string(fields)})

    def chart_equity(self, keys, fields, command="ADD"):
        """
        Chart equity
        :param keys: list of keys to use (e.g. ["GOOG", "AAPL"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("CHART_EQUITY", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                       "fields": SchwabStream._list_to_string(fields)})

    def chart_futures(self, keys, fields, command="ADD"):
        """
        Chart futures, key format: '/' + 'root symbol' + 'month code' + 'year code'; month code is 1 character: (F: Jan, G: Feb, H: Mar, J: Apr, K: May, M: Jun, N: Jul, Q: Aug, U: Sep, V: Oct, X: Nov, Z: Dec), year code is 2 characters (i.e. 2024 = 24)
        :param keys: list of keys to use (e.g. ["/ESF24", "/GCG24"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("CHART_FUTURES", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                        "fields": SchwabStream._list_to_string(fields)})

    def screener_equity(self, keys, fields, command="ADD"):
        """
        Screener equity, key format: (PREFIX)_(SORTFIELD)_(FREQUENCY); Prefix: ($COMPX, $DJI, $SPX.X, INDEX_AL, NYSE, NASDAQ, OTCBB, EQUITY_ALL); Sortfield: (VOLUME, TRADES, PERCENT_CHANGE_UP, PERCENT_CHANGE_DOWN, AVERAGE_PERCENT_VOLUME), Frequency: (0 (all day), 1, 5, 10, 30 60)
        :param keys: list of keys to use (e.g. ["$DJI_PERCENT_CHANGE_UP_60", "NASDAQ_VOLUME_30"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("SCREENER_EQUITY", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                          "fields": SchwabStream._list_to_string(
                                                                              fields)})

    def screener_option(self, keys, fields, command="ADD"):
        """
        Screener option key format: (PREFIX)_(SORTFIELD)_(FREQUENCY); Prefix: (OPTION_PUT, OPTION_CALL, OPTION_ALL); Sortfield: (VOLUME, TRADES, PERCENT_CHANGE_UP, PERCENT_CHANGE_DOWN, AVERAGE_PERCENT_VOLUME), Frequency: (0 (all day), 1, 5, 10, 30 60)
        :param keys: list of keys to use (e.g. ["OPTION_PUT_PERCENT_CHANGE_UP_60", "OPTION_CALL_TRADES_30"])
        :type keys: list | str
        :param fields: list of fields to use
        :type fields: list | str
        :param command: command to use ("SUBS"|"ADD"|"UNSUBS"|"VIEW")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("SCREENER_OPTION", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                          "fields": SchwabStream._list_to_string(
                                                                              fields)})

    def account_activity(self, keys="Account Activity", fields="0,1,2,3", command="SUBS"):
        """
        Account activity
        :param keys: list of keys to use (e.g. ["Account Activity"])
        :type keys: list | str
        :param fields: list of fields to use (e.g. ["0,1,2,3"])
        :type fields: list | str
        :param command: command to use ("SUBS"|"UNSUBS")
        :type command: str
        :return: stream request
        :rtype: dict
        """
        return self.basic_request("ACCT_ACTIVITY", command, parameters={"keys": SchwabStream._list_to_string(keys),
                                                                        "fields": SchwabStream._list_to_string(fields)})
