import json
from datetime import datetime

import requests

from schwab_stream import SchwabStream


class SchwabClient:

    def __init__(self, callback_url="https://127.0.0.1", tokens_file="tokens.json", timeout=5,
                 verbose=False, update_tokens_auto=True):
        """
        Initialize a client to access the Schwab API.
        """
        self._callback_url = callback_url  # callback url to use
        self.access_token = None  # access token from auth
        self.refresh_token = None  # refresh token from auth
        self.id_token = None  # id token from auth
        self._access_token_issued = None  # datetime of access token issue
        self._refresh_token_issued = None  # datetime of refresh token issue
        self._access_token_timeout = 1800  # in seconds (from schwab)
        self._refresh_token_timeout = 7  # in days (from schwab)
        self._tokens_file = tokens_file  # path to tokens file
        self.timeout = timeout  # timeout to use in requests
        self.verbose = verbose  # verbose mode
        self.stream = SchwabStream(self)
        self.awaiting_input = False  # whether we are awaiting user input
        res = self._post_oauth_token()
        self.access_token = res['access_token']  # access token from auth
        self.refresh_token = res['refresh_token']  # refresh token from auth

        self._base_api_url = "https://api.schwabapi.com"

    def _update_access_token(self):
        """
        "refresh" the access token using the refresh token
        """
        # get the token dictionary (we will need to rewrite the file)
        access_token_time_old, refresh_token_issued, token_dictionary_old = self._read_tokens_file()
        # get new tokens
        for i in range(3):
            response = self._post_oauth_token()
            # get and update to the new access token
            self._access_token_issued = datetime.now()
            new_td = response
            self.access_token = new_td.get("access_token")
            self.refresh_token = new_td.get("refresh_token")
            self.id_token = new_td.get("id_token")
            self._write_tokens_file(self._access_token_issued, refresh_token_issued, new_td)
            if self.verbose:  # show user that we have updated the access token
                print(f"Access token updated: {self._access_token_issued}")
                break
            else:
                print(f"Could not get new access token ({i + 1} of 3).")

    def _post_oauth_token(self):
        """
        Makes API calls for auth code and refresh tokens
        """
        response = requests.get('http://127.0.0.1:8080/api/schwab/queryToken?ts=1')
        res = eval(response.content.decode(), {"true": True, "false": False, "null": None})
        access_token = res['data']["AccessToken"]
        refresh_token = res['data']["RefreshToken"]
        pre_login_data = {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'id_token': 122
        }
        print(pre_login_data)
        return pre_login_data

    def _write_tokens_file(self, at_issued, rt_issued, token_dictionary):
        """
        Writes token file
        :param at_issued: access token issued
        :type at_issued: datetime
        :param rt_issued: refresh token issued
        :type rt_issued: datetime
        :param token_dictionary: token dictionary
        :type token_dictionary: dict
        """
        try:
            with open(self._tokens_file, 'w') as f:
                toWrite = {"access_token_issued": at_issued.isoformat(), "refresh_token_issued": rt_issued.isoformat(),
                           "token_dictionary": token_dictionary}
                json.dump(toWrite, f, ensure_ascii=False, indent=4)
                f.flush()
        except Exception as e:
            print(e)

    def _read_tokens_file(self):
        """
        Reads token file
        :return: access token issued, refresh token issued, token dictionary
        :rtype: datetime, datetime, dict
        """
        try:
            with open(self._tokens_file, 'r') as f:
                d = json.load(f)
                return datetime.fromisoformat(d.get("access_token_issued")), datetime.fromisoformat(
                    d.get("refresh_token_issued")), d.get("token_dictionary")
        except Exception as e:
            print(e)
            return None, None, None

    def transaction_details(self, accountHash, transactionId):
        """
        Get specific transaction information for a specific account
        :param accountHash: account hash number
        :type accountHash: str
        :param transactionId: transaction id
        :type transactionId: str|int
        :return: transaction details of transaction id using accountHash
        :rtype: request.Response
        """
        return requests.get(f'{self._base_api_url}/trader/v1/accounts/{accountHash}/transactions/{transactionId}',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params={'accountNumber': accountHash, 'transactionId': transactionId},
                            timeout=self.timeout)

    def preferences(self):
        """
        Get user preference information for the logged in user.
        :return: User Preferences and Streaming Info
        :rtype: request.Response
        """
        response = requests.get(f'{self._base_api_url}/trader/v1/userPreference',
                                headers={'Authorization': f'Bearer {self.access_token}'},
                                timeout=self.timeout)
        return eval(response.content.decode(), {"true": True, "false": False, "null": None})


