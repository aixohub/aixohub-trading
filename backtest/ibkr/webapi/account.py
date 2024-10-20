import requests
import urllib3

urllib3.disable_warnings()

baseUrl = "http://localhost:4002/v1/api"



if __name__ == '__main__':
    # request_url = f"{baseUrl}/iserver/account/mta"
    # request_url = f"{baseUrl}/v1/api/ws"
    # request_url = f"{baseUrl}/portfolio/subaccounts"
    request_url = f"{baseUrl}/iserver/account/trades"
    request_url = f"{baseUrl}/iserver/account/pnl/partitioned"
    request_url = f"{baseUrl}/iserver/accounts"
    # request_url = f"{baseUrl}/iserver/account/orders"
    # request_url = f"{baseUrl}/iserver/account/trade"
    # request_url = f"{baseUrl}/iserver/auth/status"
    response = requests.get(url=request_url)
    if response.status_code == 200:
        token_data = response.json()
        print(token_data)
    else:
        raise Exception(f"Failed : {response.text}")