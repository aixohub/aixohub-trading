import threading
import time

import requests



if __name__ == '__main__':


    # 请求 URL
    url = "https://www.futunn.com/quote-api/quote-v2/get-financial-list?beginDate=20250124&market=2&stockType=0&from=0&count=20"

    # 请求头
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9",
        "cookie": "cipher_device_id=1736221408679352; _gcl_au=1.1.333840100.1736221410; _fbp=fb.1.1736221410810.148926295814520016; csrfToken=Mz9Wdo457phlrhiYd1shaMmV; locale=zh-cn; locale.sig=ObiqV0BmZw7fEycdGJRoK-Q0Yeuop294gBeiHL1LqgQ; device_id=1736221408679352; Hm_lvt_f3ecfeb354419b501942b6f9caf8d0db=1737691463; HMACCOUNT=FBAE959879D47658; futu-csrf=fGqhsE3su1HXywKkmJRo+f5aYcY=; _gid=GA1.2.1905023747.1737691465; _uetsid=fa17da40da0811ef9ce23da59dd3c720; _uetvid=fa180fb0da0811efb7aba30b0d85f569; _ga_NTZDYESDX1=GS1.2.1737691756.2.0.1737691756.60.0.0; uid=16263383; web_sig=uWMNuWUEZXsa%2BVGnz4VTzB2ZCjMIE5x%2FUVlM2uZ8jG%2FpB4kbmKlsOL%2F8iFAMi3Kc8bk%2B6HEbNpmdOeBIeL%2BrdaXX0676%2Bg3hs4qajZqe6OGqCMkpm6wz8EfA4l7xL6rPVJ4KHvE7syoBH4Pu44Is%2FA%3D%3D; ci_sig=%2BpbGOYfbVGRKLiXaZIoM%2F4ggJIFhPTEwMDAwNTM4JmI9MjAxMTM2Jms9QUtJRFZvdWxPbGNoS2lxUmxxbFlKcDhCMWFDYW1qYmM5U2JxJmU9MTc0MDI4Mzc3OSZ0PTE3Mzc2OTE3Nzkmcj00NzU0MTY1NzkmdT0mZj0%3D; _ga_K1RSSMGBHL=GS1.1.1737691755.2.1.1737691779.0.0.0; Hm_lpvt_f3ecfeb354419b501942b6f9caf8d0db=1737699991; ftreport-jssdk%40session={%22distinctId%22:%22ftv1L3XCdIiJKZvvbWKpPOeXFNvraA4FnpIOM1mu7r9sUNL9fIn58vSM73e6YznEOO5X%22%2C%22firstId%22:%22ftv1VAmLfNSMZH2Sp8HHzEvvu+pGkRR8VvJTciqgkfrL/IhR24kGr6CszyQe16JlbR9T%22%2C%22latestReferrer%22:%22https://www.futunn.com/quote/calendar%22}; _gat_UA-71722593-3=1; _ga_XECT8CPR37=GS1.1.1737699991.3.0.1737699991.60.0.0; _ga=GA1.1.1318014100.1736221410; _ga_370Q8HQYD7=GS1.2.1737699992.3.0.1737699992.60.0.0; _ga_EJJJZFNPTW=GS1.1.1737699991.3.1.1737699994.0.0.0",
        "futu-x-csrf-token": "Mz9Wdo457phlrhiYd1shaMmV",
        "priority": "u=1, i",
        "quote-token": "969690f6fa",
        "referer": "https://www.futunn.com/quote/calendar",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }
    # 发送请求
    response = requests.get(url, headers=headers)
    data = response.json()
    print(data)

