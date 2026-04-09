import os
import requests
import xml.etree.ElementTree as ET
from urllib.parse import unquote

# .env 파일을 수동으로 읽어오기 (dotenv 라이브러리 의존성 없이)
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

def debug_apis():
    load_env()
    raw_key = os.getenv('DATA_PORTAL_KEY')
    youth_key = os.getenv('YOUTH_CENTER_KEY')
    fss_key = os.getenv('FSS_FINANCE_KEY')
    
    if not raw_key:
        print("ERROR: DATA_PORTAL_KEY not found in .env!")
        return
        
    decoded_key = unquote(raw_key)

    print(f"--- Debugging MyHome (API Key: {decoded_key[:10]}...) ---")
    url = "http://apis.data.go.kr/1613000/HWSPR02/rsdtRcritNtcList"
    params = {'serviceKey': decoded_key, 'numOfRows': 3, 'pageNo': 1} # _type 제거
    try:
        res = requests.get(url, params=params, timeout=15)
        print(f"Status: {res.status_code}")
        print(f"Response Header: {res.headers.get('Content-Type')}")
        print(f"Raw Response: {res.text[:800]}")
    except Exception as e: print(f"Error: {e}")

    print(f"\n--- Debugging HUG (API Key: {raw_key[:10]}...) ---")
    url = "https://api.odcloud.kr/api/15134235/v1/uddi:fc11f971-d890-484d-a2f0-f02787884d50"
    params = {'serviceKey': raw_key, 'page': 1, 'perPage': 3}
    try:
        res = requests.get(url, params=params, timeout=15)
        print(f"Status: {res.status_code}")
        print(f"JSON Response Keys: {res.json().keys()}")
        if 'data' in res.json(): print(f"Sample Data: {res.json()['data'][0] if res.json()['data'] else 'EMPTY'}")
    except Exception as e: print(f"Error: {e}")

    print(f"\n--- Debugging Bokjiro (B554287) ---")
    url = "http://apis.data.go.kr/B554287/NationalWelfareInformationsV001/NationalWelfarelistV001"
    params = {'serviceKey': decoded_key, 'callTp': 'L', 'numOfRows': 3, 'pageNo': 1}
    try:
        res = requests.get(url, params=params, timeout=15)
        print(f"Status: {res.status_code}")
        print(f"Raw Response: {res.text[:800]}")
    except Exception as e: print(f"Error: {e}")

    print(f"\n--- Debugging YouthCenter (Key: {youth_key[:10]}...) ---")
    url = "https://www.youthcenter.go.kr/opi/youthPlcyList.do"
    params = {'openApiVlak': youth_key, 'display': 3, 'pageIndex': 1}
    try:
        res = requests.get(url, params=params, timeout=15)
        print(f"Status: {res.status_code}")
        print(f"Raw Response: {res.text[:800]}")
    except Exception as e: print(f"Error: {e}")

if __name__ == "__main__":
    debug_apis()
