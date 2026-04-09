import os
import requests
import xml.etree.ElementTree as ET
from urllib.parse import unquote

# Mock environ
DATA_PORTAL_KEY = "c59c241aebb26596d40484c1946d9af59feb143da8f8b2f23027d7bef61b6e3c"
YOUTH_CENTER_KEY = "ffc1326f-318b-4637-ae86-b20daebca082"
FSS_FINANCE_KEY = "dbb23ecc96bfb397fdd9f8377a280fdd" # Removed suffix for testing

def test_lh():
    print("Testing LH API (Public Data Portal)...")
    base_url = "http://apis.data.go.kr/B552555/lhLeaseNotice1/lhLeaseNotice1/getLeaseNoticeInfo1"
    
    strategies = [
        ("Raw Key", DATA_PORTAL_KEY),
        ("Unquoted Key", unquote(DATA_PORTAL_KEY))
    ]
    
    for label, key in strategies:
        print(f"\n--- Strategy: {label} ---")
        full_url = f"{base_url}?serviceKey={key}&PG_SZ=10&PAGE=1&CNP_CD=11&UPP_AIS_TP_CD=05"
        try:
            res = requests.get(full_url, timeout=10)
            print(f"Status: {res.status_code}")
            if res.status_code == 200:
                print(f"Sample: {res.text[:150]}")
                if "SERVICE_KEY_IS_NOT_REGISTERED" in res.text:
                    print("Auth Result: Key Not Registered")
                elif "NORMAL_SERVICE" in res.text or "<item>" in res.text:
                    print("Auth Result: SUCCESS")
            else:
                print(f"Error Body: {res.text}")
        except Exception as e:
            print(f"Error: {e}")

def test_welfare():
    print("\nTesting Welfare API (Youth Center)...")
    url = f"https://www.youthcenter.go.kr/opi/youthPlcyList.do?display=10&pageIndex=1&openApiVlak={YOUTH_CENTER_KEY}"
    try:
        res = requests.get(url, timeout=15)
        print(f"Status: {res.status_code}")
        if res.status_code == 200:
             print(f"Sample: {res.text[:150]}")
             if "<youthPolicy>" in res.text:
                 print("Result: SUCCESS")
             else:
                 print("Result: No Policy Data Found (Empty Response)")
    except Exception as e:
        print(f"Welfare Error: {e}")

def test_fss():
    print("\nTesting FSS Finance API...")
    url = "http://finlife.fss.or.kr/api/leaseLoanProducts.json"
    params = {'auth': FSS_FINANCE_KEY, 'topFinGrpNo': '020000', 'pageNo': '1'}
    try:
        res = requests.get(url, params=params, timeout=10)
        print(f"Status: {res.status_code}")
        if res.status_code == 200:
            data = res.json()
            if data.get('result', {}).get('err_cd') == '000':
                print("Result: SUCCESS")
                print(f"Sample Product: {data['result']['baseList'][0]['fin_prdt_nm']}")
            else:
                print(f"Auth Error: {data.get('result', {}).get('err_msg')}")
    except Exception as e:
        print(f"FSS Error: {e}")

if __name__ == "__main__":
    test_lh()
    test_welfare()
    test_fss()
