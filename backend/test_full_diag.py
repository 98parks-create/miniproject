import os
import requests
import xml.etree.ElementTree as ET
import urllib.parse
import environ
from pathlib import Path

# Initialize environ
env = environ.Env()
BASE_DIR = Path(__file__).resolve().parent
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

def test_full_diagnostics():
    print("🛰️ [Full Diagnostic] 전국 전산망 마스터 진단 가동...")
    
    # 🏠 1. LH API (Safe Encoding Test)
    print("\n🏢 [LH] 연동 테스트 시작...")
    raw_key = env('DATA_PORTAL_KEY', default='').strip()
    api_key = urllib.parse.unquote(raw_key)
    base_lh = "http://apis.data.go.kr/B552555/lhLeaseNotice1/lhLeaseNotice1/getLeaseNoticeInfo1"
    # 서울(11), 임대(05)
    full_url_lh = f"{base_lh}?serviceKey={raw_key}&PG_SZ=10&PAGE=1&CNP_CD=11&UPP_AIS_TP_CD=05"
    
    try:
        res = requests.get(full_url_lh, timeout=15)
        print(f"🔄 LH Status: {res.status_code}")
        if res.status_code == 200 and "<item>" in res.text:
            print(f"✅ LH SUCCESS: Data identified.")
        else:
            print(f"❌ LH FAILED: {res.text[:100]}")
    except Exception as e:
        print(f"❌ LH ERROR: {e}")

    # 🏠 2. 청약홈 API (ApplyHome)
    print("\n🏠 [ApplyHome] 연동 테스트 시작...")
    url_ah = "http://apis.data.go.kr/B551232/getAptLttotPblancDetail"
    params_ah = {'serviceKey': raw_key, 'numOfRows': '10', 'pageNo': '1'}
    
    try:
        res = requests.get(url_ah, params=params_ah, timeout=15)
        print(f"🔄 ApplyHome Status: {res.status_code}")
        if res.status_code == 200:
            print(f"✅ ApplyHome SUCCESS: {res.text[:100]}...")
        else:
            print(f"❌ ApplyHome FAILED: {res.text[:100]}")
    except Exception as e:
        print(f"❌ ApplyHome ERROR: {e}")

    # 🎁 3. 온통청년 API (Welfare)
    print("\n🎁 [Welfare] 온통청년 API 연동 테스트 시작...")
    key_w = env('YOUTH_CENTER_KEY', default='').strip()
    url_w = f"https://www.youthcenter.go.kr/opi/youthPlcyList.do?display=10&pageIndex=1&openApiVlak={key_w}"
    
    try:
        res = requests.get(url_w, timeout=15)
        print(f"🔄 Welfare Status: {res.status_code}")
        if res.status_code == 200 and "<youthPolicy>" in res.text:
            print(f"✅ Welfare SUCCESS: Policies identified.")
        else:
            print(f"❌ Welfare FAILED: {res.text[:100]}")
    except Exception as e:
        print(f"❌ Welfare ERROR: {e}")

if __name__ == "__main__":
    test_full_diagnostics()
