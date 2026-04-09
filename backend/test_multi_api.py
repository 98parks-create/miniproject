import os
import requests
import xml.etree.ElementTree as ET
import environ
from pathlib import Path

# Initialize environ
env = environ.Env()
BASE_DIR = Path(__file__).resolve().parent
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

def test_youth_welfare_api():
    print("🎁 [Welfare] 온통청년 정책 API 연동 테스트 시작...")
    api_key = env('YOUTH_CENTER_KEY', default='').strip()
    url = f"https://www.youthcenter.go.kr/opi/youthPlcyList.do?display=5&pageIndex=1&openApiVlak={api_key}"
    
    try:
        response = requests.get(url, timeout=10)
        print(f"🔄 Status Code: {response.status_code}")
        if "<totalCnt>" in response.text:
            root = ET.fromstring(response.text)
            total = root.findtext('.//totalCnt')
            print(f"✅ SUCCESS: {total} policies found.")
        else:
            print(f"❌ FAILED: Response doesn't contain results. Msg: {response.text[:200]}")
    except Exception as e:
        print(f"❌ ERROR: {e}")

def test_apply_home_api():
    print("\n🏠 [Housing] 청약홈 분양정보 API 연동 테스트 시작...")
    api_key = env('DATA_PORTAL_KEY', default='').strip()
    # APT 분양정보 엔드포인트
    url = "https://apis.data.go.kr/1613000/ApplyHomeInfoService/getLttotPblancList"
    params = {
        'serviceKey': api_key,
        'numOfRows': '5',
        'pageNo': '1'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"🔄 Status Code: {response.status_code}")
        if "<totalCount>" in response.text:
            print(f"✅ SUCCESS: Data found in ApplyHome!")
        else:
            print(f"❌ FAILED: {response.text[:200]}")
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    test_youth_welfare_api()
    test_apply_home_api()
