import os
import requests
import xml.etree.ElementTree as ET
import environ
from pathlib import Path

# Initialize environ
env = environ.Env()
BASE_DIR = Path(__file__).resolve().parent
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

def test_lh_api():
    api_key = env('DATA_PORTAL_KEY', default='').strip()
    print(f"📡 LH 공고 API 연동 테스트 시작...")
    
    # 서울 지역(11) 임대공고(05) 테스트 (최신 엔드포인트 경로 반영)
    url = "http://apis.data.go.kr/B552555/lhLeaseNotice1/lhLeaseNotice1/getLeaseNoticeInfo1"
    params = {
        'serviceKey': api_key,
        'PG_SZ': '1',
        'PAGE': '1',
        'CNP_CD': '11',
        'UPP_AIS_TP_CD': '05' # 🛠️ 필수 파라미터 보강 (임대주택)
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"🔄 Status Code: {response.status_code}")
        
        # 🧪 XML 응답 전체 출력 (주요 부분)
        print("💬 RAW XML Response:")
        print(response.text[:1000])
        
        if "SERVICE_KEY_IS_NOT_REGISTERED_ERROR" in response.text:
            print("\n❌ 에러: 공공데이터포털에 등록되지 않은 서비스 키입니다. (키 복사 오류 혹은 승인 대기 필요)")
        elif "<item>" in response.text:
            print("\n✅ 성공: 데이터가 존재합니다. 파싱 로직을 점검하면 됩니다.")
        else:
            print("\n⚠️ 경고: 연결은 되었으나 데이터가 없습니다. (검색 조건 혹은 상세 API 키 권한 확인 필요)")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")

if __name__ == "__main__":
    test_lh_api()
