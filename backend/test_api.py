import os
import requests
import environ
from pathlib import Path

# Initialize environ
env = environ.Env()
BASE_DIR = Path(__file__).resolve().parent
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

def test_odcloud_sh_multi():
    api_key = env('DATA_PORTAL_KEY', default='').strip()
    print(f"📡 Odcloud (SH 공공주택) 멀티 엔드포인트 연동 테스트")
    
    # 🧭 조사된 모든 후보 uddi 리스트
    uddi_list = [
        "a51daba9-b9e5-4f38-8e6c-7e793e25d25a", # 후보 1
        "a51daba9-b9e5-4453-bb04-c45681d40791", # 후보 2
        "00b957e8-e5e6-42d7-b847-aefa46d316e6"  # 후보 3
    ]
    
    for uddi in uddi_list:
        url = f"https://api.odcloud.kr/api/15099541/v1/uddi:{uddi}"
        print(f"\n🔍 Testing UDDI: {uddi}")
        
        params = {'page': 1, 'perPage': 5, 'serviceKey': api_key}
        headers = {"Authorization": f"Infuser {api_key}"}

        try:
            # 🚀 파라미터 호출
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                print(f"✅ 연동 성공 (Target: {uddi})")
                print(f"💬 데이터: {response.text[:200]}...")
                return
            
            # 🚀 헤더 호출 (Fallback)
            response = requests.get(url, params={'page': 1, 'perPage': 5}, headers=headers, timeout=10)
            if response.status_code == 200:
                print(f"✅ 연동 성공 (Header-based, Target: {uddi})")
                return

            print(f"❌ 실패 (Status: {response.status_code}, Msg: {response.text[:100]})")
            
        except Exception as e:
            print(f"❌ 에러: {e}")

    print("\n⚠️ 모든 후보 주소에서 '등록되지 않은 서비스' 혹은 인증 실패가 발생했습니다.")
    print("💡 원인: 사용자님의 키가 서버에 아직 동기화되지 않았거나(대기 1~2시간),")
    print("   해당 특정 '네임스페이스(15099541)'에 대해 다른 세부 키가 필요할 수 있습니다.")

if __name__ == "__main__":
    test_odcloud_sh_multi()
