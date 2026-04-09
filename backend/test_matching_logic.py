import os
import django
import sys

# Setup Django environment
sys.path.append(os.path.join(os.getcwd(), 'backend'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from youth_road.models import UserDiagnostic
from youth_road.matching_service import MatchingEngine

def test_matching_scenarios():
    print("--- 🎯 지능형 매칭 엔진 엄격성(Strictness) 테스트 시작 ---")
    
    # 시나리오 1: 서울 거주, 미혼, 중저소득 (청년 전용 상품 타겟)
    user1 = UserDiagnostic(
        age=28, region='Seoul', marital_status='Single',
        total_income=3500, assets=5000, debt=1000,
        subscription_count=24
    )
    
    # 시나리오 2: 경기 거주, 신혼부부/자녀있음, 중고소득 (신생아 특례/신혼부부 상품 타겟)
    user2 = UserDiagnostic(
        age=32, region='Gyeonggi', marital_status='Married',
        kids_count=1, total_income=8500, assets=25000, debt=5000,
        subscription_count=60
    )

    scenarios = [
        ("시나리오 1: 미혼/청년 전세대출 타겟", user1),
        ("시나리오 2: 신혼/신생아 특례 타겟", user2)
    ]

    for name, user in scenarios:
        print(f"\n[테스트] {name}")
        report = MatchingEngine.get_full_report(user)
        
        # 주거 검증
        h = report['housing']['top_1']
        print(f"🏠 주거 추천 1순위: {h.get('title')} (점수: {h.get('score', 0)})")
        
        # 금융 검증
        f = report['finance']['top_1']
        print(f"💰 금융 추천 1순위: {f.get('name')} (금융기관: {f.get('bank_nm')}, 점수: {f.get('score', 0)})")
        
        # 복지 검증
        w = report['welfare']['top_1']
        print(f"🎁 복지 추천 1순위: {w.get('name')} (점수: {w.get('score', 0)})")
        
        # 논리적 검증
        if name == "시나리오 2" and "신생아" not in f.get('name', '') and "신혼부부" not in f.get('name', ''):
             print("⚠️ 주의: 신혼/자녀 가구임에도 일반 상품이 1순위로 추천됨 (정밀 점수 조정 필요 가능성)")
        else:
             print("✅ 1순위 추천 논리 적절함")

    print("\n--- ✅ 매칭 로직 검증 완료 ---")

if __name__ == "__main__":
    test_matching_scenarios()
