import os
import django
import sys
from datetime import date

sys.path.append(os.path.join(os.getcwd(), 'backend'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from youth_road.matching_service import MatchingEngine
from youth_road.models import UserDiagnostic, HousingProduct, FinanceProduct

def run_test():
    print("--- 🧪 Hyper-Strict v19 Logic Verification ---")
    
    # [Case 1] 유주택자 (Homeowner)
    print("\n[테스트 1] 유주택자 (Single, Homeowner)")
    u1 = UserDiagnostic(
        age=30, region='Seoul', marital_status='Single', 
        total_income=4000, assets=10000, 
        is_homeless=False, is_first_home=False, homeless_years=0
    )
    report1 = MatchingEngine.get_full_report(u1)
    
    f1_top = report1['finance']['top_1'].get('name', 'N/A')
    print(f"💰 금융 추천: {f1_top}")
    if any(x in f1_top for x in ["디딤돌", "버팀목", "생애최초"]):
        print("❌ 실패: 유주택자에게 무주택 전용 대출이 추천됨")
    else:
        print("✅ 성공: 무주택/생애최초 전용 상품 배제됨")

    # [Case 2] 신혼부부 + 생애최초 (Married + First-time)
    print("\n[테스트 2] 신혼부부 + 생애최초 (Married, Homeless, First-time)")
    u2 = UserDiagnostic(
        age=32, region='Seoul', marital_status='Married_1', 
        total_income=6000, assets=20000, 
        is_homeless=True, is_first_home=True, homeless_years=5
    )
    report2 = MatchingEngine.get_full_report(u2)
    
    f2_top = report2['finance']['top_1'].get('name', 'N/A')
    print(f"💰 금융 추천: {f2_top}")
    if "신혼부부" in f2_top and "생애최초" in f2_top:
        print("✅ 성공: 신혼부부 생애최초 특례 상품 매칭됨")
    else:
        print("❓ 확인 필요: 가장 적합한 상품이 1순위인지 검토")

    print("\n--- ✅ v19 검증 완료 ---")

if __name__ == "__main__":
    run_test()
