import os
import django
import sys

# Setup Django environment
sys.path.append(os.path.join(os.getcwd(), 'backend'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from youth_road.services import PublicDataHousingService, FssFinanceService, OntongWelfareService
from chatbot.core.services import ask_expert_ai

def verify_repairs():
    print("--- 🩺 청춘로(路) 통합 진단 및 검증 시작 ---")
    
    # 1. DB 및 모델 로드 테스트
    try:
        from youth_road.models import HousingProduct
        count = HousingProduct.objects.count()
        print(f"🟢 [DB] 데이터베이스 연결 및 조희 성공 (HousingProduct Count: {count})")
    except Exception as e:
        print(f"🔴 [DB] 데이터베이스 연결 실패: {e}")

    # 2. Gemini AI 연동 테스트
    try:
        print("\n--- 🤖 Gemini AI 테스트 ---")
        dummy_user = {"name": "테스터", "age": 28, "income": 3500}
        response = ask_expert_ai("디딤돌 대출에 대해 설명해줘", user_data=dummy_user)
        if response and len(response) > 50:
            print(f"🟢 [AI] Gemini 응답 성공 (길이: {len(response)})")
            print(f"AI 응답 샘플: {response[:100]}...")
        else:
            print("🔴 [AI] Gemini 응답 부실 또는 실패")
    except Exception as e:
        print(f"🔴 [AI] Gemini 연동 오류: {e}")

    # 3. 외부 API 서비스 레이어 테스트 (서비스 레이어가 직접 호출되는지 확인)
    print("\n--- 📡 외부 API 서비스 레이어 테스트 ---")
    
    # LH API
    try:
        housing = PublicDataHousingService.get_lh_sh_notices("Seoul")
        print(f"🟢 [LH] 주거 공고 서비스 호출 성공 (가져온 데이터 개수: {len(housing)})")
    except Exception as e:
        print(f"🔴 [LH] 주거 서비스 오류: {e}")

    # FSS API
    try:
        loans = FssFinanceService.get_loan_products(4000, "Single")
        print(f"🟢 [FSS] 금융 상품 서비스 호출 성공 (가져온 데이터 개수: {len(loans)})")
    except Exception as e:
        print(f"🔴 [FSS] 금융 서비스 오류: {e}")

    # Welfare API
    try:
        welfare = OntongWelfareService.get_welfare_policies(29, "Seoul")
        print(f"🟢 [Bokji] 복지 정책 서비스 호출 성공 (가져온 데이터 개수: {len(welfare)})")
    except Exception as e:
        print(f"🔴 [Bokji] 복지 서비스 오류: {e}")

    print("\n--- ✅ 검증 완료 ---")

if __name__ == "__main__":
    verify_repairs()
