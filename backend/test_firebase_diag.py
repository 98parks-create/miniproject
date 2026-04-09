import os
import django
import sys

# Django 환경 설정
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from youth_road.firebase_service import FirebaseManager

def diagnose():
    print("🔍 [Firebase Diagnosis] Starting...")
    db = FirebaseManager.get_db()
    
    if db is None:
        print("❌ Firebase DB connection failed.")
        return

    # 1. 문서 개수 확인
    housing_count = len(list(db.collection('housing_notices').limit(5).stream()))
    welfare_count = len(list(db.collection('welfare_policies').limit(5).stream()))
    
    print(f"📊 Housing Notices Sample: {housing_count} docs found.")
    print(f"📊 Welfare Policies Sample: {welfare_count} docs found.")

    # 2. 지역 매칭 테스트 (서울)
    test_region = "서울"
    results = FirebaseManager.fetch_archive('housing_notices', test_region)
    print(f"📍 Region Match Test ({test_region}): {len(results)} items found.")

    if len(results) == 0:
        # 데이터 샘플 출력 (필드 확인용)
        sample = list(db.collection('housing_notices').limit(1).stream())
        if sample:
            print("📝 Sample Data Field Check:", sample[0].to_dict())
        else:
            print("⚠️ No data in 'housing_notices' collection.")

if __name__ == "__main__":
    diagnose()
