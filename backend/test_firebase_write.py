import os
import django
import sys

# Django 환경 설정
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from youth_road.firebase_service import FirebaseManager

def force_test_write():
    print("🚀 [Firebase Force Write Test] Starting...")
    db = FirebaseManager.get_db()
    
    if db is None:
        print("❌ Firebase DB connection failed during singleton initialization.")
        return

    try:
        # 🧪 테스트 데이터 1건 쓰기
        test_data = {
            "id": "TEST_ID_123",
            "title": "Firebase 연결 테스트 성공",
            "org": "AI_AGENT",
            "content": "이 데이터가 보인다면 연결이 정상입니다.",
            "timestamp": django.utils.timezone.now().isoformat()
        }
        
        doc_ref = db.collection('debug_test').document('TEST_ID_123')
        doc_ref.set(test_data)
        print("✅ SUCCESS: Test data written to 'debug_test' collection!")
    except Exception as e:
        print(f"❌ FAILED: Write error: {e}")

if __name__ == "__main__":
    force_test_write()
