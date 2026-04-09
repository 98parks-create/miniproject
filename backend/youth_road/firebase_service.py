import os
import firebase_admin
from firebase_admin import credentials, firestore
from django.conf import settings

class FirebaseManager:
    """Firebase Firestore 연동 및 실시간 동기화 엔진"""
    _db = None

    @classmethod
    def get_db(cls):
        if cls._db is None:
            try:
                # 서비스 계정 키 경로 (backend/config/firebase-key.json)
                key_path = os.path.join(settings.BASE_DIR, 'config', 'firebase-key.json')
                
                if not os.path.exists(key_path):
                    print(f"Warning: Firebase Key not found at {key_path}")
                    return None

                cred = credentials.Certificate(key_path)
                
                # 이미 초기화되었는지 확인
                if not firebase_admin._apps:
                    firebase_admin.initialize_app(cred)
                
                cls._db = firestore.client()
                print("Firebase Firestore Initialized Successfully.")
            except Exception as e:
                print(f"Firebase Initialization Error: {e}")
                return None
        return cls._db

    @classmethod
    def sync_data(cls, collection_name, data_list, id_field='id'):
        """데이터를 Firestore에 업서트(Upsert)하여 아카이빙합니다."""
        db = cls.get_db()
        if db is None: return False

        try:
            collection_ref = db.collection(collection_name)
            
            # Firestore Batch limit: 500 units per commit
            chunk_size = 450 
            for i in range(0, len(data_list), chunk_size):
                batch = db.batch()
                chunk = data_list[i:i + chunk_size]
                
                for item in chunk:
                    doc_id = item.get(id_field)
                    if not doc_id: continue
                    
                    doc_ref = collection_ref.document(str(doc_id))
                    batch.set(doc_ref, item, merge=True)
                
                batch.commit()
                
            print(f"[Firebase] {len(data_list)} items synced to '{collection_name}' collection via chunked batches.")
            return True
        except Exception as e:
            print(f"Firebase Sync Error ({collection_name}): {e}")
            return False

    @classmethod
    def fetch_archive(cls, collection_name, region_name=None):
        """Firebase에 저장된 아카이브 데이터를 불러옵니다."""
        db = cls.get_db()
        if db is None: return []

        try:
            query = db.collection(collection_name)
            if region_name:
                # 지역별 필터링
                docs = query.stream()
                results = []
                for d in docs:
                    data = d.to_dict()
                    # 지역 매칭 (대소문자 무시 및 부분 일치)
                    region = str(data.get('region', '')).lower()
                    title = str(data.get('title', '')).lower()
                    target_region = str(region_name).lower()
                    
                    if target_region in region or target_region in title:
                        results.append(data)
                return results
            else:
                return [d.to_dict() for d in query.stream()]
        except Exception as e:
            print(f"Firebase Fetch Error ({collection_name}): {e}")
            return []
