import time
from django.core.management.base import BaseCommand
from web_for_youth.youth_road.services import RegionMapper, PublicDataHousingService, SubscriptionHomeService, OntongWelfareService
from web_for_youth.youth_road.firebase_service import FirebaseManager

class Command(BaseCommand):
    help = '🧭 전국의 모든 주거 및 정책 데이터를 Firebase Firestore에 대량으로 동기화합니다.'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('🚀 전국 데이터 동기화 엔진 가동 시작...'))
        
        # 🌏 수집 대상 지역 목록
        regions = ['Seoul', 'Gyeonggi', 'Incheon', 'Busan', 'Daegu', 'Gwangju', 'Daejeon', 'Ulsan', 'Sejong', 'Gangwon', 'Chungbuk', 'Chungnam', 'Jeonbuk', 'Jeonnam', 'Gyeongbuk', 'Gyeongnam', 'Jeju']
        
        # 🏠 1. 주거 데이터 수집 (LH 및 청약홈)
        self.stdout.write('🏠 주거 데이터(LH, 청약홈) 수집 중...')
        housing_types = ['01', '05', '06', '15'] # 행복주택, 국민임대, 분양, 전세임대
        
        for region in regions:
            self.stdout.write(f'  - {region} 지역 주거 정보 수집 중...')
            # LH 유형별 수집
            for t in housing_types:
                try:
                    PublicDataHousingService.get_lh_sh_notices(region, type_code=t)
                    time.sleep(0.5) # API 부하 방지
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'    ❌ LH ({t}) 에러: {e}'))
            
            # 청약홈 수집
            try:
                SubscriptionHomeService.get_subscription_notices(region)
                time.sleep(0.5)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'    ❌ 청약홈 에러: {e}'))

        # 🎁 2. 청년 정책 데이터 수집 (온통청년)
        self.stdout.write('🎁 청년 정책 데이터(온통청년) 수집 중...')
        for region in regions:
            self.stdout.write(f'  - {region} 지역 정책 정보 수집 중...')
            try:
                OntongWelfareService.get_welfare_policies(30, region) # 연령대 샘플 30세
                time.sleep(0.5)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'    ❌ 온통청년 에러: {e}'))

        self.stdout.write(self.style.SUCCESS('✨ 모든 데이터 동기화가 완료되었습니다. Firebase 콘솔에서 확인하세요!'))
