from django.core.management.base import BaseCommand
from web_for_youth.chatbot.core.models import Policy

class Command(BaseCommand):
    help = 'Seed initial policy data into the database'

    def handle(self, *args, **options):
        policies = [
            # --- Finance ---
            {
                'category': 'Finance', 'title': '신생아 특례 디딤돌 대출',
                'summary': '최저 1.6% 금리로 최대 5억원 지원 (출산가구 전용)', 'tags': ['출산가구', '저금리', '최대5억'],
                'age_max': 50, 'income_limit': 13000, 'requires_kids': True, 'is_non_homeowner_only': True
            },
            {
                'category': 'Finance', 'title': '청년 버팀목 전세자금대출',
                'summary': '사회초년생을 위한 연 1.8~2.7% 저금리 전세 대출', 'tags': ['청년전용', '전세자금', '저금리'],
                'age_max': 34, 'income_limit': 5000, 'marital_status': ['single'],
                'is_non_homeowner_only': True, 'employment_types': ['employee', 'business']
            },
            {
                'category': 'Finance', 'title': '청년도약계좌',
                'summary': '5년 납입 시 최대 5,000만원 목돈 마련 (정부 기여금 매칭)', 'tags': ['자산형성', '고금리', '정부지원'],
                'age_max': 34, 'income_limit': 7500, 'employment_types': ['employee', 'business']
            },
            {
                'category': 'Finance', 'title': '버팀목 전세자금 (중소기업 취업청년)',
                'summary': '중소/중견기업 취업청년 대상 고정금급 1.2% 전세 대출', 'tags': ['중기청', '최저금리', '1억한도'],
                'age_max': 34, 'income_limit': 3500, 'employment_types': ['employee'], 'is_non_homeowner_only': True
            },
            {
                'category': 'Finance', 'title': 'K-패스 (지능형 교통비 환급)',
                'summary': '대중교통비의 최대 53% 환급 (청년층 30% 환급)', 'tags': ['교통비', '환급', '실생활혜택'],
                'age_max': 34
            },

            # --- Housing ---
            {
                'category': 'Housing', 'title': '청년 월세 특별지원 (2차)',
                'summary': '매월 최대 20만원, 최장 12개월 월세 지원 (부모 주택 미소유)', 'tags': ['월세지원', '현금지원', '국토부'],
                'age_max': 34, 'income_limit': 2400, 'marital_status': ['single'],
                'is_non_homeowner_only': True, 'is_low_income_only': True
            },
            {
                'category': 'Housing', 'title': 'LH 청년전세임대주택',
                'summary': 'LH가 집을 빌려 사용자에게 재임대하여 보증금 지원', 'tags': ['LH', '임대주택', '저렴한보증금'],
                'age_max': 39, 'income_limit': 4500, 'is_non_homeowner_only': True
            },
            {
                'category': 'Housing', 'title': 'SH 청년안심주택',
                'summary': '서울 역세권 고품질 임대주택 공급 (민간/공공 복합)', 'tags': ['서울전용', '역세권', '안심주택'],
                'age_max': 39, 'is_non_homeowner_only': True, 'marital_status': ['single', 'newly']
            },
            {
                'category': 'Housing', 'title': '행복주택 공급 입주 지원',
                'summary': '대학생, 청년, 신혼부부 대상 공공임내주택 (시세 60-80%)', 'tags': ['공공임대', '장기거주', '임대료저렴'],
                'age_max': 39, 'income_limit': 6000, 'is_non_homeowner_only': True
            },
            {
                'category': 'Housing', 'title': '청년 주택드림 청약통장',
                'summary': '청약 당첨 시 연 2.2% 저금리 대출 연계형 통장', 'tags': ['청약통장', '내집마련', '저금리연계'],
                'age_max': 34, 'income_limit': 5000, 'is_non_homeowner_only': True
            },

            # --- Welfare ---
            {
                'category': 'Welfare', 'title': '부모급여',
                'summary': '0세 월 100만원, 1세 월 50만원 현금 지급 (육아 비용 보전)', 'tags': ['육아지원', '현금복지', '영유아'],
                'requires_kids': True
            },
            {
                'category': 'Welfare', 'title': '청년내일저축계좌',
                'summary': '본인 10만원 저축 시 정부가 최대 30만원 매칭 지원', 'tags': ['자산형성', '정부매칭', '저소득지원'],
                'age_max': 34, 'income_limit': 2400, 'employment_types': ['employee', 'business']
            },
            {
                'category': 'Welfare', 'title': '국민취업지원제도 (I유형)',
                'summary': '구직촉진수당 월 50만원씩 6개월 지원 및 취업 서비스', 'tags': ['취업지원', '수당지원', '고용노동부'],
                'age_max': 34, 'income_limit': 2400, 'employment_types': ['unemployed']
            },
            {
                'category': 'Welfare', 'title': '청년문화예술패스',
                'summary': '19세 청년에게 문화생활 관람비 15만원 지원', 'tags': ['문화생활', '바우처', '19세전용'],
                'age_max': 19
            },
            {
                'category': 'Welfare', 'title': '경기도 청년 기본소득',
                'summary': '경기도 거주 청년에게 연 100만원 지역화폐 지급', 'tags': ['경기도', '기본소득', '지역화폐'],
                'age_max': 24
            }
        ]

        for p in policies:
            obj, created = Policy.objects.get_or_create(
                title=p['title'],
                defaults={
                    'category': p['category'],
                    'summary': p['summary'],
                    'tags': p.get('tags', []),
                    'age_max': p.get('age_max'),
                    'income_limit': p.get('income_limit'),
                    'requires_kids': p.get('requires_kids', False),
                    'marital_status': p.get('marital_status', []),
                    'is_non_homeowner_only': p.get('is_non_homeowner_only', False),
                    'employment_types': p.get('employment_types', []),
                    'is_low_income_only': p.get('is_low_income_only', False),
                    'is_disabled_only': p.get('is_disabled_only', False),
                }
            )
            if not created:
                obj.summary = p['summary']
                obj.tags = p.get('tags', [])
                obj.age_max = p.get('age_max')
                obj.income_limit = p.get('income_limit')
                obj.is_non_homeowner_only = p.get('is_non_homeowner_only', False)
                obj.employment_types = p.get('employment_types', [])
                obj.save()
        
        self.stdout.write(self.style.SUCCESS(f'Successfully expanded knowledge to {len(policies)} policies.'))
