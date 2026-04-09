import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from chatbot.models import Policy, UserProfile

def seed():
    # 1. Create User Profile
    UserProfile.objects.get_or_create(
        name='기본 사용자',
        defaults={
            'age': 29,
            'income': 4500,
            'ai_persona': '친절하고 상세한 대한민국 정책 전문 분석관'
        }
    )
    
    # 2. Create Sample Policies for AI Context
    policies = [
        {
            'category': 'Finance', 'title': '청년 버팀목 전세자금대출',
            'summary': '연 1.8~2.7% 저금리로 최대 2억원까지 전세 보증금 지원',
            'tags': ['청년전용', '전세자금', '저금리']
        },
        {
            'category': 'Housing', 'title': '청년 월세 특별지원',
            'summary': '매월 최대 20만원씩 12개월간 월세 지원 (무주택 청년 대상)',
            'tags': ['월세지원', '현금지원']
        }
    ]
    
    for p in policies:
        Policy.objects.get_or_create(title=p['title'], defaults=p)

    print("✅ 데이터 시딩 완료!")

if __name__ == '__main__':
    seed()
