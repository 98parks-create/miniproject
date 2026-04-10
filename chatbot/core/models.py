from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver

class Policy(models.Model):
    # ... (Keep existing Policy model as is)
    CATEGORY_CHOICES = [
        ('Finance', '금융'),
        ('Housing', '주거'),
        ('Welfare', '복지'),
        ('Employment', '고용/취업'),
        ('Legal', '법률/상담'),
        ('Youth', '청년특화'),
        ('Institution', '제도/민원'),
    ]
    
    category = models.CharField(max_length=20, choices=CATEGORY_CHOICES)
    title = models.CharField(max_length=200)
    summary = models.TextField()
    description = models.TextField(blank=True, null=True)
    tags = models.JSONField(default=list, blank=True)
    
    # 필터링 기준들
    age_max = models.IntegerField(null=True, blank=True)
    age_min = models.IntegerField(null=True, blank=True, default=0)
    income_limit = models.IntegerField(null=True, blank=True, help_text="합산 연소득 제한 (만원)")
    asset_limit = models.IntegerField(null=True, blank=True, help_text="보유 자산 제한 (만원)")
    requires_kids = models.BooleanField(default=False)
    marital_status = models.JSONField(default=list, blank=True, help_text="['single', 'newly', 'expecting'] 등")
    
    # 정밀 진단 요건 추가
    is_non_homeowner_only = models.BooleanField(default=False, help_text="무주택자 전용 여부")
    employment_types = models.JSONField(default=list, blank=True, help_text="['employee', 'business', 'unemployed', 'freelancer'] 등")
    is_low_income_only = models.BooleanField(default=False, help_text="기초생활수급자/차상위 전용 여부")
    is_disabled_only = models.BooleanField(default=False, help_text="장애인 전용 여부")
    
    # 시뮬레이션용 기술 필드 추가
    interest_rate = models.DecimalField(max_digits=4, decimal_places=2, null=True, blank=True, help_text="평균 금리 (%)")
    max_limit = models.IntegerField(null=True, blank=True, help_text="최대 한도/수혜액 (만원)")
    benefit_type = models.CharField(max_length=20, choices=[('Loan', '대출'), ('Grant', '보조금'), ('Savings', '저축')], null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"[{self.category}] {self.title}"

    class Meta:
        verbose_name = "정책"
        verbose_name_plural = "정책 목록"

class UserProfile(models.Model):
    """DB에 등록된 '그 사람들'을 위한 프로필 모델"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, null=True, blank=True)
    name = models.CharField(max_length=50, verbose_name="이름/식별자", blank=True)
    age = models.IntegerField(default=29, verbose_name="만 나이")
    income = models.IntegerField(default=5000, verbose_name="연소득 (만원)")
    region = models.CharField(max_length=50, default="seoul", verbose_name="지역")
    sub_region = models.CharField(max_length=50, blank=True, null=True, verbose_name="세부 지역")
    is_non_homeowner = models.BooleanField(default=True, verbose_name="무주택 여부")
    
    personal_api_key = models.CharField(max_length=100, blank=True, null=True, help_text="사용자 전용 API 키")
    ai_persona = models.TextField(blank=True, null=True, help_text="AI 비서의 성격")

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user.username if self.user else self.name}"

    class Meta:
        verbose_name = "사용자 프로필"
        verbose_name_plural = "사용자 프로필 목록"

@receiver(post_save, sender=User)
def create_user_profile(_sender, instance, created, **_kwargs):
    if created:
        UserProfile.objects.create(user=instance, name=instance.username)

@receiver(post_save, sender=User)
def save_user_profile(_sender, instance, **_kwargs):
    if hasattr(instance, 'userprofile'):
        instance.userprofile.save()
