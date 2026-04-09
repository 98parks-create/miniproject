from django.db import models
from django.contrib.auth.models import User

class UserDiagnostic(models.Model):
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        related_name='diagnostics',
        null=True, 
        blank=True,
        help_text="정보를 입력한 사용자 계정 (익명 가능)"
    )

    REGION_CHOICES = [
        ('Seoul', '서울'), ('Gyeonggi', '경기'), ('Incheon', '인천'), ('Busan', '부산'),
        ('Daegu', '대구'), ('Gwangju', '광주'), ('Daejeon', '대전'), ('Ulsan', '울산'),
        ('Sejong', '세종'), ('Gangwon', '강원'), ('Chungbuk', '충북'), ('Chungnam', '충남'),
        ('Jeonbuk', '전북'), ('Jeonnam', '전남'), ('Gyeongbuk', '경북'), ('Gyeongnam', '경남'),
        ('Jeju', '제주'), ('Other', '기타'),
    ]
    MARITAL_CHOICES = [
        ('Single', '미혼'),
        ('Engaged', '예비신혼'),
        ('Married', '신혼부부(7년 이내)'),
        ('Other', '기타'),
    ]

    age = models.IntegerField(verbose_name="연령", default=29)
    region = models.CharField(max_length=20, choices=REGION_CHOICES, default='Seoul', verbose_name="거주지")
    marital_status = models.CharField(max_length=20, choices=MARITAL_CHOICES, default='Single', verbose_name="혼인상태")
    kids_count = models.IntegerField(default=0, verbose_name="자녀 수")
    is_pregnant = models.BooleanField(default=False, verbose_name="임신여부")

    total_income = models.IntegerField(help_text="가구 합산 연소득 (단위: 만원)", verbose_name="연소득")
    assets = models.IntegerField(help_text="가구 총 자산 (단위: 만원)", verbose_name="보유 자산")
    debt = models.IntegerField(default=0, help_text="현재 부채 규모 (단위: 만원)", verbose_name="부채 규모")

    subscription_count = models.IntegerField(default=24, verbose_name="청약통장 납입 횟수")
    subscription_amount = models.IntegerField(default=240, help_text="청약 총 불입 금액 (단위: 만원)", verbose_name="청약 총액")

    # [v19] 주택 소유 여부 및 조건
    is_first_home = models.BooleanField(default=True, verbose_name="생애최초 주택구입 여부", help_text="태어나서 지금까지 집을 소유한 적이 없는 경우")
    is_homeless = models.BooleanField(default=True, verbose_name="현재 무주택 여부", help_text="현재 본인 및 세대원 전원이 무주택인 경우")
    homeless_years = models.IntegerField(default=0, verbose_name="무주택 기간 (년)", help_text="무주택 기간이 0~15년(이상) 중 해당되는 기간")

    created_at = models.DateTimeField(auto_now_add=True, verbose_name="진단 일자")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="수정 일자")

    class Meta:
        verbose_name = "사용자 진단 기록"
        verbose_name_plural = "사용자 진단 기록 목록"
        ordering = ['-created_at']

    def __str__(self):
        username = self.user.username if self.user else "익명 사용자"
        return f"{username}의 진단 기록 ({self.created_at.strftime('%Y-%m-%d')})"

class HousingProduct(models.Model):
    """주거 전용: 청약홈, SH, LH 모집 공고"""
    manage_no = models.CharField(max_length=100, unique=True, verbose_name="주택관리번호")
    pblanc_no = models.CharField(max_length=100, null=True, blank=True, verbose_name="공고번호")
    title = models.CharField(max_length=255, verbose_name="주택명")
    category = models.CharField(max_length=100, null=True, blank=True, verbose_name="주택구분")
    region = models.CharField(max_length=100, null=True, blank=True, verbose_name="공급지역")
    location = models.TextField(null=True, blank=True, verbose_name="공급위치")
    
    notice_date = models.DateField(null=True, blank=True, verbose_name="모집공고일")
    start_date = models.DateField(null=True, blank=True, verbose_name="접수시작일")
    end_date = models.DateField(null=True, blank=True, verbose_name="접수종료일")
    
    url = models.URLField(max_length=500, null=True, blank=True, verbose_name="공고상세URL")
    org = models.CharField(max_length=255, null=True, blank=True, verbose_name="주관 기관")
    
    # [v21] 실거래가 및 분양가 정보 추가
    sales_price = models.IntegerField(default=0, verbose_name="주택 가격", help_text="단위: 만원 (분양가 또는 매매 시세)")
    
    is_active = models.BooleanField(default=True, verbose_name="활성화 여부")
    raw_data = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    @property
    def formatted_price(self):
        if not self.sales_price:
            return "정보 없음"
        if self.sales_price >= 10000:
            eok = self.sales_price // 10000
            man = self.sales_price % 10000
            if man > 0:
                return f"{eok}억 {man:,}만"
            return f"{eok}억"
        return f"{self.sales_price:,}만"

    class Meta:
        verbose_name = "주거 상품"
        verbose_name_plural = "주거 상품 목록"
        ordering = ['-notice_date']

    def __str__(self):
        return f"[{self.region}] {self.title}"

class FinanceProduct(models.Model):
    """금융 전용: HUG, 시중은행 대출 상품"""
    product_id = models.CharField(max_length=100, unique=True, verbose_name="상품ID")
    title = models.CharField(max_length=255, verbose_name="상품명")
    bank_nm = models.CharField(max_length=100, verbose_name="금융기관")
    category = models.CharField(max_length=100, default="대출", verbose_name="상품구분")
    
    base_rate = models.FloatField(default=0.0, verbose_name="기본금리")
    max_rate = models.FloatField(default=0.0, verbose_name="우대금리포함")
    limit_amt = models.BigIntegerField(default=0, verbose_name="대출한도(원)")
    
    target_desc = models.TextField(null=True, blank=True, verbose_name="지원대상")
    notice_date = models.DateField(null=True, blank=True, verbose_name="공고일")
    end_date = models.DateField(null=True, blank=True, verbose_name="종료일")
    url = models.URLField(max_length=500, null=True, blank=True, verbose_name="상세URL")
    
    is_active = models.BooleanField(default=True)
    raw_data = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "금융 상품"
        verbose_name_plural = "금융 상품 목록"

    def __str__(self):
        return f"({self.bank_nm}) {self.title}"

class WelfareProduct(models.Model):
    """복지 전용: 복지로, 온통청년 정책 및 수당"""
    policy_id = models.CharField(max_length=100, unique=True, verbose_name="정책ID")
    title = models.CharField(max_length=255, verbose_name="정책명")
    org_nm = models.CharField(max_length=100, verbose_name="주관기관")
    category = models.CharField(max_length=100, default="복지정책", verbose_name="정책구분")
    
    benefit_desc = models.TextField(null=True, blank=True, verbose_name="지원내용")
    target_desc = models.TextField(null=True, blank=True, verbose_name="지원대상")
    age_limit = models.CharField(max_length=100, null=True, blank=True, verbose_name="연령제한")
    region = models.CharField(max_length=100, null=True, blank=True, verbose_name="지원지역")
    notice_date = models.DateField(null=True, blank=True, verbose_name="공고일")
    end_date = models.DateField(null=True, blank=True, verbose_name="종료일")
    
    url = models.URLField(max_length=500, null=True, blank=True, verbose_name="상세URL")
    
    is_active = models.BooleanField(default=True)
    raw_data = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "복지 상품"
        verbose_name_plural = "복지 상품 목록"

    def __str__(self):
        return f"[{self.org_nm}] {self.title}"

class HousingMarketData(models.Model):
    region = models.CharField(max_length=100)
    complex_name = models.CharField(max_length=200)
    category = models.CharField(max_length=50, blank=True, null=True)
    avg_competition_rate = models.FloatField(default=0.0)
    avg_winner_score = models.FloatField(default=0.0)
    avg_winner_age = models.FloatField(default=0.0)
    sales_price = models.BigIntegerField(default=0)
    price_per_meter = models.BigIntegerField(default=0)
    data_year = models.IntegerField(default=2024)
    raw_data = models.JSONField(default=dict)
    source = models.CharField(max_length=100, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "주거 시장 데이터"
        verbose_name_plural = "주거 시장 데이터 목록"
