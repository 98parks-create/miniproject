from django.db import models
from django.contrib.auth.models import User

# 🗺️ 전국 17개 시도별 세부 시/군/구 데이터 매핑 (v20 Premium)
REGIONAL_DATA = {
    'Seoul': ['강남구', '강동구', '강북구', '강서구', '관악구', '광진구', '구로구', '금천구', '노원구', '도봉구', '동대문구', '동작구', '마포구', '서대문구', '서초구', '성동구', '성북구', '송파구', '양천구', '영등포구', '용산구', '은평구', '종로구', '중구', '중랑구'],
    'Busan': ['강서구', '금정구', '기장군', '남구', '동구', '동래구', '부산진구', '북구', '사상구', '사하구', '서구', '수영구', '연제구', '영도구', '중구', '해운대구'],
    'Daegu': ['남구', '달서구', '달성군', '동구', '북구', '서구', '수성구', '중구', '군위군'],
    'Incheon': ['강화군', '계양구', '미추홀구', '남동구', '동구', '부평구', '서구', '연수구', '옹진군', '중구'],
    'Gwangju': ['광산구', '남구', '동구', '북구', '서구'],
    'Daejeon': ['대덕구', '동구', '서구', '유성구', '중구'],
    'Ulsan': ['남구', '동구', '북구', '울주군', '중구'],
    'Sejong': ['세종특별자치시'],
    'Gyeonggi': ['수원시', '성남시', '의정부시', '안양시', '부천시', '광명시', '평택시', '동두천시', '안산시', '고양시', '과천시', '구리시', '남양주시', '오산시', '시흥시', '군포시', '의왕시', '하남시', '용인시', '파주시', '이천시', '안성시', '김포시', '화성시', '광주시', '양주시', '포천시', '여주시', '연천군', '가평군', '양평군'],
    'Gangwon': ['춘천시', '원주시', '강릉시', '동해시', '태백시', '속초시', '삼척시', '홍천군', '횡성군', '영월군', '평창군', '정선군', '철원군', '화천군', '양구군', '인제군', '고성군', '양양군'],
    'Chungbuk': ['청주시', '충주시', '제천시', '보은군', '옥천군', '영동군', '증평군', '진천군', '괴산군', '음성군', '단양군'],
    'Chungnam': ['천안시', '공주시', '보령시', '아산시', '서산시', '논산시', '계룡시', '당진시', '금산군', '부여군', '서천군', '청양군', '홍성군', '예산군', '태안군'],
    'Jeonbuk': ['전주시', '군산시', '익산시', '정읍시', '남원시', '김제시', '완주군', '진안군', '무주군', '장수군', '임실군', '순창군', '고창군', '부안군'],
    'Jeonnam': ['목포시', '여수시', '순천시', '나주시', '광양시', '담양군', '곡성군', '구례군', '고흥군', '보성군', '화순군', '장흥군', '강진군', '해남군', '영암군', '무안군', '함평군', '영광군', '장성군', '완도군', '진도군', '신안군'],
    'Gyeongbuk': ['포항시', '경주시', '김천시', '안동시', '구미시', '영주시', '영천시', '상주시', '문경시', '경산시', '의성군', '청송군', '영양군', '영덕군', '청도군', '고령군', '성주군', '칠곡군', '예천군', '봉화군', '울진군', '울릉군'],
    'Gyeongnam': ['창원시', '진주시', '통영시', '사천시', '김해시', '밀양시', '거제시', '양산시', '의령군', '함안군', '창녕군', '고성군', '남해군', '하동군', '산청군', '함양군', '거창군', '합천군'],
    'Jeju': ['제주시', '서귀포시']
}

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
        ('Seoul', '서울특별시'),
        ('Busan', '부산광역시'),
        ('Daegu', '대구광역시'),
        ('Incheon', '인천광역시'),
        ('Gwangju', '광주광역시'),
        ('Daejeon', '대전광역시'),
        ('Ulsan', '울산광역시'),
        ('Sejong', '세종특별자치시'),
        ('Gyeonggi', '경기도'),
        ('Gangwon', '강원특별자치도'),
        ('Chungbuk', '충청북도'),
        ('Chungnam', '충청남도'),
        ('Jeonbuk', '전북특별자치도'),
        ('Jeonnam', '전라남도'),
        ('Gyeongbuk', '경상북도'),
        ('Gyeongnam', '경상남도'),
        ('Jeju', '제주특별자치도'),
    ]
    MARITAL_CHOICES = [
        ('Single', '미혼'),
        ('Engaged', '예비신혼'),
        ('Married', '신혼부부(7년 이내)'),
        ('Other', '기타'),
    ]

    age = models.IntegerField(verbose_name="연령", default=29)
    region = models.CharField(max_length=20, choices=REGION_CHOICES, default='Seoul', verbose_name="거주지 (시/도)")
    sub_region = models.CharField(max_length=50, null=True, blank=True, verbose_name="세부 지역 (시/군/구)")
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
    org = models.CharField(max_length=100, null=True, blank=True, verbose_name="시행/시공사")
    
    is_active = models.BooleanField(default=True)
    raw_data = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

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
    
    benefit_desc = models.TextField(verbose_name="지원내용")
    target_desc = models.TextField(verbose_name="지원대상")
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
