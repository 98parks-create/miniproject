from django import forms
from .models import UserDiagnostic

# 청춘로(路) 전역 선택 옵션 정의 (models.py에서 중앙 관리)
REGION_CHOICES = UserDiagnostic.REGION_CHOICES

MARITAL_CHOICES = [
    ('Single', '미혼'),
    ('Engaged', '예비부부 (1년 이내 결혼 예정)'),
    ('Married_1', '신혼부부 (2년 이내)'),
    ('Married_2', '신혼부부 (7년 이내)'),
]

class DiagnosticForm(forms.ModelForm):
    """모델 필드와 100% 정규화된 지능형 진단 폼 (보정 완료)"""
    
    class Meta:
        model = UserDiagnostic
        fields = [
            'age', 'region', 'sub_region', 'marital_status', 'total_income', 'assets', 
            'debt', 'subscription_amount', 'kids_count', 'is_pregnant', 
            'subscription_count', 'is_first_home', 'is_homeless', 'homeless_years'
        ]
        
        # 장고 모델 표준 필드명(snake_case)에 커스텀 위젯 매핑
        widgets = {
            'age': forms.NumberInput(attrs={'class': 'input-standard', 'min': 19, 'max': 49}),
            'region': forms.Select(choices=REGION_CHOICES, attrs={'class': 'input-standard', 'id': 'id_region'}),
            'sub_region': forms.Select(choices=[], attrs={'class': 'input-standard', 'id': 'id_sub_region'}),
            'marital_status': forms.Select(choices=MARITAL_CHOICES, attrs={'class': 'input-standard'}),
            'total_income': forms.NumberInput(attrs={'class': 'input-standard', 'placeholder': '단위: 만원'}),
            'assets': forms.NumberInput(attrs={'class': 'input-standard', 'placeholder': '단위: 만원'}),
            'debt': forms.NumberInput(attrs={'class': 'input-standard', 'placeholder': '단위: 만원'}),
            'subscription_amount': forms.NumberInput(attrs={'class': 'input-standard', 'placeholder': '단위: 만원'}),
            'kids_count': forms.NumberInput(attrs={'class': 'input-standard', 'min': 0}),
            'subscription_count': forms.NumberInput(attrs={'class': 'input-standard', 'min': 0, 'placeholder': '단위: 개월'}),
            'is_pregnant': forms.CheckboxInput(attrs={'style': 'width: 24px; height: 24px;'}),
            'is_first_home': forms.CheckboxInput(attrs={'style': 'width: 24px; height: 24px;'}),
            'is_homeless': forms.CheckboxInput(attrs={'style': 'width: 24px; height: 24px;'}),
            'homeless_years': forms.NumberInput(attrs={'class': 'input-standard', 'min': 0, 'max': 15}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 레이블 수동 보정 (한국어 명칭 유지)
        self.fields['age'].label = "만 나이"
        self.fields['region'].label = "거주 시/도"
        self.fields['sub_region'].label = "거주 시/군/구"
        self.fields['marital_status'].label = "혼인 상태"
        self.fields['total_income'].label = "합산 연소득 (만원)"
        self.fields['assets'].label = "보유 자산 (만원)"
        self.fields['debt'].label = "부채 규모 (만원)"
        self.fields['subscription_amount'].label = "주택청약 납입 총액 (만원)"
        self.fields['kids_count'].label = "자녀 수 (명)"
        self.fields['is_pregnant'].label = "임신 여부"
        self.fields['subscription_count'].label = "청약통장 가입 기간 (개월)"
        self.fields['is_first_home'].label = "생애최초 주택구입 여부 (평생 무주택)"
        self.fields['is_homeless'].label = "현재 무주택 여부 (세대원 포함)"
        self.fields['homeless_years'].label = "무주택 기간 (0~15년)"
