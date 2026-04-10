from .models import HousingProduct, FinanceProduct, WelfareProduct
from .firebase_service import FirebaseManager
import random
from datetime import datetime, date, timedelta
from django.db.models import Q

class MatchingEngine:
    """청춘로 지능형 초정밀(Housing/Finance/Welfare) 매칭 엔진 v20 (17 Regions Edition)"""

    # 🗺️ 전국 17개 광역자치단체 키워드 매핑 (중앙 관리)
    REGION_KEYWORD_MAP = {
        'Seoul': '서울', 'Busan': '부산', 'Daegu': '대구', 'Incheon': '인천', 
        'Gwangju': '광주', 'Daejeon': '대전', 'Ulsan': '울산', 'Sejong': '세종', 
        'Gyeonggi': '경기', 'Gangwon': '강원', 'Chungbuk': '충북', 'Chungnam': '충남', 
        'Jeonbuk': '전북', 'Jeonnam': '전남', 'Gyeongbuk': '경북', 'Gyeongnam': '경남', 
        'Jeju': '제주'
    }

    @staticmethod
    def get_default_item(category_name, message=None):
        return {
            "top_1": {
                "title": f"조건에 부합하는 {category_name} 상품을 찾고 있습니다.",
                "name": f"{category_name} 상품 정밀 분석 중",
                "org": "청춘로 분석 엔진",
                "bank_nm": "청춘로",
                "base_rate": "-",
                "limit": "-",
                "benefit": "현재 조건에서 가입 가능한 상품을 정밀 검색 중입니다.",
                "url": "#",
                "is_default": True
            },
            "list": [],
            "reason": message or f"사용자님의 조건에 가장 근접한 {category_name} 정보를 추출 중입니다.",
            "category": category_name
        }

    @staticmethod
    def calculate_simulation(instance, collateral_value=None):
        """DSR(40%) / LTV(70%) 기반 가상 대출 한도 시뮬레이션 (단위: 만원)"""
        income = instance.total_income
        debt = instance.debt
        
        # 1. LTV 기준 한도 (주택가액의 70%)
        house_val = collateral_value or (income * 10)
        ltv_limit = int(house_val * 0.7)
        
        # 2. DSR 기준 한도 (원리금 상환액이 소득의 40% 이내)
        dsr_limit = max(0, int(income * 8) - debt)
        
        # 3. 상품 최대 한도 (일반 청년 대출 5억 상한)
        product_max = 50000
        
        calculated_limit = min(ltv_limit, dsr_limit, product_max)
        
        # 예상 금리 (소득 구간별 차등 예시)
        rate = 4.2
        if income < 3000: rate = 2.1
        elif income < 5000: rate = 3.2
        elif instance.kids_count > 0 or instance.is_pregnant: rate = 1.8 
        
        # 4. 월 예상 납입 이자 (만원 단위)
        # 공식: (대출금액 * 이자율 / 100) / 12
        monthly_interest = int((calculated_limit * rate / 100) / 12)
        
        return {
            "max_limit": calculated_limit,
            "expected_rate": rate,
            "monthly_interest": monthly_interest,
            "ltv": 70,
            "dsr": 40
        }

    @staticmethod
    def is_eligible_housing(instance, product):
        """[STRICT] 주거 상품 지능형 필터링 v17"""
        title = product.get('title', '')
        region = product.get('region', '')
        sales_price = product.get('sales_price', 0)
        income = instance.total_income
        
        # 1. PIR 기초 필터링 (가용 자본 대비 지나친 고가 매물 제외)
        if sales_price > 0:
            pir = sales_price / income
            if pir > 15: return False 
        
        # 2. 순자산 컷오프 (엄격 적용)
        net_assets = instance.assets - instance.debt
        if net_assets > 37900: # 2024년 기준 자산 기준
            if any(term in title for term in ["국민임대", "행복주택", "영구임대", "공공분양", "LH", "SH"]): 
                return False
        
        # 3. 지역 일치 여부 (핵심 필터)
        target_keyword = MatchingEngine.REGION_KEYWORD_MAP.get(instance.region, '')
        product_region = product.get('region', '')
        
        if target_keyword and target_keyword not in product_region: 
            return False
            
        # [v20] 세부 지역(시/군/구) 정밀 필터링
        if instance.sub_region:
            # 공고의 지역명이나 제목에 세부 지역명이 포함되어 있는지 확인
            if instance.sub_region not in product_region and instance.sub_region not in title:
                # 만약 공고가 해당 '도' 전체 대상이 아니라 특정 '시' 대상인 경우 필터링
                if any(city in product_region for city in MatchingEngine.REGION_KEYWORD_MAP.values() if city != target_keyword):
                    return False
            
        # 4. 모집 기간 필터링 (Strict - 과거 공고 배제)
        today = date.today()
        end_date = product.get('end_date')
        notice_date = product.get('notice_date')
        
        # 날짜 정보가 아예 없는 유령 데이터/과거 시장 데이터 배제
        if not end_date and not notice_date:
            return False
            
        if end_date and end_date < today:
            return False
        
        if not end_date and notice_date:
            from datetime import timedelta
            if notice_date < (today - timedelta(days=180)):
                return False
            
        # 5. [v19] 무주택 및 소유 이력 필터링 (Hyper-Strict)
        # 공공임대 및 대부분의 청약은 무주택 필수
        if not instance.is_homeless:
            # 유주택자는 일반 민영 청약 외에는 배제
            if any(x in product.get('category', '') for x in ["국민", "임대", "공공"]):
                return False
        
        # 생애최초 전용 매물인데 소유 이력이 있는 경우 배제
        if "생애최초" in product.get('title', '') and not instance.is_first_home:
            return False

        return True

    @staticmethod
    def analyze_housing(instance):
        """[STRICT] 주거: 현실성 검증 및 추천 로직"""
        try:
            reg_key = MatchingEngine.REGION_KEYWORD_MAP.get(instance.region, '')
            
            # 필터링 및 점수화 (사용자 지역 + 전국 공고 통합)
            # 🎯 STRICT: 모집 종료일이 오늘 이후이거나 없는 경우(미정)만 포함
            today = date.today()
            local_products = list(HousingProduct.objects.filter(
                Q(region__icontains=reg_key) | Q(region__icontains="전용") | Q(region__icontains="전국"),
                Q(end_date__gte=today) | Q(end_date__isnull=True),
                is_active=True
            ).order_by('-notice_date')[:50])
            
            valid = []
            
            for p in local_products:
                s_price = getattr(p, 'sales_price', 0)
                if not s_price: s_price = instance.total_income * 6
                
                p_data = {
                    'title': p.title, 'org': p.org, 'region': p.region,
                    'sales_price': s_price, 'end_date': p.end_date, 
                    'notice_date': p.notice_date, 'url': p.url or '#', 'score': 0
                }
                
                if MatchingEngine.is_eligible_housing(instance, p_data):
                    pir = s_price / max(instance.total_income, 1)
                    score = 1000 - int(pir * 30)
                    if "공공" in p.category or any(x in p.title for x in ["LH", "SH", "행복"]): score += 300
                    if instance.subscription_count >= 24: score += 100
                    # [v19] 무주택 기간 가점 (최대 150점)
                    score += min(instance.homeless_years * 10, 150)
                    p_data['score'] = score
                    valid.append(p_data)
                # else: print(f"SKIP: {p_data['title']} filtered out")
                    
            if not valid:
                return MatchingEngine.get_default_item("주거", "현재 모집 중인 적격 공고가 없습니다. 추후 공고를 기다려주세요.")

            valid.sort(key=lambda x: x['score'], reverse=True)
            top = valid[0]
            pir_val = round(top['sales_price']/max(instance.total_income, 1), 1)
            
            return { 
                "top_1": top, 
                "list": valid[1:11], 
                "reason": f"거주지({reg_key}) 및 소득 대비 현실적 주거 비용(PIR {pir_val}배)을 고려한 최적 매물입니다. (현재 모집 중)" 
            }
        except Exception as e:
            # print(f"Housing Error: {e}")
            return MatchingEngine.get_default_item("주거")

    @staticmethod
    def analyze_finance(instance):
        """[STRICT] 금융: 소득 및 상황별 적격성 무한 대조"""
        try:
            local = list(FinanceProduct.objects.filter(is_active=True)[:100])
            valid = []
            sim = MatchingEngine.calculate_simulation(instance)
            
            for p in local:
                title = p.title
                
                # [STRICT] 모집 기간 필터링
                if p.end_date and p.end_date < date.today():
                    continue
                
                # [v19] 생애최초/무주택 자격 필터링
                if "생애최초" in title and not instance.is_first_home:
                    continue
                if any(x in title for x in ["무주택", "디딤돌", "버팀목"]) and not instance.is_homeless:
                    continue

                score = 100
                
                # [v19] 타겟팅 가산점 (생애최초/신혼부부 전용)
                if "생애최초" in title and instance.is_first_home:
                    score += 500
                if "신혼부부" in title and "Married" in instance.marital_status:
                    score += 500
                if "청년" in title and instance.age < 35:
                    score += 300
                if (instance.kids_count > 0 or instance.is_pregnant) and "신생아" in title: 
                    score += 800
                if instance.marital_status in ['Engaged', 'Married'] and "신혼부부" in title:
                    score += 500
                
                # 2. 소득 기반 엄격 필터링
                income = instance.total_income
                income_limit = 999999
                if "버팀목" in title: income_limit = 6000
                elif "청년전용" in title: income_limit = 5000
                elif "신생아" in title: income_limit = 13000
                elif "신혼부부" in title: income_limit = 7500
                
                if income > income_limit: continue
                
                # 3. 금리 기반 점수화 (저금리 우대)
                rate = p.base_rate or 4.0
                score += int((5.0 - rate) * 100)
                
                valid.append({
                    'name': p.title,
                    'bank_nm': p.bank_nm,
                    'base_rate': rate,
                    'limit': min(p.limit_amt // 10000 if p.limit_amt > 0 else 50000, sim['max_limit']),
                    'url': p.url or '#',
                    'score': score
                })
            
            if not valid: return MatchingEngine.get_default_item("금융")
            valid.sort(key=lambda x: x['score'], reverse=True)
            
            return {
                "top_1": valid[0],
                "list": valid[1:6],
                "reason": f"사용자님의 {instance.get_marital_status_display()} 상태와 소득({instance.total_income}만원)에서 최저 금리가 예상되는 상품입니다."
            }
        except Exception:
            return MatchingEngine.get_default_item("금융")

    @staticmethod
    def calculate_welfare_score(instance, policy):
        """복지 상품 정밀 스코어링 엔진 v17"""
        score = 0
        title = policy.title
        target = policy.target_desc or ""
        
        # 1. 연령 적합도 (만 39세 미만 청년 기본형)
        if instance.age <= 34: score += 300
        elif instance.age <= 39: score += 100
        else: return -1 # 연령 미달 칼같이 탈락
        
        # 2. 지역 가점 (v20 정밀화)
        reg_key = MatchingEngine.REGION_KEYWORD_MAP.get(instance.region, '')
        if reg_key and (reg_key in policy.region or "전국" in policy.region):
            score += 500
            
        # [v20] 세부 지역(시/군/구) 보너스 가점
        if instance.sub_region and instance.sub_region in (policy.region or ""):
            score += 1000 # 해당 기초지자체 전용 정책은 아주 높은 우선순위 부여
            
        # 3. 상황적 키워드 매칭 (Strict & Negative)
        marital = instance.marital_status
        is_parent = (instance.kids_count > 0 or instance.is_pregnant)
        
        # 긍정 매칭
        if marital == 'Single' and any(x in target for x in ["미혼", "1인", "독신"]): score += 200
        if marital in ['Engaged', 'Married'] and any(x in target for x in ["신혼", "부부", "혼인"]): score += 400
        if is_parent and any(x in target for x in ["자녀", "출산", "임신", "양육"]): score += 500
        
        # 부정 매칭 (오차 차단: 기혼자에게 미혼 전용 정책 추천 방지 등)
        if marital != 'Single' and any(x in target for x in ["미혼 전용", "1인 가구 한정"]): score -= 1000
        if marital == 'Single' and "신혼부부 전용" in target: score -= 1000
        if not is_parent and "다자녀 가구" in target: score -= 500
        
        return score

    @staticmethod
    def analyze_welfare(instance):
        """[STRICT] 복지: 스코어링 시스템 기반 최적 정책 선별"""
        try:
            reg_key = MatchingEngine.REGION_KEYWORD_MAP.get(instance.region, '')
            
            # 사용자 지역 혹은 전국 정책 통합 검색 (기타 지역일 경우 전국 공고만)
            query = Q(region__icontains="전국") | Q(region__isnull=True)
            if reg_key:
                query |= Q(region__icontains=reg_key)
            
            local = list(WelfareProduct.objects.filter(query, is_active=True)[:150])
            valid = []
            
            for p in local:
                # [STRICT] 모집 기간 필터링
                today = date.today()
                if p.end_date and p.end_date < today:
                    continue
                
                # 공고일이 너무 오래된 경우 (1년 이상) 배제 (상시 정책 제외)
                if p.notice_date and p.notice_date < (today - timedelta(days=365)):
                    if not any(x in p.title for x in ["상시", "기본", "급여"]):
                        continue

                score = MatchingEngine.calculate_welfare_score(instance, p)
                if score < 0: continue
                
                valid.append({
                    'name': p.title,
                    'org': p.org_nm,
                    'benefit': p.benefit_desc,
                    'url': p.url or '#',
                    'score': score
                })
            
            if not valid: return MatchingEngine.get_default_item("복지")
            
            # 점수 기준 정렬
            valid.sort(key=lambda x: x['score'], reverse=True)
            
            return { 
                "top_1": valid[0], 
                "list": valid[1:11], 
                "reason": f"회원님의 생애주기({instance.get_marital_status_display()})와 연령에 가장 특화된 혜택을 1순위에 배치했습니다." 
            }
        except Exception:
            return MatchingEngine.get_default_item("복지")

    @classmethod
    def get_full_report(cls, instance):
        """안티그래비티 전문가 보고서 통합 출력 (v20 Premium Visual)"""
        sim = cls.calculate_simulation(instance)
        housing = cls.analyze_housing(instance)
        finance = cls.analyze_finance(instance)
        welfare = cls.analyze_welfare(instance)
        
        # 📊 시각화 전용 데이터 엔진 (Radar & Donut)
        # 6축: 주거안정, 금융파워, 복지수혜, 자산안전, 청년대응, 미래성장
        radar_scores = {
            "주거": min(100, (housing.get('top_1', {}).get('score', 0) / 1300) * 100) if housing.get('top_1') else 40,
            "금융": min(100, (finance.get('top_1', {}).get('score', 0) / 1500) * 100) if finance.get('top_1') else 30,
            "복지": min(100, (welfare.get('top_1', {}).get('score', 0) / 1000) * 100) if welfare.get('top_1') else 50,
            "안전": max(20, 100 - (instance.debt / max(instance.total_income, 1) * 10)),
            "청년": 100 if instance.age <= 34 else 60,
            "성장": min(100, (instance.total_income / 1000) * 10 + 20)
        }

        return {
            "housing": housing,
            "finance": finance,
            "welfare": welfare,
            "user_summary": { 
                "total_income": instance.total_income, 
                "assets": instance.assets, 
                "debt": instance.debt, 
                "age": instance.age,
                "marital_desc": instance.get_marital_status_display(),
                "kid_status": "자녀/임신" if (instance.kids_count > 0 or instance.is_pregnant) else "미해당"
            },
            "financial_simulation": sim,
            "chart_data": {
                "assets": instance.assets,
                "debt": instance.debt,
                "projected_loan": sim['max_limit'],
                "radar": radar_scores
            }
        }
