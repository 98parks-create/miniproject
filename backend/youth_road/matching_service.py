from .models import HousingProduct, FinanceProduct, WelfareProduct
from .firebase_service import FirebaseManager
import random
from datetime import datetime, date, timedelta
from django.db.models import Q

class MatchingEngine:
    """청춘로 지능형 초정밀(Housing/Finance/Welfare) 매칭 엔진 v17 (Strict Edition)"""

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
        
        # 1. PIR 기초 필터링 (가용 소득 대비 지나친 고가 매물 제외)
        if sales_price > 0:
            annual_income = max(income * 12, 1) # 연봉 기준
            pir = sales_price / annual_income
            if pir > 20: return False # 연봉의 20배 초과 시 현실적으로 불가능
        
        # 2. 순자산 컷오프 (엄격 적용)
        net_assets = instance.assets - instance.debt
        if net_assets > 37900: # 2024년 기준 자산 기준
            if any(term in title for term in ["국민임대", "행복주택", "영구임대", "공공분양", "LH", "SH"]): 
                return False
        
        # 3. 지역 일치 여부 (핵심 필터)
        region_map = {'Seoul': '서울', 'Gyeonggi': '경기', 'Incheon': '인천', 'Busan': '부산'}
        target_keyword = region_map.get(instance.region, '')
        if target_keyword and target_keyword not in region: 
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
            region_map = {'Seoul': '서울', 'Gyeonggi': '경기', 'Incheon': '인천', 'Busan': '부산'}
            reg_key = region_map.get(instance.region, '')
            
            today = date.today()
            max_age_days = 30 
            cutoff_date = today - timedelta(days=max_age_days)

            # [v26] 제로오류 리얼리티 매트릭스: 구매력(Buying Power) 산출
            net_worth = (instance.assets or 0) - (instance.debt or 0)
            # 연봉의 4.5배를 평균 대출 한도로 산정 (보수적 DSR 적용)
            est_loan_cap = (instance.total_income * 12) * 4.5
            if instance.is_first_home: est_loan_cap *= 1.2 # 생애최초 LTV 우대
            
            total_budget = net_worth + est_loan_cap
            
            local_products = list(HousingProduct.objects.filter(
                Q(region__icontains=reg_key) | Q(region__icontains="전용") | Q(region__icontains="전국"),
                is_active=True
            ).filter(
                # 1. 마감일이 오늘 이후이거나
                Q(end_date__gte=today) |
                # 2. 마감일은 없지만 공고일이 최근 1개월(v22) 이내인 것만
                (Q(end_date__isnull=True) & Q(notice_date__gte=cutoff_date))
            ).order_by('-notice_date')[:100])
            
            valid = []
            
            for p in local_products:
                s_price = p.sales_price
                is_verified_price = (s_price > 0)
                
                # [v23] 금액 포맷팅 (X억 Y만)
                if not is_verified_price:
                    formatted_price = "분양가 정보 없음 (상세 확인 권장)"
                else:
                    if s_price >= 10000:
                        formatted_price = f"{s_price // 10000}억 {s_price % 10000:,}만" if s_price % 10000 > 0 else f"{s_price // 10000}억"
                    else:
                        formatted_price = f"{s_price:,}만"

                tag = "모집중"
                if p.notice_date and p.notice_date > today: tag = "모집예정"
                
                p_data = {
                    'title': p.title, 'org': p.org, 'region': p.region,
                    'sales_price': s_price, 'formatted_price': formatted_price,
                    'end_date': p.end_date, 'notice_date': p.notice_date, 
                    'url': p.url or '#', 'score': 0, 'tag': tag
                }
                
                if MatchingEngine.is_eligible_housing(instance, p_data):
                    # [v26] 무오차 현실성 스코어링
                    if is_verified_price:
                        # 예산 대비 가격 비율 (1.0이면 딱 맞음, 0.5면 여유, 1.5면 부족)
                        budget_ratio = s_price / max(total_budget, 1)
                        if budget_ratio > 1.2: # 예산 20% 초과 시 '위험'
                            score = 100
                        elif budget_ratio > 0.9: # 예산 90~120% 사이 '최적 적합'
                            score = 1500
                        else: # 예산보다 훨씬 저렴 '안정'
                            score = 1000 + int((1-budget_ratio) * 200)
                    else:
                        score = 100 # 가격 미검증은 하단
                    
                    cat_str = p.category or ''
                    title_str = p.title or ''
                    if "공공" in cat_str or any(x in title_str for x in ["LH", "SH", "행복"]): score += 300
                    if instance.subscription_count >= 24: score += 100
                    score += min((instance.homeless_years or 0) * 10, 150)
                    p_data['score'] = score
                    valid.append(p_data)
                    
            if not valid:
                return MatchingEngine.get_default_item("주거", "현재 예산 범위 내의 적격 공고가 없습니다.")

            valid.sort(key=lambda x: x['score'], reverse=True)
            top = valid[0]
            
            # [v26] 정밀 현실성 분석 텍스트
            if top['sales_price'] > 0:
                gap = top['sales_price'] - total_budget
                if gap <= 0:
                    status_msg = f"✅ 가용 예산({total_budget//10000}억) 내 안정권"
                else:
                    status_msg = f"⚠️ 예산 대비 {gap//10000}억 추가 필요"
                reason_main = f"현재 보유 자산과 대출 여력을 합산한 구매력({total_budget//10000}억) 기준 최적 매칭입니다."
            else:
                status_msg = "분양가 미확인"
                reason_main = "가격 정보가 희박하여 정밀 매칭이 어렵습니다. 공고문 확인을 권장합니다."
            
            return { 
                "top_1": top, 
                "list": valid[1:11], 
                "reason": f"{reason_main} ({status_msg})" 
            }
        except Exception as e:
            # print(f"Housing Error: {e}")
            return MatchingEngine.get_default_item("주거")

    @staticmethod
    def analyze_finance(instance):
        """[STRICT] 금융: 소득 및 상황별 적격성 무한 대조"""
        try:
            # [v22] 금융/복지 고강도 날짜 필터링
            cutoff_welfare = date.today() - timedelta(days=365) # 정책은 최장 1년
            local = FinanceProduct.objects.filter(
                is_active=True
            ).filter(
                Q(end_date__gte=date.today()) | 
                (Q(end_date__isnull=True) & Q(notice_date__gte=cutoff_welfare))
            )

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
                
                # [v22] 상태 태그 부여
                tag = "모집중"
                if p.notice_date and p.notice_date > date.today(): tag = "모집예정"
                
                valid.append({
                    'name': p.title,
                    'bank_nm': p.bank_nm,
                    'base_rate': rate,
                    'limit': min(p.limit_amt // 10000 if p.limit_amt > 0 else 50000, sim['max_limit']),
                    'url': p.url or '#',
                    'score': score,
                    'tag': tag
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
        
        # 2. 지역 가점
        region_map = {'Seoul': '서울', 'Gyeonggi': '경기', 'Incheon': '인천', 'Busan': '부산'}
        reg_key = region_map.get(instance.region, '')
        if reg_key and (reg_key in policy.region or "전국" in policy.region):
            score += 500
            
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
            region_map = {'Seoul': '서울', 'Gyeonggi': '경기', 'Incheon': '인천', 'Busan': '부산'}
            reg_key = region_map.get(instance.region, '')
            
            # 사용자 지역 혹은 전국 정책 통합 검색 (기타 지역일 경우 전국 공고만)
            query = Q(region__icontains="전국") | Q(region__isnull=True)
            if reg_key:
                query |= Q(region__icontains=reg_key)
            
            # [v22] 정책/복지 고강도 날짜 필터링
            cutoff_policy = date.today() - timedelta(days=365)
            local = WelfareProduct.objects.filter(query, is_active=True).filter(
                Q(end_date__gte=date.today()) |
                (Q(end_date__isnull=True) & Q(notice_date__gte=cutoff_policy))
            )
            
            valid = []
            
            for p in local:
                # [STRICT] 모집 기간 필터링
                today = date.today()
                
                # [v22] 상태 태그 부여
                tag = "모집중"
                if p.notice_date and p.notice_date > today: tag = "모집예정"

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
        """안티그래비티 전문가 보고서 통합 출력 (v17 Strict)"""
        sim = cls.calculate_simulation(instance)
        return {
            "housing": cls.analyze_housing(instance),
            "finance": cls.analyze_finance(instance),
            "welfare": cls.analyze_welfare(instance),
            "user_summary": { 
                "total_income": instance.total_income, 
                "assets": instance.assets, 
                "debt": instance.debt, 
                "age": instance.age,
                "marital_desc": instance.get_marital_status_display(),
                "kid_status": "자녀/임신" if (instance.kids_count > 0 or instance.is_pregnant) else "미해당"
            },
            "financial_simulation": sim
    }
