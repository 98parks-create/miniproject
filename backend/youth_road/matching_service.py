from .models import HousingProduct, FinanceProduct, WelfareProduct
from .firebase_service import FirebaseManager
import random
from datetime import datetime, date, timedelta
from django.db.models import Q

class MatchingEngine:
    """청춘로 지능형 초정밀(Housing/Finance/Welfare) 매칭 엔진 v42.0 (Precision PCM Edition)"""

    # v42: 2025년 기준 중위소득 테이블 (단위: 원/월)
    MEDIAN_INCOME_2025 = {
        1: 2392013,
        2: 3932658,
        3: 5025353,
        4: 6097773,
        5: 7092347,
        6: 8022963
    }

    @staticmethod
    def get_income_criteria(household_size=1):
        """사용자 가구원 수에 따른 2025년 중위소득 기준표 반환"""
        base = MatchingEngine.MEDIAN_INCOME_2025.get(household_size, 6097773)
        return {
            "monthly_100": base,
            "monthly_120": int(base * 1.2),
            "monthly_150": int(base * 1.5),
            "annual_100": base * 12,
            "annual_120": int(base * 1.2 * 12),
            "annual_150": int(base * 1.5 * 12)
        }

    @staticmethod
    def get_default_item(category_name, message=None):
        return {
            "top_1": {
                "is_default": True, # v39.3: 타입 안정성 필드 추가
                "title": f"조건에 부합하는 {category_name} 상품을 찾고 있습니다.",
                "name": f"{category_name} 상품 정밀 분석 중",
                "org": "청춘로 분석 엔진",
                "bank_nm": "청춘로",
                "base_rate": "-",
                "limit": 0, # v39.3: 문자열이 아닌 0으로 고정
                "benefit": "현재 조건에서 가입 가능한 상품을 정밀 검색 중입니다.",
                "tag": "분석 중"
            },
            "list": [],
            "reason": message or f"현재 사용자님의 자본 및 소득 조건에서 수혜 가능한 실질적 {category_name} 혜택을 찾고 있습니다.",
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
        
        # 3. 상품 최대 한도 (v30.2 - 고액 자산가 대응을 위해 50억으로 상향)
        product_max = 500000
        
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
        # v31.1: 소득 단위 정상화 적용 (instance.total_income은 연봉)
        if sales_price > 0:
            annual_income = max(income, 1) # v31: 이미 연봉임
            pir = sales_price / annual_income
            if pir > 20 and instance.assets < 50000: return False 
        
        # 2. 순자산 컷오프 (엄격 적용)
        net_assets = instance.assets - instance.debt
        if net_assets > 37900: # 2024년 기준 자산 기준
            if any(term in title for term in ["국민임대", "행복주택", "영구임대", "공공분양", "LH", "SH"]): 
                return False
        
        # 3. 지역 일치 여부 (핵심 필터)
        # v38.1: 전국 17개 시도 매핑 동기화 및 변수 오타 수정
        region_map = {
            'Seoul': '서울', 'Gyeonggi': '경기', 'Incheon': '인천', 'Busan': '부산',
            'Daegu': '대구', 'Gwangju': '광주', 'Daejeon': '대전', 'Ulsan': '울산',
            'Sejong': '세종', 'Gangwon': '강원', 'Chungbuk': '충북', 'Chungnam': '충남',
            'Jeonbuk': '전북', 'Jeonnam': '전남', 'Gyeongbuk': '경북', 'Gyeongnam': '경남', 'Jeju': '제주'
        }
        target_keyword = region_map.get(instance.region, '')
        product_region = product.get('region', '')
        if target_keyword and target_keyword not in product_region: 
            return False
            
        # 4. 모집 기간 필터링 (Strict - 과거 공고 배제)
        today = date.today()
        end_date = product.get('end_date')
        notice_date = product.get('notice_date')
        
        # v38: [상시 모집] 상품 지원 (날짜 정보가 없어도 활성 데이터면 허용)
        if not end_date and not notice_date:
            return True
            
        if end_date and end_date < today:
            return False
        
        # v38: 공고일이 오래되었더라도 마감일이 없으면 '상시 모집'으로 간주하여 허용
        if not end_date and notice_date:
            # 180일 제한을 제거하여 모든 가용 공고 노출
            return True
            
        # 5. [v19] 무주택 및 소유 이력 필터링 (Hyper-Strict)
        # 공공임대 및 대부분의 청약은 무주택 필수
        if not instance.is_homeless:
            # 유주택자는 일반 민영 청약 외에는 배제
            if any(x in product.get('category', '') for x in ["국민", "임대", "공공"]):
                return False
        
        # 생애최초 전용 매물인데 소유 이력이 있는 경우 배제
        if "생애최초" in product.get('title', '') and not instance.is_first_home:
            return False

        # 6. [v44.2] 하이퍼 정밀 필터링 및 시너지 스코어링
        household_count = 1 if instance.marital_status == 'Single' and instance.kids_count == 0 else (instance.kids_count + (2 if "Married" in instance.marital_status else 1))
        criteria = MatchingEngine.get_income_criteria(household_count)
        income_monthly = (instance.total_income * 10000) / 12

        # [STRICT] 소득 120% 컷오프
        if any(x in title for x in ["행복주택", "임대", "공공"]):
            if income_monthly > criteria["monthly_120"]: return False

        # [STRICT] 나이/혼인/자녀 필터 (청년 만 34세 기준 강화)
        if "청년" in title and instance.age > 34: return False
        if any(x in title for x in ["신혼", "부부", "신생아"]) and instance.marital_status == 'Single' and not instance.is_pregnant: return False

        return True

    @staticmethod
    def calculate_housing_score(instance, product):
        """[v44.2] 주거 상품 정밀 점수 산정 로직"""
        score = 1000
        title = product.get('title', '')
        today = date.today()
        
        # 1. 마감 임박 가중치 (v44.2)
        end_date = product.get('end_date')
        if end_date:
            days_left = (end_date - today).days
            if 0 <= days_left <= 7: score += 200 # 7일 이내 마감 보너스
        
        # 2. 고가치 브랜드 가중치
        if "[LH]" in title or "[SH]" in title: score += 100
        if "[HUG]" in title: score += 150 # 주택도시보증공사 시너지
        if "[LH/사전]" in title: score += 250 # 사전청약 선점 가중치
        
        # 3. 상황별 시너지
        if instance.is_first_home and "생애최초" in title: score += 300
        if instance.is_pregnant and any(x in title for x in ["신생아", "출산"]): score += 500
        
        return score

    @staticmethod
    def analyze_housing(instance, finance_limit=0):
        """[STRICT] 주거: 현실성 검증 및 추천 로직"""
        try:
            region_map = {
                'Seoul': '서울', 'Gyeonggi': '경기', 'Incheon': '인천', 'Busan': '부산',
                'Daegu': '대구', 'Gwangju': '광주', 'Daejeon': '대전', 'Ulsan': '울산',
                'Sejong': '세종', 'Gangwon': '강원', 'Chungbuk': '충북', 'Chungnam': '충남',
                'Jeonbuk': '전북', 'Jeonnam': '전남', 'Gyeongbuk': '경북', 'Gyeongnam': '경남', 'Jeju': '제주'
            }
            reg_key = region_map.get(instance.region, '')
            
            today = date.today()
            # v33: 과거 데이터 전면 배제 (오늘 기준 마감되지 않았거나, 곧 공고될 예정인 것만 추출)

            # 1. 가용 자산 (Net Worth)
            net_worth = (instance.assets or 0) - (instance.debt or 0)
            
            # v31: 연봉 단위 정상화 (기존 뻥튀기 오류 6억 -> 5천만 수정)
            # 사용자 요청: "연봉으로 하자" -> total_income이 이미 연봉(5000)임
            annual_income = instance.total_income
            
            # 2. 대출 잠재력 (Finance Product 연동)
            # v30: 금융 섹션에서 가져온 실제 상품의 한도를 우선 적용
            # 소득이 5천만일 때 대출이 없는 경우, 현실적 한도(DSR 40%)로 약 3.5억 산출
            est_loan_cap = finance_limit if finance_limit > 0 else (annual_income * 7)
            
            # [Steel Wall v31] 총 구매력: (보유 자금) + (연봉) + (대출 한도)
            total_budget = net_worth + annual_income + est_loan_cap
            
            local_products = list(HousingProduct.objects.filter(
                Q(region__icontains=reg_key) | Q(region__icontains="전용") | Q(region__icontains="전국"),
                is_active=True
            ).filter(
                # v40.1: 마감일이 미래이거나, 마감일이 명시되지 않은 '상시 모집' 상품 전체 수용 (공고일 무관)
                Q(end_date__gte=today) | Q(notice_date__gte=today) | Q(end_date__isnull=True)
            ).order_by('notice_date')[:150])
            
            valid = []
            
            for p in local_products:
                s_price = p.sales_price
                
                # [v36 High-Precision Strict Filters]
                cat_str = (p.category or '').lower()
                title_str = (p.title or '').lower()
                
                # 1. 신혼부부 필터: 미혼자에게 신혼전용 노출 차단
                if instance.marital_status == 'Single' and any(x in title_str for x in ['신혼', '부부', '결혼']):
                    continue
                # 2. 청년 필터: 만 34세 초과자에게 청년전용 노출 차단
                if instance.age > 34 and any(x in title_str for x in ['청년', '대학생', '사회초년생']):
                    continue
                # 3. 자격/자산 필터: 자산 기준 초과 시 공공임대 배제 (SH/LH 기준 3.45억)
                if instance.assets > 34500 and any(x in title_str for x in ['국민임대', '행복주택', '영구임대']):
                    continue
                
                # [v35 Zero-Price Inclusion]
                if s_price > total_budget + 5000: continue 
                
                is_strictly_safe = (s_price <= total_budget)
                is_attractive_plus = (not is_strictly_safe and s_price <= total_budget + 5000)
                
                # [v35] 금액 포맷팅 (0원인 경우 상세 공고 참조로 변경)
                if s_price <= 0:
                    formatted_price = "상세 공고 참조"
                elif s_price >= 10000:
                    formatted_price = f"{s_price // 10000}억 {s_price % 10000:,}만" if s_price % 10000 > 0 else f"{s_price // 10000}억"
                else:
                    formatted_price = f"{s_price:,}만"

                tag = "모집중"
                if p.notice_date and p.notice_date > today: tag = "모집예정"
                
                # v34: 날짜 정보가 없으면 '상시 모집' 태그 부여
                if not p.notice_date and not p.end_date:
                    tag = "상시 모집"
                
                p_data = {
                    'title': p.title, 'category': p.category, 'org': p.org,
                    'sales_price': s_price, 'formatted_price': formatted_price,
                    'end_date': p.end_date, 'notice_date': p.notice_date, 
                    'region': p.region, # v38.3: 지역 검격기 연동용 필드 추가
                    'url': p.url or '#', 'score': 0, 'tag': tag
                }
                
                if MatchingEngine.is_eligible_housing(instance, p_data):
                    if instance.subscription_count >= 24: score += 150
                    
                    p_data['score'] = score
                    p_data['is_attractive_plus'] = is_attractive_plus
                    valid.append(p_data)
                    
            if not valid:
                return MatchingEngine.get_default_item("주거", f"현재 {reg_key or '전국'} 지역에 모집 중이거나 예정된 실시간 공고가 없습니다. (과거 데이터 제외됨)")

            valid.sort(key=lambda x: x['score'], reverse=True)
            top = valid[0]
            
            # [v33.1] 정직한 데이터 리포트 (Safe vs Attractive)
            # 금액 단위: 만원 -> 억 단위 변환 시 실수 계산 적용 (0.5억 등 표현)
            formatted_budget = f"{total_budget/10000:.1f}억" if total_budget < 10000 else f"{total_budget//10000}억 {total_budget%10000:,}만"
            
            if top.get('is_attractive_plus'):
                status_msg = f"⚠️ 예산 근소 초과({(top['sales_price'] - total_budget)//1000}만)"
                reason_main = f"수익성과 선호도가 워낙 높아 예산을 약간(5천만 이내) 초과하더라도 강력히 추천드리는 매물입니다."
            else:
                # v39: 대출 잠재력 정밀 동기화 (finance_limit이 있으면 그대로 사용, 없으면 현실적 추산 적용)
                # Dashboard의 "실질 가용 한도(Sim)"와 1원 단위까지 일치시킴
                loan_txt = f"{est_loan_cap/10000:.1f}억"
                breakdown = f"보유 {net_worth/10000:.1f}억 + 연봉 {annual_income/10000:.1f}억 + 대출 {loan_txt}"
                reason_main = f"사용자님의 {breakdown} 합산 범위 내 최적의 매물입니다."
                status_msg = "안정적 매칭" # v40.2: Crash Fix (UnboundLocalError 해결)
            
            household_count = 1 if instance.marital_status == 'Single' and instance.kids_count == 0 else (instance.kids_count + (2 if "Married" in instance.marital_status else 1))
            return { 
                "top_1": top, 
                "list": valid[1:11], 
                "reason": f"{reason_main} ({status_msg})",
                "criteria": MatchingEngine.get_income_criteria(household_count) # v42: 기준 정보 수신 기능
            }
        except Exception as e:
            import traceback
            traceback.print_exc()
            return MatchingEngine.get_default_item("주거")

    @staticmethod
    def analyze_finance(instance):
        """[STRICT] 금융: 소득 및 상황별 적격성 무한 대조"""
        try:
            # v35: 금융 데이터 가시성 확보 (날짜 필터 해제)
            local = FinanceProduct.objects.filter(is_active=True).filter(
                Q(end_date__gte=date.today()) | Q(end_date__isnull=True)
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
                
                # v40: 금융 상품 초정밀 적격성 차단 필터 (Hyper-Strict)
                # 미혼자에게 신혼부부 전용 배제
                if instance.marital_status == 'Single' and any(x in title for x in ["신혼", "부부"]): continue
                # 나이 초과 시 청년전용 배제 (금융은 대개 청년 기준 만 34세)
                if instance.age > 34 and "청년" in title: continue
                # 무자녀/비임신자에게 신생아/다자녀/임산부 특례 배제
                if instance.kids_count == 0 and not instance.is_pregnant and any(x in title for x in ["신생아", "다자녀", "자녀", "임신", "출산"]): continue
                
                # v36: 소득 기반 엄격 필터링 (연봉 단위 5000 vs 6000 비교)
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
                
                # v39.2: 금융 데이터 가시성 확보 점수 보정
                # 5000만원 연봉인 경우 '버팀목', '디딤돌' 등 서민금융 상품에 가점 부여
                if "버팀목" in title or "디딤돌" in title: 
                    score += 400
                
                # v39: 동기화 - 데이터가 없을 시 사용자 소득 기준 한도 정밀 추산 (연봉7배, 최대 5억 캡)
                # Dashboard 상의 "실질 가용 한도(Sim)"로 표시됨
                sim_limit = p.limit_amt // 10000 if p.limit_amt > 0 else min(instance.total_income * 7 // 10000, 50000 // 10000)
                real_limit = sim_limit * 10000 # 만원 단위
                
                valid.append({
                    'name': p.title,
                    'bank_nm': p.bank_nm,
                    'base_rate': rate,
                    'limit': real_limit,
                    'url': p.url or '#',
                    'score': score,
                    'tag': "마감임박" if score > 1000 else tag
                })
            
            if not valid: return MatchingEngine.get_default_item("금융")
            valid.sort(key=lambda x: x['score'], reverse=True)
            
            household_count = 1 if instance.marital_status == 'Single' and instance.kids_count == 0 else (instance.kids_count + (2 if "Married" in instance.marital_status else 1))
            return {
                "top_1": valid[0],
                "list": valid[1:6],
                "reason": f"사용자님의 {instance.get_marital_status_display()} 상태와 소득({instance.total_income}만원)에서 최저 금리가 예상되는 상품입니다.",
                "criteria": MatchingEngine.get_income_criteria(household_count) # v42: 기준 정보 수신
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
        # v39.5: 전국 17개 시도 매핑 및 NoneType 예외 처리
        region_map = {
            'Seoul': '서울', 'Gyeonggi': '경기', 'Incheon': '인천', 'Busan': '부산',
            'Daegu': '대구', 'Gwangju': '광주', 'Daejeon': '대전', 'Ulsan': '울산',
            'Sejong': '세종', 'Gangwon': '강원', 'Chungbuk': '충북', 'Chungnam': '충남',
            'Jeonbuk': '전북', 'Jeonnam': '전남', 'Gyeongbuk': '경북', 'Gyeongnam': '경남', 'Jeju': '제주'
        }
        reg_key = region_map.get(instance.region, '')
        p_region = policy.region or "" # v39.5: Null Safety
        if reg_key and (reg_key in p_region or "전국" in p_region):
            score += 500
            
        # 3. 상황적 키워드 매칭 (Married_1, Married_2 지원)
        marital = instance.marital_status
        is_parent = (instance.kids_count > 0 or instance.is_pregnant)
        
        # 긍정 매칭 (v39: Married_1/2 동시 지원)
        if marital == 'Single' and any(x in target for x in ["미혼", "1인", "독신"]): score += 200
        if marital.startswith('Married') or marital == 'Engaged':
             if any(x in target for x in ["신혼", "부부", "혼인"]): score += 400
        if is_parent and any(x in target for x in ["자녀", "출산", "임신", "양육"]): score += 500
        
        # v42: 중위소득 정밀 필터 (Welfare PCM)
        household_count = 1 if marital == 'Single' and not is_parent else (instance.kids_count + (2 if marital.startswith('Married') else 1))
        criteria = MatchingEngine.get_income_criteria(household_count)
        income_monthly = (instance.total_income * 10000) / 12
        
        # 저소득/차상위 키워드 있을 시 100% 초과자 칼같이 탈락
        if any(x in target for x in ["저소득", "중위소득 100", "중위 100"]):
            if income_monthly > criteria["monthly_100"]: return -1
        if any(x in target for x in ["중위소득 120", "중위 120"]):
            if income_monthly > criteria["monthly_120"]: return -1

        # v44.2: 마감/상황 시너지 (Welfare Advanced)
        today = date.today()
        if policy.end_date:
            days_left = (policy.end_date - today).days
            if 0 <= days_left <= 7: score += 200
            
        if "[서울]" in title and reg_key == '서울': score += 300
        if "수당" in title and income_monthly < criteria["monthly_100"]: score += 150 # 저소득 타겟팅
        if instance.debt > 2000 and "이자" in title: score += 200 # 부채 보유자 금리 지원 우대

        # v40: 부정 매칭 (자격이 안 될 경우 100% 강제 탈락)
        if marital != 'Single' and any(x in target for x in ["미혼", "1인 가구", "독신"]): return -1
        if marital == 'Single' and any(x in target for x in ["신혼", "부부", "혼인"]): return -1
        if not is_parent and any(x in target for x in ["다자녀", "자녀", "신생아", "임신", "출산"]): return -1
        
        return score

    @staticmethod
    def analyze_welfare(instance):
        """[STRICT] 복지: 스코어링 시스템 기반 최적 정책 선별"""
        try:
            # v39.5: 전국 17개 시도 매핑
            region_map = {
                'Seoul': '서울', 'Gyeonggi': '경기', 'Incheon': '인천', 'Busan': '부산',
                'Daegu': '대구', 'Gwangju': '광주', 'Daejeon': '대전', 'Ulsan': '울산',
                'Sejong': '세종', 'Gangwon': '강원', 'Chungbuk': '충북', 'Chungnam': '충남',
                'Jeonbuk': '전북', 'Jeonnam': '전남', 'Gyeongbuk': '경북', 'Gyeongnam': '경남', 'Jeju': '제주'
            }
            reg_key = region_map.get(instance.region, '')
            
            # 사용자 지역 혹은 전국 정책 통합 검색 (기타 지역일 경우 전국 공고만)
            query = Q(region__icontains="전국") | Q(region__isnull=True)
            if reg_key:
                query |= Q(region__icontains=reg_key)
            
            # v35: 복지 정책 가시성 확보 (날짜 필터 해제)
            local = WelfareProduct.objects.filter(query, is_active=True).filter(
                Q(end_date__gte=date.today()) | Q(end_date__isnull=True)
            )
            
            valid = []
            
            for p in local:
                # [STRICT] 모집 기간 필터링
                today = date.today()
                
                # [v22] 상태 태그 부여
                tag = "모집중"
                if p.notice_date and p.notice_date > today: tag = "모집예정"

                score = MatchingEngine.calculate_welfare_score(instance, p)
                # v39: 복지 가시성 확보 (threshold 하향: 소량의 양수면 노출)
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
            
            household_count = 1 if instance.marital_status == 'Single' and not (instance.kids_count > 0 or instance.is_pregnant) else (instance.kids_count + (2 if instance.marital_status.startswith('Married') else 1))
            return { 
                "top_1": valid[0], 
                "list": valid[1:11], 
                "reason": f"회원님의 생애주기({instance.get_marital_status_display()})와 연령에 가장 특화된 혜택을 1순위에 배치했습니다.",
                "criteria": MatchingEngine.get_income_criteria(household_count) # v42: 기준 정보 수신
            }
        except Exception:
            return MatchingEngine.get_default_item("복지")

    @classmethod
    def get_full_report(cls, instance):
        """안티그래비티 전문가 보고서 통합 출력 (v30 Credit-Linked)"""
        # 1. 금융 상품을 먼저 분석하여 '최대 대출 한도' 확보
        fin_report = cls.analyze_finance(instance)
        
        top_loan_limit = 0
        if fin_report.get('top_1') and not fin_report['top_1'].get('is_default'):
            # 상품 데이터가 있으면 만원 단위 한도 사용
            try:
                top_loan_limit = int(fin_report['top_1'].get('limit', 0))
            except (ValueError, TypeError):
                top_loan_limit = 0
            
        # 2. 확보된 대출 한도를 주거 매칭에 주입 (타입 안정성 확보)
        housing_report = cls.analyze_housing(instance, finance_limit=top_loan_limit)
        
        sim = cls.calculate_simulation(instance)
        return {
            "housing": housing_report,
            "finance": fin_report,
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
