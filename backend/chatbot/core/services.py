import os
import requests
import random
import google.generativeai as genai
from dotenv import load_dotenv
from .models import Policy

# API 설정 (LH, 복지로 등)
HOUSING_KEY = os.environ.get('VITE_HOUSING_API_KEY', '')
WELFARE_KEY = os.environ.get('VITE_WELFARE_API_KEY', '')
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")

if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)

# --- 정부 오픈 API 기반 실시간 데이터 수집 ---
LH_BASE_URL = 'https://apis.data.go.kr/B552555/lhLeaseNoticeInfo1'
BOKJIRO_BASE_URL = 'https://apis.data.go.kr/B554287/NationalWelfareInformations'

FALLBACK_POLICIES = [
    {
        'id': 'fin-1', 'category': 'Finance', 'title': '신생아 특례 디딤돌 대출',
        'summary': '최저 1.6% 금리로 최대 5억원 지원', 'tags': ['출산가구', '저금리', '최대5억'],
        'ageMax': 50, 'incomeLimit': 13000, 'requiresKids': True
    },
    {
        'id': 'fin-2', 'category': 'Finance', 'title': '청년 버팀목 전세자금대출',
        'summary': '사회초년생을 위한 연 1.8~2.7% 저금리 전세 대출', 'tags': ['청년전용', '전세자금', '저금리'],
        'ageMax': 34, 'incomeLimit': 5000, 'maritalStatus': ['single']
    },
    {
        'id': 'fin-3', 'category': 'Finance', 'title': '신혼부부 전용 버팀목 대출',
        'summary': '신혼부부 합산 연소득 7.5천만원 이하 대상', 'tags': ['신혼부부', '전세자금', '우대금리'],
        'ageMax': 45, 'incomeLimit': 7500, 'maritalStatus': ['newly', 'expecting']
    }
]

def calculate_score(user_data, policy):
    """
    초정밀 적합도 연산 엔진. 나이, 소득, 가구, 고용 상태 등을 종합하여 
    0~100점 사이의 전문가 적합 지수를 산출합니다.
    """
    if not user_data: return 0
    score = 75.0 # 기본 전문가 기준점
    
    age = int(user_data.get('age') or 25)
    income = int(user_data.get('income') or 3000)
    region = user_data.get('region') or 'seoul'
    marital = user_data.get('marital') or 'single'
    
    # 1. 연령 적합도 (청년 특화 가점)
    p_age_max = policy.get('ageMax') or 100
    if age > p_age_max: score -= 35.5
    elif 19 <= age <= 34: score += 10.5 # 청년층 특별 가점
    
    # 2. 소득 적합도 (역진적 가중치)
    p_income_limit = policy.get('incomeLimit') or 99999
    if income > p_income_limit: score -= 45.0
    elif income < 2400: score += 15.0 # 저소득층 우대 정책
    
    # 3. 카테고리별 특화 가점
    p_cat = policy.get('category', 'Finance')
    if p_cat == 'Employment' and user_data.get('isUnemployed'): score += 20.0
    if p_cat == 'Legal' and user_data.get('needsCounsel'): score += 25.0
    if p_cat == 'Youth' and age <= 24: score += 15.0
    
    # 4. 혼인 및 거주지 (지역 가점)
    if policy.get('maritalStatus') and marital not in policy['maritalStatus']:
        score -= 20.0
        
    return max(0.0, min(100.0, score))

def fetch_housing_policies():
    try:
        url = f"{LH_BASE_URL}/getLeaseNoticeInfo?serviceKey={HOUSING_KEY}&numOfRows=10&pageNo=1&_type=json"
        res = requests.get(url, timeout=5)
        res.raise_for_status()
        data = res.json()
        items = data.get('response', {}).get('body', {}).get('items', [])
        return [{
            'id': f"HOU_{i.get('pblancId')}", 'category': 'Housing', 'title': i.get('pblancNm') or '주거공고',
            'summary': i.get('insttNm'), 'ageMax': 39, 'incomeLimit': 6000
        } for i in items]
    except: return []

def fetch_welfare_policies():
    try:
        url = f"{BOKJIRO_BASE_URL}/getNationalWelfareInformations?serviceKey={WELFARE_KEY}&numOfRows=10&pageNo=1&_type=json"
        res = requests.get(url, timeout=5)
        data = res.json()
        items = data.get('response', {}).get('body', {}).get('items', [])
        return [{
            'id': f"WEL_{i.get('servId')}", 'category': 'Welfare', 'title': i.get('servNm'),
            'summary': i.get('jurMnstNm'), 'ageMax': 60, 'incomeLimit': 4000
        } for i in items]
    except: return []

def get_all_policies(user_data=None):
    """DB와 실시간 API 데이터를 결합하여 정밀 점수를 매긴 리스트 반환"""
    db_policies = list(Policy.objects.all().values())
    formatted_db = [{
        'id': f"DB_{p['id']}", 'category': p['category'], 'title': p['title'],
        'summary': p['summary'], 'ageMax': p['age_max'], 'incomeLimit': p['income_limit'],
        'maritalStatus': p['marital_status'] or [], 'url': f"/policy/{p['id']}"
    } for p in db_policies]
    
    housing = fetch_housing_policies()
    welfare = fetch_welfare_policies()
    
    combined = formatted_db + housing + welfare
    if not combined: combined = FALLBACK_POLICIES
    
    for p in combined:
        p['score'] = calculate_score(user_data, p)
        
    return sorted(combined, key=lambda x: x['score'], reverse=True)

def ask_expert_ai(user_message, user_data=None, report_data=None):
    """전문가형 AI 챗봇 로직: 고용, 법률 등 도메인별 최적 답변 제공"""
    name = user_data.get('name') or '방문자'
    top_policies = user_data.get('top_matches', [])[:3]
    
    # 🧭 v15 리포트에서 DSR/PIR 핵심 데이터 추출
    sim_data = report_data.get('financial_simulation', {}) if report_data else {}
    housing_report = report_data.get('housing', {}) if report_data else {}
    
    def expert_fallback(msg, data):
        msg_l = msg.lower()
        
        # 1. 인사 및 정서적 교감 테마
        openers = [
            f"반갑습니다 {name}님, 정책 전문가로서 **[{msg}]**에 대해 면밀히 분석해 드릴게요. 🧐",
            f"안녕하세요 {name}님. 현재 상황에서 가장 이득이 되는 선택지를 선별 중입니다. ✨",
            f"{name}님만을 위한 전용 가이드입니다. 데이터 기반의 인사이트를 도출했습니다. 🚀",
            f"청년 정책의 모든 것, {name}님께 꼭 필요한 정보만 골라왔어요! 🎁"
        ]
        
        # 2. 메타 질문 처리 (반복 문의, 정체성 등)
        if any(x in msg_l for x in ['똑깥', '똑같', '반복', '이유', '누구', '뭐야', '에러']):
            reason = "현재 AI 가동을 위한 **API 키가 설정되지 않았거나 쿼터가 초과**되어 기본 모드로 동작 중입니다. 😅"
            if not GOOGLE_API_KEY:
                return f"{reason}\n\n하지만 걱정 마세요! 제가 {name}님의 프로필을 바탕으로 계산한 **최적의 정책 리스트**는 변함없이 정확하답니다. 키를 `.env`에 등록하시면 더 다양한 대화가 가능해져요!"
            return "앗, 현재 실시간 분석 엔진이 혼잡하여 로컬 분석 데이터로 전환되었습니다. 더 구체적인 상황을 말씀해 주시면 정해진 규칙 내에서 최선을 다해 안내해 드릴게요!"

        # 3. 카테고리별 다이내믹 답변
        if any(x in msg_l for x in ['취업', '고용', '일자리', '면접', 'job']):
            res = f"{random.choice(openers)}\n\n현재 청년 일자리 도약 장려금이나 국민취업지원제도가 {name}님께 가장 적합한 옵션으로 보이네요. 혹시 특정 직무나 지역이 궁금하신가요?"
        elif any(x in msg_l for x in ['법률', '상담', '변호사', '소송', 'legal']):
            res = f"{random.choice(openers)}\n\n대한법률구조공단의 청년 무료 상담 서비스가 1순위로 고려됩니다. 특히 임대차 계약이나 근로 권익 관련 상담이 매우 활발하니 꼭 확인해 보세요!"
        elif any(x in msg_l for x in ['금융', '대출', '돈', '이자', '자금']):
            res = f"{random.choice(openers)}\n\n현재 저금리 대출 상품들이 {name}님의 소득 구간에서 아주 매력적입니다. 특히 '버팀목 전세자금대출'의 금리 우대 혜택을 놓치지 마세요."
        else:
            # 기본 추천 (다양성 강화 버전)
            res = f"{random.choice(openers)}\n\n데이터 엔진이 분석한 {name}님 맞춤형 **TOP 혜택**입니다.\n\n"
            if top_policies:
                for i, p in enumerate(top_policies, 1):
                    res += f"{i}. **{p.get('title')}** (적합도 {p.get('score', 0):.1f}%)\n"
                res += f"\n수치 기반으로 도출된 결과입니다. 이 중 더 자세한 신청 방법이 궁금한 정책이 있으신가요?"
            else:
                res += "앗, 현재 적절한 정책을 찾는 중이에요. 정밀 진단 폼을 먼저 작성해 주시면 더 정확한 안내가 가능합니다! 🧭"
        return res

    try:
        # 동적 키 로드 및 설정 (서버 재시작 없이 반영)
        load_dotenv(override=True)
        current_key = os.environ.get("GOOGLE_API_KEY", "")
        if not current_key: raise Exception("Key Missing")
        
        genai.configure(api_key=current_key)
        
        # 🎯 하이브리드 모델 전략 (1.5 Flash 시도 -> Pro Fallback)
        try:
            model = genai.GenerativeModel('gemini-1.5-flash-latest')
            prompt = f"""당신은 청년 주거/금융 전문가 '안티그래비티 AI'입니다. 다음 데이터를 근거로 조언하세요: 사용자 {name}, DSR 한도 {sim_data.get('max_limit')}만원, PIR 점검 {housing_report.get('reason')}. 질문: '{user_message}'. 지침: 반드시 수치를 언급하며 전문가답게 답변하세요."""
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as api_err:
            print(f"Primary AI API Issue (gemini-1.5-flash-latest): {api_err}")
            # 2차 시도 (legacy pro)
            model = genai.GenerativeModel('gemini-pro')
            response = model.generate_content(prompt)
            return response.text.strip()
            
    except Exception as e:
        print(f"AI ERROR (Final Fallback to Local Engine): {str(e)}")
        return expert_fallback(user_message, user_data)

def generate_expert_report(user_data, top_matches):
    """정밀 진단 후 제공하는 전문가용 보고서 생성"""
    if not GOOGLE_API_KEY:
        report = f"### [전문 가이드] {user_data.get('name', '사용자')}님 맞춤형 정책 제언\n\n"
        for p in top_matches[:5]:
            report += f"- **{p.get('title')}** (적합 지수 {p.get('score', 0):.1f}%)\n"
        report += "\n> 본 보고서는 사용자님의 현재 정보(나이, 소득)를 바탕으로 데이터 엔진이 자동 작성하였습니다."
        return report
    
    try:
        # 🎯 최신 모델 우선 시도
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        prompt = f"다음 데이터를 바탕으로 청년 눈높이의 정책 보고서를 작성하세요: {user_data}, 추천목록: {top_matches[:5]}"
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"Report AI Issue: {e}")
        return "현재 분석 서버 실시간 쿼터가 초과되었습니다. 우선순위 요약 리스트를 확인해 주세요."
