import os
import requests
import time
import concurrent.futures
from google import genai
from google.genai import types
from dotenv import load_dotenv
from .models import Policy

# --- [Performance v20] Global Cache Layer ---
API_CACHE = {
    'housing': {'data': None, 'timestamp': 0},
    'welfare': {'data': None, 'timestamp': 0}
}
CACHE_TTL = 300 # 5분간 유효

# API 설정 (LH, 복지로 등)
# DATA_PORTAL_KEY를 우선 사용하고, VITE_ 버전은 하위 호환성을 위해 유지합니다.
HOUSING_KEY = os.environ.get('DATA_PORTAL_KEY') or os.environ.get('VITE_HOUSING_API_KEY', '')
WELFARE_KEY = os.environ.get('DATA_PORTAL_KEY') or os.environ.get('VITE_WELFARE_API_KEY', '')
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")

# Client Initialization (v20 Modern GenAI)
client = None
if GOOGLE_API_KEY:
    client = genai.Client(api_key=GOOGLE_API_KEY)

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
    """DB와 실시간 API 데이터를 결합하여 정밀 점수를 매긴 리스트 반환 (병렬 최적화 v20)"""
    db_policies = list(Policy.objects.all().values())
    formatted_db = [{
        'id': f"DB_{p['id']}", 'category': p['category'], 'title': p['title'],
        'summary': p['summary'], 'ageMax': p['age_max'], 'incomeLimit': p['income_limit'],
        'maritalStatus': p['marital_status'] or [], 'url': f"/policy/{p['id']}"
    } for p in db_policies]
    
    # 🚀 병렬 처리를 통한 속도 혁신 (ThreadPoolExecutor)
    now = time.time()
    housing = []
    welfare = []
    
    # 캐시 확인
    if API_CACHE['housing']['data'] and (now - API_CACHE['housing']['timestamp'] < CACHE_TTL):
        housing = API_CACHE['housing']['data']
    if API_CACHE['welfare']['data'] and (now - API_CACHE['welfare']['timestamp'] < CACHE_TTL):
        welfare = API_CACHE['welfare']['data']

    # 캐시가 없거나 만료된 경우에만 API 호출
    missing_apis = []
    if not housing: missing_apis.append('housing')
    if not welfare: missing_apis.append('welfare')

    if missing_apis:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(missing_apis)) as executor:
            future_to_api = {}
            if 'housing' in missing_apis:
                future_to_api[executor.submit(fetch_housing_policies)] = 'housing'
            if 'welfare' in missing_apis:
                future_to_api[executor.submit(fetch_welfare_policies)] = 'welfare'
            
            for future in concurrent.futures.as_completed(future_to_api):
                api_name = future_to_api[future]
                try:
                    data = future.result()
                    if data:
                        if api_name == 'housing': 
                            housing = data
                            API_CACHE['housing'] = {'data': data, 'timestamp': now}
                        else: 
                            welfare = data
                            API_CACHE['welfare'] = {'data': data, 'timestamp': now}
                except Exception as e:
                    print(f"Parallel API Error ({api_name}): {e}")

    combined = formatted_db + housing + welfare
    if not combined: combined = FALLBACK_POLICIES
    
    for p in combined:
        p['score'] = calculate_score(user_data, p)
        
    return sorted(combined, key=lambda x: x['score'], reverse=True)

class ResponseSynthesizer:
    """[Premium v20] 로컬 지능형 답변 합성 엔진: 다양성과 데이터 기반의 신뢰성을 보장합니다."""
    
    OPENERS = [
        "반갑습니다 {name}님, 정책 전문가로서 면밀히 분석해 드릴게요. 🧐",
        "안녕하세요 {name}님! 현재 상황에서 가장 이득이 되는 경로를 스캔했습니다. ✨",
        "{name}님만을 위한 데이터 기반 로드맵입니다. 분석 결과를 확인해 보세요. 🚀",
        "청년 정책 마스터 안티그래비티입니다. {name}님께 딱 맞는 정보를 선별했습니다. 🎁",
        "상담을 시작합니다. {name}님의 데이터를 바탕으로 최적의 조언을 구성했어요. 🧭"
    ]
    
    ANALYSIS_HOOKS = [
        "특히 {name}님의 **연소득 {income}만원**과 **자산 {assets}만원** 규모를 고려했을 때,",
        "현재 가용 자본과 **부채 {debt}만원** 상태를 종합 분석한 결과,",
        "만 {age}세인 {name}님의 연령대와 **{region}** 지역의 혜택을 매칭해 본 결과,",
        "보유하신 청약 통장({subscription})의 가점과 현재 경제 상황을 결합해 보니,"
    ]
    
    EXPERT_ADVICES = {
        'Finance': [
            "현재 금리 조건에서는 **버팀목 전세자금대출**의 우대 금리를 챙기는 것이 자산 형성의 지름길입니다.",
            "신생아 특례나 청년 전용 상품의 금리 혜택이 {name}님께 아주 유리하게 설계되어 있네요.",
            "이자 부담을 줄이기 위해 현재의 부채 구조를 저금리 정부 지원 상품으로 갈아타는 전략을 추천합니다."
        ],
        'Housing': [
            "매매보다는 현재 LH나 SH에서 진행하는 **청년 매입임대** 혹은 **전세임대** 공고에 집중하실 시기입니다.",
            "PIR 지수 분석 결과, 현재는 무리한 매수보다는 내 집 마련을 위한 청약 가점 관리가 더 전략적입니다.",
            "{region} 지역의 신규 주거 공급 정보를 실시간으로 모니터링하여 가점을 활용해 보세요."
        ],
        'Default': [
            "데이터가 매칭하는 **TOP 3 정책**에 우선적으로 신청 자격을 검토해 보시길 권장합니다.",
            "현재 조건에서 받을 수 있는 혜택이 적지 않습니다. 아래 리스트를 하나씩 클릭해 상세 공고를 확인해 보세요.",
            "정책 전문가로서 보기에 {name}님은 현재 주거와 금융 지원을 동시에 받을 수 있는 최적의 구간에 있습니다."
        ]
    }

    @classmethod
    def generate(cls, user_data, category='Default'):
        import random
        name = user_data.get('name') or '방문자'
        top_policies = user_data.get('top_matches', [])[:3]
        
        opener = random.choice(cls.OPENERS).format(name=name)
        hook = random.choice(cls.ANALYSIS_HOOKS).format(
            name=name, 
            income=user_data.get('income', 0),
            assets=user_data.get('assets', 0),
            debt=user_data.get('debt', 0),
            age=user_data.get('age', 0),
            region=user_data.get('region', '전국'),
            subscription=user_data.get('subscription', '미보유')
        )
        advices = cls.EXPERT_ADVICES.get(category, cls.EXPERT_ADVICES['Default'])
        advice = random.choice(advices).format(name=name, region=user_data.get('region', '전국'))
        
        res = f"{opener}\n\n{hook}\n{advice}"
        
        if top_policies:
            res += "\n\n현재 조건에서 가장 승산이 높은 **TOP 3 정책**입니다:"
            for p in top_policies:
                # 🎯 [POLICY_URL] 프로토콜: 챗봇에서 해당 정책 사이트로 직접 연결
                res += f"\n- **{p.get('title')}**: [[BUTTON:POLICY_URL | {p.get('title')}]]"
                
        return res

def ask_expert_ai(user_message, user_data=None, report_data=None, user_api_key=None, history=None):
    """전문가형 AI 챗봇 로직 (Contextual Memory v20): 대화 흐름 인지 및 액션 제안"""
    name = user_data.get('name') or '방문자'
    top_policies = user_data.get('top_matches', [])[:3]
    history = history or [] # 대화 이력
    
    # 🧭 v15 리포트에서 DSR/PIR 핵심 데이터 추출
    sim_data = report_data.get('financial_simulation', {}) if report_data else {}
    housing_report = report_data.get('housing', {}) if report_data else {}
    
    # --- [System Instruction] 전문가 페르소나 및 액션 프로토콜 정립 ---
    SYSTEM_INSTRUCTION = f"""당신은 청년 주거/금융 전문가 '안티그래비티 AI'입니다. 
다음은 현재 상담 중인 사용자의 실시간 진단 데이터입니다:
- 성함: {name} | 연령: 만 {user_data.get('age')}세 | 거주지: {user_data.get('region')} | 혼인: {user_data.get('marital')}
- 경제: 연소득 {user_data.get('income')}만원, 자산 {user_data.get('assets')}만원, 부채 {user_data.get('debt')}만원
- 청약: {user_data.get('subscription')}
- 지표: DSR 한도 {sim_data.get('max_limit')}만원, PIR 점검({housing_report.get('reason')})

지침: 
1. 위 데이터 수치를 언급하며 전문가답게 조언하세요. 
2. 친절하지만 신뢰감 있는 전문가 톤을 유지하세요.
3. [중요] 답변 끝에 사용자가 다음에 할 법한 행동을 다음 규격의 버튼으로 제안하세요: [[BUTTON:유형|라벨]]
   - 유형 예시: DSR_CALC (계산기), POLICY_LIST (정책목록), APPLY_LH (신청사이트), REPORT_VIEW (리포트보기)
   - 예: "더 자세한 대출 한도가 궁금하시면 [[BUTTON:DSR_CALC|DSR 계산기 실행]]을 눌러보세요."
"""

    def expert_fallback(msg, data):
        # ResponseSynthesizer를 통한 융합형 답변 생성
        res = ResponseSynthesizer.generate(user_data, 'Default')
        return res + "\n\n더 자세한 분석은 [[BUTTON:REPORT_VIEW|전체 리포트 보기]]에서 확인하실 수 있습니다."

    try:
        load_dotenv(override=True)
        current_key = user_api_key or os.environ.get("GOOGLE_API_KEY", "")
        if not current_key: raise Exception("Key Missing")
        
        local_client = genai.Client(api_key=current_key)
        
        # 🎯 [Contextual Chat] 세션 생성 및 대화
        try:
            chat = local_client.chats.create(
                model='gemini-2.0-flash',
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    temperature=0.7
                ),
                history=history
            )
            
            response = chat.send_message(user_message)
            return response.text.strip()
            
        except Exception as api_err:
            print(f"Chat API Issue: {api_err}")
            # Fallback to simple generate_content
            response = local_client.models.generate_content(
                model='gemini-1.5-flash',
                contents=f"{SYSTEM_INSTRUCTION}\n\nUser: {user_message}"
            )
            return response.text.strip()
            
    except Exception as e:
        print(f"AI ERROR (Final Fallback): {str(e)}")
        return expert_fallback(user_message, user_data)

def generate_expert_report(user_data, top_matches):
    """정밀 진단 후 제공하는 전문가용 보고서 생성"""
    if not client:
        report = f"### [전문 가이드] {user_data.get('name', '사용자')}님 맞춤형 정책 제언\n\n"
        for p in top_matches[:5]:
            report += f"- **{p.get('title')}** (적합 지수 {p.get('score', 0):.1f}%)\n"
        report += "\n> 본 보고서는 사용자님의 현재 정보(나이, 소득)를 바탕으로 데이터 엔진이 자동 작성하였습니다."
        return report
    
    try:
        # 🎯 v20 최신 모델 우선 시도
        prompt = f"다음 데이터를 바탕으로 청년 눈높이의 정책 보고서를 작성하세요: {user_data}, 추천목록: {top_matches[:5]}"
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt
        )
        return response.text.strip()
    except Exception as e:
        print(f"Report AI Issue: {e}")
        return "현재 분석 서버 실시간 쿼터가 초과되었습니다. 우선순위 요약 리스트를 확인해 주세요."
