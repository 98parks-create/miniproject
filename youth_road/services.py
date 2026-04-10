import requests
import environ
import urllib.parse
import xml.etree.ElementTree as ET
import os
from .firebase_service import FirebaseManager
from django.conf import settings

# Initialize environ
env = environ.Env()
env.read_env(os.path.join(settings.BASE_DIR, '.env'))

class RegionMapper:
    """각 기관별 Open API에 최적화된 지역 코드 매핑 시스템"""
    
    # LH (한국토지주택공사) 지역 코드 (CNP_CD)
    LH_MAP = {
        'Seoul': '11', 'Gyeonggi': '41', 'Incheon': '28', 
        'Busan': '26', 'Daegu': '27', 'Gwangju': '29', 
        'Daejeon': '30', 'Ulsan': '31', 'Sejong': '36', 'Other': ''
    }

    # 온라인청년센터(온통청년) 지역 코드 (srchPolyBizSecd)
    YOUTH_CENTER_MAP = {
        'Seoul': '003002001', 'Busan': '003002002', 'Daegu': '003002003',
        'Incheon': '003002004', 'Gwangju': '003002005', 'Daejeon': '003002006',
        'Ulsan': '003002007', 'Sejong': '003002008', 'Gyeonggi': '003002009',
        'Other': '003002' # 전국 단위
    }

    @classmethod
    def get_lh_code(cls, region_name):
        return cls.LH_MAP.get(region_name, '')

    @classmethod
    def get_youth_center_code(cls, region_name):
        return cls.YOUTH_CENTER_MAP.get(region_name, '003002')

class PublicDataHousingService:
    """1단계: LH 및 SH 실시간 임대공고 연동 엔진"""
    
    @staticmethod
    def get_lh_sh_notices(region_name, type_code='05'):
        # 1. Firebase Archive 먼저 로드 (수동 데이터 포함)
        items = FirebaseManager.fetch_archive('housing_notices', region_name)
        
        # 2. API 호출 시도
        raw_key = env('DATA_PORTAL_KEY', default='').strip()
        if not raw_key:
            return items

        # 서비스 키 인코딩 이슈 해결 (unquote 후 사용)
        decoded_key = urllib.parse.unquote(raw_key)
        
        # 🎯 LH 공고 API (최신 API 엔드포인트로 보정)
        base_url = "http://apis.data.go.kr/B552555/lhLeaseNotice1/lhLeaseNotice1/getLeaseNoticeInfo1"
        # 🧭 예비 엔드포인트 (MyHome)
        alt_url = "http://apis.data.go.kr/1613000/HWSPR02/rsdtRcritNtcList"
        
        full_url = f"{base_url}?serviceKey={decoded_key}&PG_SZ=100&PAGE=1&CNP_CD={RegionMapper.get_lh_code(region_name)}&UPP_AIS_TP_CD={type_code}"
        
        try:
            # 타임아웃 10초, User-Agent 추가 (차단 방지)
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(full_url, headers=headers, timeout=10)
            
            if response.status_code != 200 or "SERVICE_KEY_IS_NOT_REGISTERED" in response.text:
                print(f"LH API Primary Fail ({response.status_code}), trying Alternative...")
                # MyHome API로 폴백
                response = requests.get(f"{alt_url}?serviceKey={decoded_key}&numOfRows=10&pageNo=1", headers=headers, timeout=10)
            
            if response.status_code == 200:
                root = ET.fromstring(response.text)
                api_items = []
                for item_xml in root.findall('.//item'):
                    # ... (기존 파싱 로직 유지)
                    notice_id = f"LH_{item_xml.findtext('PAN_ID', '') or item_xml.findtext('AIS_TP_CD_NM', '')}_{item_xml.findtext('PAN_NT_DT', '')}"
                    api_items.append({
                        "id": notice_id,
                        "org": "LH",
                        "title": item_xml.findtext('PAN_NM', 'LH 주거 공고'),
                        "region": item_xml.findtext('CNP_CD_NM', region_name),
                        "type": item_xml.findtext('UPP_AIS_TP_NM', 'Rental'),
                        "schedule": f"접수: {item_xml.findtext('RCEPT_BGNDE', '-')} ~ {item_xml.findtext('RCEPT_ENDDE', '-')}",
                        "url": "https://apply.lh.or.kr/",
                        "raw_data": {child.tag: child.text for child in item_xml}
                    })
                
                if api_items:
                    FirebaseManager.sync_data('housing_notices', api_items, id_field='id')
                    existing_ids = {i.get('id') for i in items}
                    items += [ai for ai in api_items if ai.get('id') not in existing_ids]

        except Exception as e:
            print(f"LH API Comprehensive Error: {e}")
        
        return items

class SubscriptionHomeService:
    """1.5단계: 청약홈(한국부동산원) 실시간 공고 연동"""
    
    @staticmethod
    def get_subscription_notices(region_name):
        # 1. Firebase Archive 먼저 로드
        items = FirebaseManager.fetch_archive('housing_notices', region_name)
        
        # 2. API 호출 시도
        raw_key = env('DATA_PORTAL_KEY', default='').strip()
        if not raw_key:
            return items
        
        decoded_key = urllib.parse.unquote(raw_key)
        url = f"https://apis.data.go.kr/1613000/ApplyHomeInfoService/getLttotPblancList?serviceKey={decoded_key}&numOfRows=50&pageNo=1"
        
        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                if "SERVICE_KEY_IS_NOT_REGISTERED" in response.text:
                    return items
                    
                root = ET.fromstring(response.text)
                api_items = []
                for it in (root.findall('.//item') or root.findall('.//row')):
                    item_region = it.findtext('SUBSCRPT_AREA_CODE_NM', '')
                    if region_name in item_region or not region_name:
                        notice_id = f"SUB_{it.findtext('PBLANC_NO', '00')}"
                        api_items.append({
                            "id": notice_id,
                            "org": "ApplyHome",
                            "title": it.findtext('HOUSE_NM', '청약홈 분양 공고'),
                            "region": item_region,
                            "type": it.findtext('HOUSE_SECD_NM', 'Sale'),
                            "schedule": f"모집공고일: {it.findtext('RCEPT_BGNDE', '-')}",
                            "url": "https://www.applyhome.co.kr/",
                            "raw_data": {child.tag: child.text for child in it}
                        })
                
                if api_items:
                    FirebaseManager.sync_data('housing_notices', api_items, id_field='id')
                    existing_ids = {i.get('id') for i in items}
                    items += [ai for ai in api_items if ai.get('id') not in existing_ids]

        except Exception as e:
            print(f"ApplyHome API Error: {e}")
        
        return items

class FssFinanceService:
    """2단계: 금융감독원(금감원) 금융상품 비교공시 연동"""
    
    @staticmethod
    def get_loan_products(income, marital_status):
        # 1. 고정 데이터 + Firebase 데이터 로드
        policy_loans = [
            {"id": "P_01", "name": "신생아 특례 대출", "base_rate": 1.2, "target": "Kids", "limit": 50000, "org": "정부지원", "url": "https://nhuf.molit.go.kr/"},
            {"id": "P_02", "name": "디딤돌 대출(내집마련)", "base_rate": 2.15, "target": "FirstHome", "limit": 40000, "org": "정부지원", "url": "https://nhuf.molit.go.kr/"},
            {"id": "P_03", "name": "버팀목 전세자금", "base_rate": 1.8, "target": "Rent", "limit": 20000, "org": "정부지원", "url": "https://nhuf.molit.go.kr/"},
            {"id": "P_04", "name": "청년 전용 보증부월세", "base_rate": 1.0, "target": "LowIncome", "limit": 5000, "org": "정부지원", "url": "https://nhuf.molit.go.kr/"}
        ]
        items = policy_loans + FirebaseManager.fetch_archive('loan_products')
        
        # 2. API 호출 시도
        api_key = env('FSS_FINANCE_KEY', default='').strip()
        if not api_key:
            return items

        # 🎯 FSS 금융상품 API (검증된 최신 엔드포인트 적용)
        url = "http://finlife.fss.or.kr/finlifeapi/rentHouseLoanProductsSearch.json"
        params = {'auth': api_key, 'topFinGrpNo': '020000', 'pageNo': '1'}
        
        try:
            # 타임아웃 10초, User-Agent 추가
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 200 and "baseList" in res.text:
                data = res.json()
                api_loans = []
                for l in data.get('result', {}).get('baseList', []):
                    api_loans.append({
                        "id": l.get('fin_prdt_cd'),
                        "org": l.get('kor_co_nm'),
                        "name": l.get('fin_prdt_nm'),
                        "base_rate": 3.5,
                        "limit": 30000,
                        "url": "http://finlife.fss.or.kr/"
                    })
                
                if api_loans:
                    FirebaseManager.sync_data('loan_products', api_loans, id_field='id')
                    existing_ids = {i.get('id') for i in items}
                    items += [al for al in api_loans if al.get('id') not in existing_ids]
            else:
                print(f"FSS API Response Error: Received HTML or Invalid JSON (Status: {res.status_code})")
        except Exception as e:
            print(f"FSS API Logic Error: {e}")
            
        return items

class OntongWelfareService:
    """3단계: 고용노동부(온통청년) 청년정책 연동"""
    
    @staticmethod
    def get_welfare_policies(age, region_name):
        # 1. Firebase Archive 로드
        items = FirebaseManager.fetch_archive('welfare_policies', region_name)
        
        # 2. API 호출 시도
        api_key = env('YOUTH_CENTER_KEY', default='').strip()
        if not api_key:
            return items
        
        # 🎯 복지로(Bokjiro) API - 온통청년 지연 시 우선 활용 (신뢰도 높음)
        url_bokjiro = "http://apis.data.go.kr/B554287/NationalWelfareInformationsV001/NationalWelfarelistV001"
        decoded_key = urllib.parse.unquote(api_key) # 온통청년 키를 범용 공공데이터 키로 가정 (보통 동일함)
        params_bokjiro = {'serviceKey': decoded_key, 'callTp': 'L', 'srchKeyCode': '001', 'numOfRows': 20, 'pageNo': 1}
        
        try:
            res_b = requests.get(url_bokjiro, params=params_bokjiro, timeout=10)
            if res_b.status_code == 200:
                root = ET.fromstring(res_b.content)
                for s in root.findall('.//servList'):
                    items.append({
                        "id": f"BOK_{s.findtext('servId')}",
                        "name": s.findtext('servNm'),
                        "org": s.findtext('jurOrgNm', '중앙부처'),
                        "benefit": s.findtext('servDtlNm', '-'),
                        "url": "https://www.bokjiro.go.kr/"
                    })
        except Exception as e:
            print(f"Bokjiro Fallback Error: {e}")

        # 🎯 온통청년 API (명시적 포트 443 사용 및 User-Agent 추가)
        url = f"https://www.youthcenter.go.kr/opi/youthPlcyList.do?display=100&pageIndex=1&openApiVlak={api_key}"
        
        try:
            # 타임아웃 20초로 연장 + 헤더 추가 (봇 차단 방지)
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=20)
            
            if response.status_code == 200:
                root = ET.fromstring(response.text)
                api_policies = []
                for p in root.findall('.//youthPolicy'):
                    policy_id = f"WLF_{p.findtext('bizId', '00')}"
                    api_policies.append({
                        "id": policy_id,
                        "name": p.findtext('polyBizSjnm', '청년 정책'),
                        "org": p.findtext('polyBizTy', 'Youth Center'),
                        "benefit": p.findtext('polyItcnCn', '-'),
                        "url": "https://www.youthcenter.go.kr/",
                        "raw_data": {child.tag: child.text for child in p}
                    })
                
                if api_policies:
                    FirebaseManager.sync_data('welfare_policies', api_policies, id_field='id')
                    existing_names = {i.get('name') for i in items}
                    items += [ap for ap in api_policies if ap.get('name') not in existing_names]
            else:
                print(f"Welfare API Issue: Status {response.status_code}")

        except Exception as e:
            print(f"Welfare API Error (Timeout or Connection): {e}")
            
        return items
