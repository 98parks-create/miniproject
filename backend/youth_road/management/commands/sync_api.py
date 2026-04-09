import os
import requests
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, date
from django.core.management.base import BaseCommand
from youth_road.models import HousingProduct, FinanceProduct, WelfareProduct, HousingMarketData
from youth_road.firebase_service import FirebaseManager
from urllib.parse import unquote

import time

class Command(BaseCommand):
    help = 'Masterpiece Total Harvesting Engine v25 - Maximum Data & Detail'

    def handle(self, *args, **options):
        # [v44.4] RESTORED: Multi-Source Real-time Harvesting (LH/GH/Worknet)
        raw_key = os.getenv('DATA_PORTAL_KEY', 'c59c241aebb26596d40484c1946d9af59feb143da8f8b2f23027d7bef61b6e3c')
        lh_gh_key = 'c59c241aebb26596d40484c1946d9af59feb143da8f8b2f23027d7bef61b6e3c'
        worknet_wanted_key = 'ff512c61-a20a-4429-89ef-b173193177d9'
        worknet_prog_key = '7e32dd70-50ac-413c-a46a-357fa12439ca'
        worknet_strong_key = '33f32ca4-ccbc-4206-934a-699390ea2a02'
        
        fss_key = os.getenv('FSS_FINANCE_KEY')
        youth_key = os.getenv('YOUTH_CENTER_KEY')
        seoul_key = '674251785565757339395267676773' # Final Seoul Open Data Key
        decoded_key = unquote(raw_key) if raw_key else ""
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

        self.stdout.write(self.style.SUCCESS(f"=== Zero-Gap Harvesting Engine v44.3 (Ultimate Convergence) Started at {datetime.now()} ==="))

        # [STEP 1] 주거 (LH 6종 세트 / GH / HUG 종합 연동)
        self.sync_lh_announcements(lh_gh_key)
        self.sync_lh_pre_subscription(lh_gh_key)
        self.sync_lh_pre_subscription_supply(lh_gh_key) # [NEW] v44.7 사전청약 공급정보
        self.sync_gh_announcements(lh_gh_key)
        self.sync_hug_announcements(lh_gh_key)
        self.sync_housing_deep(raw_key)
        self.sync_sh_housing(raw_key)
        self.sync_myhome_deep(decoded_key)

        # [STEP 2] 정책/복지/고용 (서울청년/워크넷/온통청년)
        self.sync_welfare_massive(decoded_key)
        self.sync_seoul_youth_policy(seoul_key)
        self.sync_worknet_massive(worknet_wanted_key, worknet_prog_key, worknet_strong_key)
        self.sync_youth_center_resilient(youth_key)

        # [STEP 3] 금융 (FSS/서민금융)
        self.sync_finance_total(fss_key, raw_key)
        self.sync_microfinance_products(lh_gh_key)

        # [FINAL] 정화 및 동기화
        self.purge_expired_data()
        self.sync_all_to_firebase()
        
        self.stdout.write(self.style.SUCCESS("🏆 COMPREHENSIVE DATA HARVESTING COMPLETE (v43.2 - PCM Ultimate Edition)!"))

    def fetch_api_pages(self, endpoint, api_key, pages=5, per_page=100, extra_params=None):
        """다중 페이지 API 수집 범용 함수 (v27.2 재시도/안정성 특화)"""
        all_data = []
        for p in range(1, pages + 1):
            params = {'page': p, 'perPage': per_page, 'serviceKey': api_key, 'returnType': 'JSON'}
            if extra_params: params.update(extra_params)
            
            success = False
            for attempt in range(1, 4): # 최대 3회 재시도
                try:
                    res = requests.get(endpoint, params=params, headers=self.headers, timeout=30)
                    data = res.json().get('data', [])
                    if not data: 
                        success = True # 데이터가 없는 것은 정상 종료
                        break
                    all_data.extend(data)
                    self.stdout.write(f"    - Fetching {endpoint.split('/')[-1]}: Page {p} ({len(data)} items)")
                    success = True
                    break
                except Exception as e:
                    self.stderr.write(f"    ! Attempt {attempt} Failed (Page {p}): {e}")
                    time.sleep(1) # 잠시 대기 후 재시도
            
            if not success:
                self.stderr.write(f"    !! Page {p} Skip after 3 attempts.")
                continue # 다음 페이지로 진행
                
        return all_data

    def sync_housing_deep(self, api_key):
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting Housing Prices (Deep Sweep)..."))
        # 분양가 데이터 10페이지 수집 (3,000~5,000개 모델 커버)
        price_endpoint = "https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getAPTLttotPblancMdl"
        price_data = self.fetch_api_pages(price_endpoint, api_key, pages=10, per_page=500)
        price_map = {}
        for p in price_data:
            m_no = p.get('HOUSE_MANAGE_NO')
            val = int(p.get('LTTOT_TOP_AMOUNT', 0))
            if m_no not in price_map or val > price_map[m_no]:
                price_map[m_no] = val

        self.stdout.write(self.style.HTTP_INFO("  > Harvesting Housing Announcements (APT/Urbty/Ofctl)..."))
        endpoints = {
            'APT': "https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getAPTLttotPblancDetail",
            'Urbty/Ofctl': "https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getUrbtyOfctlLttotPblancDetail"
        }
        
        for cat, url in endpoints.items():
            # v34: 페이지 수를 대폭 늘려(5->20) 서울 등 인기 지역 공고 확보
            items = self.fetch_api_pages(url, api_key, pages=20, per_page=100)
            for item in items:
                m_no = item.get('HOUSE_MANAGE_NO') or item.get('PBLANC_NO')
                h_name = item.get('HOUSE_NM', '주택 공고')
                final_price = price_map.get(m_no, 0)
                
                # [v23] Fusion Fallback
                if not final_price:
                    market = HousingMarketData.objects.filter(complex_name__icontains=h_name[:8]).first()
                    if market and market.sales_price: final_price = int(market.sales_price // 10000)

                def fmt_de(d):
                    if not d: return None
                    digits = re.sub(r'[^0-9]', '', str(d))
                    if len(digits) >= 8: return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
                    return None

                # [v25] 지능형 태그/상세 요건 추출 (Regex)
                tags = []
                if re.search(r'신혼|부부', h_name): tags.append("신혼부부")
                if re.search(r'생애|최초', h_name): tags.append("생애최초")
                if re.search(r'청년|대학생', h_name): tags.append("청년전용")

                HousingProduct.objects.update_or_create(
                    manage_no=m_no, 
                    defaults={
                        'title': h_name, 
                        'category': item.get('HOUSE_SECD_NM', cat), 
                        'region': item.get('SUBSCRPT_AREA_CODE_NM', '전국'), 
                        'location': item.get('HSSPLY_ADRES', ''), 
                        'url': item.get('PBLANC_URL'), 
                        'org': item.get('BSNS_MBY_NM', '시시행사'), 
                        'sales_price': final_price,
                        'notice_date': fmt_de(item.get('RCRIT_PBLANC_DE')),
                        'end_date': fmt_de(item.get('PBLANC_END_DE')),
                        'raw_data': {**item, 'v25_tags': tags}
                    }
                )

    def sync_sh_housing(self, api_key):
        endpoint = "https://api.odcloud.kr/api/15008820/v1/uddi:6c80ca2d-dccc-4bd9-8068-feaea3d3d110"
        try:
            # v34: SH(서울주택도시공사) 공고 확보량 증대
            items = self.fetch_api_pages(endpoint, api_key, pages=10, per_page=100)
            for item in items:
                title = item.get('단지명', 'SH공공분양')
                market = HousingMarketData.objects.filter(complex_name__icontains=title[:8]).first()
                final_price = int(market.sales_price // 10000) if market and market.sales_price else 0
                HousingProduct.objects.update_or_create(
                    manage_no=f"SH_H_{title}", 
                    defaults={
                        'title': f"[SH분양] {title}", 
                        'category': '공공분양', 'region': '서울', 'org': '서울주택도시공사', 
                        'sales_price': final_price, 'raw_data': item
                    }
                )
        except Exception as e: self.stderr.write(f"SH Error: {e}")

    def sync_myhome_deep(self, api_key):
        url = "http://apis.data.go.kr/1613000/HWSPR02/rsdtRcritNtcList"
        # MyHome은 XML/JSON에 따라 다르나 파라미터로 대량 수집 (v34: 300->1000)
        try:
            res = requests.get(url, params={'serviceKey': api_key, 'numOfRows': 1000, 'pageNo': 1}, timeout=15)
            items = res.json().get('response', {}).get('body', {}).get('item', [])
            if isinstance(items, dict): items = [items]
            for item in items:
                p_id = item.get('pblancId')
                # 텍스트에서 가격 추출 시도
                p_text = item.get('pblancNm', '')
                price_guess = 0
                price_match = re.search(r'(\d+)만원|(\d+)천원', p_text)
                if price_match:
                    price_guess = int(price_match.group(1)) if '만원' in price_match.group(0) else int(price_match.group(1)) // 10
                
                def fmt_de(d):
                    if not d: return None
                    digits = re.sub(r'[^0-9]', '', str(d))
                    if len(digits) >= 8: return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
                    return None

                HousingProduct.objects.update_or_create(
                    manage_no=f"MYHOME_{p_id}", 
                    defaults={
                        'title': f"[공공임대] {item.get('pblancNm')}", 
                        'category': item.get('suplyTyNm', '공공주택'), 
                        'region': item.get('signguNm', '전국'), 
                        'org': item.get('suplyInsttNm', 'LH/SH'),
                        'sales_price': price_guess,
                        'notice_date': fmt_de(item.get('pblancDe')),
                        'end_date': fmt_de(item.get('rcritEndDe')),
                        'raw_data': item
                    }
                )
        except Exception as e: self.stderr.write(f"MyHome Error: {e}")

    def sync_finance_total(self, fss_key, hug_key):
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting Finance (Broad Spectrum)..."))
        # [FSS] 전세자금대출 + 주택담보대출 통합
        endpoints = [
            "http://finlife.fss.or.kr/finlifeapi/rentHouseLoanProductsSearch.json",
            "http://finlife.fss.or.kr/finlifeapi/mortgageLoanProductsSearch.json"
        ]
        for url in endpoints:
            try:
                res = requests.get(url, params={'auth': fss_key, 'topFinGrpNo': '020000', 'pageNo': 1}, timeout=10)
                items = res.json().get('result', {}).get('baseList', [])
                for item in items:
                    FinanceProduct.objects.update_or_create(
                        product_id=f"FSS_{item.get('fin_prdt_cd')}", 
                        defaults={'title': item.get('fin_prdt_nm'), 'bank_nm': item.get('kor_co_nm'), 'raw_data': item}
                    )
            except Exception as e: self.stdout.write(f"    ! FSS Skip: {e}")

    def sync_welfare_massive(self, api_key):
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting Welfare (Cross-Category)..."))
        url = "http://apis.data.go.kr/B554287/NationalWelfareInformationsV001/NationalWelfarelistV001"
        # 001(기초), 004(주거) 등 주요 카테고리 순회
        for code in ['001', '004']:
            try:
                res = requests.get(url, params={'serviceKey': api_key, 'callTp': 'L', 'srchKeyCode': code, 'numOfRows': 200}, timeout=15)
                root = ET.fromstring(res.content)
                items = root.findall('.//servList')
                for item in items:
                    srv_id = item.findtext('servId')
                    WelfareProduct.objects.update_or_create(
                        policy_id=f"WELFARE_{srv_id}", 
                        defaults={
                            'title': item.findtext('servNm'), 'org_nm': item.findtext('jurOrgNm'), 
                            'benefit_desc': item.findtext('servDtlNm'), 'target_desc': item.findtext('tgtrNm'),
                            'notice_date': date.today(), # 상시 정책이 많으므로 오늘 날짜로 활성화 유지
                            'raw_data': {'xml': ET.tostring(item, encoding='unicode')}
                        }
                    )
            except Exception as e: self.stdout.write(f"    ! Welfare {code} Skip: {e}")

    def sync_lh_announcements(self, api_key):
        """LH 분양임대공고 연동 (v44.6) - [인증 키 디코딩 최적화]"""
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting LH Final-Certified Announcements..."))
        url = "https://apis.data.go.kr/B552555/lhLeaseNoticeInfo1/getLeaseNoticeInfo1"
        try:
            # LH는 디코딩된 인증키(unquote) 사용이 필수인 경우가 많음
            params = {'serviceKey': unquote(api_key), 'PG_SZ': 50, 'PAGE': 1}
            res = requests.get(url, params=params, timeout=15)
            if res.status_code == 401 or res.status_code == 404:
                 # Fallback: 원본 키 시도
                 res = requests.get(url, params={'serviceKey': api_key, 'PG_SZ': 50, 'PAGE': 1}, timeout=10)
            
            if res.status_code != 200:
                self.stderr.write(f"LH Notice List Access Denied: {res.status_code}")
                return
            
            root = ET.fromstring(res.content)
            for item in root.findall('.//item'):
                pan_id = item.findtext('PAN_ID')
                HousingProduct.objects.update_or_create(
                    manage_no=f"LH_{pan_id}",
                    defaults={
                        'title': f"[LH] {item.findtext('PAN_NM')}",
                        'category': item.findtext('UPP_AIS_TP_NM', '주택공고'),
                        'region': item.findtext('CNP_CD_NM', '전국'),
                        'org': '한국토지주택공사',
                        'notice_date': item.findtext('PAN_NT_ST_DT'),
                        'end_date': item.findtext('CLSG_DT'),
                        'url': f"https://apply.lh.or.kr/lhapply/apply/panc/pancInfoDetail.do?panId={pan_id}",
                        'raw_data': {'xml': ET.tostring(item, encoding='unicode')}
                    }
                )
        except Exception as e: self.stdout.write(f"    ! LH Sync Warning: {e}")

    def sync_lh_pre_subscription(self, api_key):
        """LH 사전청약 정보 연동 (v44.6)"""
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting LH Pre-Subscription Data..."))
        url = "https://apis.data.go.kr/B552555/lhLeaseNoticeBfhInfo1/getLeaseNoticeBfhInfo1"
        try:
            res = requests.get(url, params={'serviceKey': unquote(api_key), 'PG_SZ': 30}, timeout=10)
            if res.status_code != 200: return
            root = ET.fromstring(res.content)
            for item in root.findall('.//item'):
                pan_id = item.findtext('PAN_ID')
                HousingProduct.objects.update_or_create(
                    manage_no=f"LH_PRE_{pan_id}",
                    defaults={
                        'title': f"[LH/사전] {item.findtext('PAN_NM')}",
                        'category': '사전청약',
                        'org': '한국토지주택공사',
                        'notice_date': item.findtext('PAN_NT_ST_DT'),
                        'raw_data': {'xml': ET.tostring(item, encoding='unicode')}
                    }
                )
        except Exception: pass

    def sync_lh_pre_subscription_supply(self, api_key):
        """LH 사전청약 공급정보 상세 연동 (v44.7)"""
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting LH Pre-Subs Supply Details..."))
        url = "https://apis.data.go.kr/B552555/lhLeaseNoticeBfhSplInfo1/getLeaseNoticeBfhSplInfo1"
        try:
            # v44.7: 사전청약 배정 세대수 및 상세 공급 정보 확보
            res = requests.get(url, params={'serviceKey': unquote(api_key), 'PG_SZ': 50}, timeout=10)
            if res.status_code != 200: return
            root = ET.fromstring(res.content)
            for item in root.findall('.//item'):
                pan_id = item.findtext('PAN_ID')
                # 기존 사전청약 상품 데이터 고도화 (Raw Data 업데이트)
                HousingProduct.objects.filter(manage_no=f"LH_PRE_{pan_id}").update(
                    raw_data_detail={'supply_xml': ET.tostring(item, encoding='unicode')}
                )
        except Exception: pass

    def sync_gh_announcements(self, api_key):
        """GH 주택청약 연동 (v44.3)"""
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting GH Housing Announcements..."))
        url = "https://api.odcloud.kr/api/15119391/v1/getGhousePblancInfo"
        try:
            # Odcloud(GH)는 종종 디코딩된 키를 요구함
            res = requests.get(url, params={'serviceKey': unquote(api_key), 'page': 1, 'perPage': 20}, timeout=10)
            self.stdout.write(f"    - GH Response: {res.status_code}")
            data = res.json().get('data', [])
            for item in data:
                title = item.get('PBLANC_NM')
                HousingProduct.objects.update_or_create(
                    manage_no=f"GH_{item.get('PBLANC_ID', title)}",
                    defaults={
                        'title': f"[GH] {title}",
                        'category': '경기공공주택',
                        'region': '경기',
                        'org': '경기주택도시공사',
                        'notice_date': item.get('PBLANC_DE'),
                        'end_date': item.get('RCRIT_END_DE'),
                        'raw_data': item
                    }
                )
        except Exception as e: self.stderr.write(f"GH Error: {e}")

    def sync_hug_announcements(self, api_key):
        """HUG(주택도시보증공사) 융자 및 보증 상품 연동 (v44.1)"""
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting HUG Housing Services..."))
        url = "http://apis.data.go.kr/B551408/rent-housing-loan-rates/getRentHousingLoanRates"
        try:
            res = requests.get(url, params={'serviceKey': api_key, 'numOfRows': 10}, timeout=10)
            # HUG 데이터는 전세보증금 반환보증 및 기금 대출 핵심 금리 포함
            root = ET.fromstring(res.content)
            for item in root.findall('.//item'):
                title = item.findtext('loanPrdtNm', 'HUG 안심 전세')
                HousingProduct.objects.update_or_create(
                    manage_no=f"HUG_{title}",
                    defaults={
                        'title': f"[HUG] {title}",
                        'category': '기금대출/보증',
                        'org': '주택도시보증공사',
                        'benefit_desc': f"신청 대상: {item.findtext('loanTgtNm')}",
                        'raw_data': {'xml': ET.tostring(item, encoding='unicode')}
                    }
                )
        except Exception: pass

    def sync_seoul_youth_policy(self, api_key):
        """서울 청년 몽땅 정보통 정책 연동 (v44.3)"""
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting Seoul Youth Portal Data..."))
        # Seoul Open Data URL Pattern: http://openapi.seoul.go.kr:8088/(key)/(format)/(service)/(start)/(end)/
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/youthPolicy/1/50/"
        try:
            res = requests.get(url, timeout=12)
            data = res.json().get('youthPolicy', {}).get('row', [])
            for item in data:
                WelfareProduct.objects.update_or_create(
                    policy_id=f"SEOUL_{item.get('ID')}",
                    defaults={
                        'title': f"[서울] {item.get('TITLE')}",
                        'org_nm': '서울특별시',
                        'category': '지역특화',
                        'benefit_desc': item.get('CONTENT'),
                        'region': '서울',
                        'raw_data': item
                    }
                )
        except Exception: pass

    def sync_microfinance_products(self, api_key):
        """서민금융진흥원 특수 금융 상품 연동 (v44.1)"""
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting Microfinance Support Products..."))
        url = "http://apis.data.go.kr/B552881/loan-product-info/getLoanProductList"
        try:
            res = requests.get(url, params={'serviceKey': api_key, 'numOfRows': 20}, timeout=10)
            root = ET.fromstring(res.content)
            for item in root.findall('.//item'):
                p_nm = item.findtext('loanPrdtNm')
                FinanceProduct.objects.update_or_create(
                    product_id=f"MICRO_{p_nm}",
                    defaults={
                        'title': f"[서민금융] {p_nm}",
                        'bank_nm': '서민금융진흥원',
                        'raw_data': {'xml': ET.tostring(item, encoding='unicode')}
                    }
                )
        except Exception: pass

    def sync_worknet_massive(self, wanted_key, prog_key, strong_key):
        """워크넷 3종 세트 연동 (v43.2)"""
        self.stdout.write(self.style.HTTP_INFO("  > Harvesting Worknet Youth/Job Data..."))
        
        # 1. 채용정보 (Wanted)
        try:
            url = "http://openapi.work.go.kr/opi/opi/opia/wantedApi.do"
            res = requests.get(url, params={'authKey': wanted_key, 'callTp': 'L', 'returnType': 'XML', 'display': 100}, timeout=10)
            root = ET.fromstring(res.content)
            for item in root.findall('.//wanted'):
                w_id = item.findtext('wantedAuthNo')
                WelfareProduct.objects.update_or_create(
                    policy_id=f"WORKNET_W_{w_id}",
                    defaults={
                        'title': f"[채용] {item.findtext('title')}",
                        'org_nm': item.findtext('company'),
                        'category': '청년일자리',
                        'benefit_desc': f"급여: {item.findtext('sal')}, 지역: {item.findtext('region')}",
                        'url': f"https://www.work.go.kr/empInfo/empInfoSrch/detail/empDetailAuthView.do?wantedAuthNo={w_id}",
                        'raw_data': {'xml': ET.tostring(item, encoding='unicode')}
                    }
                )
        except Exception as e: self.stderr.write(f"Worknet Wanted Error: {e}")

        # 2. 취업지원프로그램 (Prog)
        try:
            url = "http://openapi.work.go.kr/opi/opi/opia/empIdpApi.do"
            res = requests.get(url, params={'authKey': prog_key, 'returnType': 'XML', 'display': 50}, timeout=10)
            root = ET.fromstring(res.content)
            for item in root.findall('.//empIdp'):
                p_id = item.findtext('empIdpNo')
                WelfareProduct.objects.update_or_create(
                    policy_id=f"WORKNET_P_{p_id}",
                    defaults={
                        'title': f"[지원] {item.findtext('empIdpNm')}",
                        'org_nm': item.findtext('instNm'),
                        'category': '취업역량강화',
                        'benefit_desc': item.findtext('empIdpCn'),
                        'raw_data': {'xml': ET.tostring(item, encoding='unicode')}
                    }
                )
        except Exception as e: self.stderr.write(f"Worknet Prog Error: {e}")

    def sync_youth_center_resilient(self, api_key):
        url = "http://www.youthcenter.go.kr/opi/youthPlcyList.do"
        params = {'openApiVlak': api_key, 'display': 1000, 'pageIndex': 1}
        try:
            res = requests.get(url, params=params, headers=self.headers, timeout=12)
            root = ET.fromstring(res.content)
            items = root.findall('.//youthPolicy')
            for item in items:
                plcy_id = item.findtext('bizId')
                WelfareProduct.objects.update_or_create(
                    policy_id=f"YOUTH_{plcy_id}", 
                    defaults={
                        'title': item.findtext('polyBizSjnm'), 'org_nm': item.findtext('cnsgNmor', '국가'),
                        'benefit_desc': item.findtext('polyItcnCn'), 'target_desc': item.findtext('ageInfo'),
                        'raw_data': {'xml': ET.tostring(item, encoding='unicode')}
                    }
                )
            self.stdout.write(self.style.SUCCESS(f"  + YouthCenter: {len(items)} synced."))
        except Exception: pass

    def sync_all_to_firebase(self):
        # [v25] 대량 전송 최적화 (활성 데이터만)
        h_all = HousingProduct.objects.filter(is_active=True)
        h_data = [{ 'manage_no': p.manage_no, 'title': p.title, 'category': p.category, 'sales_price': p.sales_price } for p in h_all]
        FirebaseManager.sync_data('housing_products_v25', h_data, id_field='manage_no')

        f_all = FinanceProduct.objects.filter(is_active=True)
        f_data = [{ 'product_id': p.product_id, 'title': p.title, 'bank_nm': p.bank_nm } for p in f_all]
        FirebaseManager.sync_data('finance_products_v25', f_data, id_field='product_id')

        w_all = WelfareProduct.objects.filter(is_active=True)
        w_data = [{ 'policy_id': p.policy_id, 'title': p.title, 'org': p.org_nm } for p in w_all]
        FirebaseManager.sync_data('welfare_policies_v25', w_data, id_field='policy_id')

    def purge_expired_data(self):
        """[STRICT] Ghost Buster v43: 과거 데이터 일괄 제거"""
        today = date.today()
        from datetime import timedelta
        ghost_limit = today - timedelta(days=180)
        
        # 1. 명시적 종료일이 지난 데이터
        h_purge = HousingProduct.objects.filter(end_date__lt=today, is_active=True).update(is_active=False)
        f_purge = FinanceProduct.objects.filter(end_date__lt=today, is_active=True).update(is_active=False)
        w_purge = WelfareProduct.objects.filter(end_date__lt=today, is_active=True).update(is_active=False)
        
        # 2. 종료일이 없으나 공고일이 6개월 이상 된 '유령 주거 데이터' 제거
        ghost_purge = HousingProduct.objects.filter(
            end_date__isnull=True, 
            notice_date__lt=ghost_limit, 
            is_active=True
        ).update(is_active=False)

        self.stdout.write(self.style.WARNING(f"  ! Purge: Housing({h_purge}), Finance({f_purge}), Welfare({w_purge}), Ghost({ghost_purge}) deactivated."))
