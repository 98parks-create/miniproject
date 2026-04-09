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

class Command(BaseCommand):
    help = 'Masterpiece Total Harvesting Engine v25 - Maximum Data & Detail'

    def handle(self, *args, **options):
        raw_key = os.getenv('DATA_PORTAL_KEY')
        youth_key = os.getenv('YOUTH_CENTER_KEY')
        fss_key = os.getenv('FSS_FINANCE_KEY')
        decoded_key = unquote(raw_key) if raw_key else ""
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

        self.stdout.write(self.style.SUCCESS("=== Total Harvesting Engine v25 (Max Data & Detail) Started ==="))

        # [STEP 1] 주거 (다각도 다량 채굴)
        self.sync_housing_deep(raw_key)
        self.sync_sh_housing(raw_key)
        self.sync_myhome_deep(decoded_key)

        # [STEP 2] 금융 (전 카테고리 기적의 전수조사)
        self.sync_finance_total(fss_key, raw_key)

        # [STEP 3] 복지/정책 (주요 키워드별 대량 수집)
        self.sync_welfare_massive(decoded_key)
        self.sync_youth_center_resilient(youth_key)

        # [FINAL] Firebase 대량 자동 연동 및 정화
        self.sync_all_to_firebase()
        self.purge_expired_data()
        
        self.stdout.write(self.style.SUCCESS("🏆 COMPREHENSIVE DATA HARVESTING COMPLETE (v25.0 - Ultimate Edition)!"))

    def fetch_api_pages(self, endpoint, api_key, pages=5, per_page=100, extra_params=None):
        """다중 페이지 API 수집 범용 함수"""
        all_data = []
        for p in range(1, pages + 1):
            params = {'page': p, 'perPage': per_page, 'serviceKey': api_key, 'returnType': 'JSON'}
            if extra_params: params.update(extra_params)
            try:
                res = requests.get(endpoint, params=params, headers=self.headers, timeout=12)
                data = res.json().get('data', [])
                if not data: break
                all_data.extend(data)
                self.stdout.write(f"    - Fetching {endpoint.split('/')[-1]}: Page {p} ({len(data)} items)")
            except Exception as e:
                self.stderr.write(f"    ! Error on Page {p}: {e}")
                break
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
            items = self.fetch_api_pages(url, api_key, pages=5, per_page=100)
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
            items = self.fetch_api_pages(endpoint, api_key, pages=3, per_page=100)
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
        # MyHome은 XML/JSON에 따라 다르나 파라미터로 대량 수집
        try:
            res = requests.get(url, params={'serviceKey': api_key, 'numOfRows': 300, 'pageNo': 1}, timeout=15)
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
                
                HousingProduct.objects.update_or_create(
                    manage_no=f"MYHOME_{p_id}", 
                    defaults={
                        'title': f"[공공임대] {item.get('pblancNm')}", 
                        'category': item.get('suplyTyNm', '공공주택'), 
                        'region': item.get('signguNm', '전국'), 
                        'org': item.get('suplyInsttNm', 'LH/SH'),
                        'sales_price': price_guess,
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
                            'raw_data': {'xml': ET.tostring(item, encoding='unicode')}
                        }
                    )
            except Exception as e: self.stdout.write(f"    ! Welfare {code} Skip: {e}")

    def sync_youth_center_resilient(self, api_key):
        url = "http://www.youthcenter.go.kr/opi/youthPlcyList.do"
        params = {'openApiVlak': api_key, 'display': 300, 'pageIndex': 1}
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
        today = date.today()
        HousingProduct.objects.filter(end_date__lt=today, is_active=True).update(is_active=False)
        FinanceProduct.objects.filter(end_date__lt=today, is_active=True).update(is_active=False)
        WelfareProduct.objects.filter(end_date__lt=today, is_active=True).update(is_active=False)
