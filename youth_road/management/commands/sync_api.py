import os
import requests
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from django.core.management.base import BaseCommand
from web_for_youth.youth_road.models import HousingProduct, FinanceProduct, WelfareProduct
from web_for_youth.youth_road.firebase_service import FirebaseManager
from urllib.parse import unquote

class Command(BaseCommand):
    help = 'Masterpiece Triple-Split Sync Engine v16 - The Commercial Bank & Youth Hub'

    def handle(self, *args, **options):
        raw_key = os.getenv('DATA_PORTAL_KEY')
        youth_key = os.getenv('YOUTH_CENTER_KEY')
        fss_key = os.getenv('FSS_FINANCE_KEY')
        decoded_key = unquote(raw_key) if raw_key else ""
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

        self.stdout.write(self.style.SUCCESS("=== Triple-Split Sync Engine v16 Started ==="))

        # [STEP 1] 주거 (완벽)
        self.sync_applyhome(raw_key)
        self.sync_sh_housing(raw_key)
        self.sync_myhome(decoded_key)

        # [STEP 2] 금융 (시중은행 FSS 우선 순위)
        self.sync_hug_finance(raw_key)
        self.sync_fss_finance(fss_key)

        # [STEP 3] 복지/정책 (온통청년 최종 연동 파라미터 적용)
        self.sync_welfare(decoded_key)
        self.sync_youth_center(youth_key)

        # [FINAL] Firebase 삼분할 전송
        self.sync_all_to_firebase()

    def sync_applyhome(self, api_key):
        endpoint = "https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getAPTLttotPblancDetail"
        params = {'page': 1, 'perPage': 100, 'serviceKey': api_key, 'returnType': 'JSON'}
        try:
            res = requests.get(endpoint, params=params, headers=self.headers, timeout=10)
            items = res.json().get('data', [])
            for item in items:
                m_no = item.get('HOUSE_MANAGE_NO')
                notice_de = item.get('RCRIT_PBLANC_DE')
                end_de = item.get('PBLANC_END_DE')
                
                # 날짜 형식 보정 (YYYYMMDD -> YYYY-MM-DD 또는 기존 형식 유지)
                def fmt_de(d):
                    if not d: return None
                    d_str = str(d).replace('.', '-').strip()
                    if len(d_str) == 10 and d_str[4] == '-' and d_str[7] == '-':
                        return d_str
                    if len(d_str) == 8 and d_str.isdigit():
                        return f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:8]}"
                    return None

                HousingProduct.objects.update_or_create(
                    manage_no=m_no, 
                    defaults={
                        'pblanc_no': item.get('PBLANC_NO'), 
                        'title': item.get('HOUSE_NM'), 
                        'category': item.get('HOUSE_SECD_NM', '민영주택'), 
                        'region': item.get('SUBSCRPT_AREA_CODE_NM', '전국'), 
                        'location': item.get('HSSPLY_ADRES', ''), 
                        'url': item.get('PBLANC_URL'), 
                        'org': item.get('BSNS_MBY_NM', '시행사'), 
                        'notice_date': fmt_de(notice_de),
                        'end_date': fmt_de(end_de),
                        'raw_data': item
                    }
                )
            self.stdout.write(self.style.SUCCESS(f"  + Housing(ApplyHome): {len(items)} synced."))
        except Exception as e: self.stderr.write(f"ApplyHome Error: {e}")

    def sync_sh_housing(self, api_key):
        endpoint = "https://api.odcloud.kr/api/15008820/v1/uddi:6c80ca2d-dccc-4bd9-8068-feaea3d3d110"
        params = {'page': 1, 'perPage': 50, 'serviceKey': api_key}
        try:
            res = requests.get(endpoint, params=params, headers=self.headers, timeout=10)
            items = res.json().get('data', [])
            for item in items:
                title = item.get('단지명', 'SH공공분양')
                year = item.get('공급시기 예정 년도')
                month = item.get('공급시기 예정월')
                
                notice_de = None
                if year and month:
                    notice_de = f"{year}-{str(month).zfill(2)}-01"

                HousingProduct.objects.update_or_create(
                    manage_no=f"SH_H_{title}", 
                    defaults={
                        'title': f"[SH분양] {title}", 
                        'category': '공공분양', 
                        'region': '서울', 
                        'org': '서울주택도시공사', 
                        'notice_date': notice_de,
                        'raw_data': item
                    }
                )
            self.stdout.write(self.style.SUCCESS(f"  + Housing(SH): {len(items)} synced."))
        except Exception as e: self.stderr.write(f"SH Housing Error: {e}")

    def sync_myhome(self, api_key):
        url = "http://apis.data.go.kr/1613000/HWSPR02/rsdtRcritNtcList"
        params = {'serviceKey': api_key, 'numOfRows': 100, 'pageNo': 1}
        try:
            res = requests.get(url, params=params, headers=self.headers, timeout=10)
            items = res.json().get('response', {}).get('body', {}).get('item', [])
            if isinstance(items, dict): items = [items]
            for item in items:
                p_id = item.get('pblancId')
                notice_de = item.get('rcritPblancDe')
                end_de = item.get('endDe')
                
                def fmt_de(d):
                    if not d or len(str(d)) < 8: return None
                    d_str = str(d)
                    return f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:8]}"

                HousingProduct.objects.update_or_create(
                    manage_no=f"MYHOME_{p_id}", 
                    defaults={
                        'title': f"[공공임대] {item.get('pblancNm', '공간임대주택')}", 
                        'category': item.get('suplyTyNm', '공공주택'), 
                        'region': item.get('signguNm', '전국'), 
                        'url': item.get('url', ''), 
                        'org': item.get('suplyInsttNm', 'LH/SH'),
                        'notice_date': fmt_de(notice_de),
                        'end_date': fmt_de(end_de),
                        'raw_data': item
                    }
                )
            self.stdout.write(self.style.SUCCESS(f"  + Housing(MyHome): {len(items)} synced."))
        except Exception as e: self.stderr.write(f"MyHome Error: {e}")

    def sync_hug_finance(self, api_key):
        endpoint = "https://api.odcloud.kr/api/15134235/v1/uddi:c301ade3-c98f-4ed7-938c-ec0f060d8cde"
        params = {'page': 1, 'perPage': 100, 'serviceKey': api_key}
        try:
            res = requests.get(endpoint, params=params, headers=self.headers, timeout=10)
            items = res.json().get('data', [])
            for item in items:
                name = item.get('상품명', '정부대출상품')
                FinanceProduct.objects.update_or_create(product_id=f"HUG_{name}", defaults={'title': name, 'bank_nm': 'HUG', 'raw_data': item})
            self.stdout.write(self.style.SUCCESS(f"  + Finance(HUG): {len(items)} synced."))
        except Exception as e: self.stderr.write(f"HUG Error: {e}")

    def sync_fss_finance(self, api_key):
        # 일반 대출 경로도 함께 확인하여 시중은행 데이터 보장
        url = "http://finlife.fss.or.kr/finlifeapi/rentHouseLoanProductsSearch.json"
        params = {'auth': api_key, 'topFinGrpNo': '020000', 'pageNo': 1}
        try:
            res = requests.get(url, params=params, headers=self.headers, timeout=10)
            data = res.json()
            items = data.get('result', {}).get('baseList', [])
            for item in items:
                FinanceProduct.objects.update_or_create(product_id=f"FSS_{item.get('fin_prdt_cd')}", defaults={'title': item.get('fin_prdt_nm'), 'bank_nm': item.get('kor_co_nm'), 'raw_data': item})
            self.stdout.write(self.style.SUCCESS(f"  + Finance(FSS-Banks): {len(items)} synced."))
        except Exception as e: self.stderr.write(f"FSS Error (v16): {e}. Trying credit loans...")
        
        # 시중은행 개인신용대출 데이터로 시중은행 데이터 확보 보완
        url2 = "http://finlife.fss.or.kr/finlifeapi/creditLoanProductsSearch.json"
        try:
            res2 = requests.get(url2, params=params, headers=self.headers, timeout=10)
            items = res2.json().get('result', {}).get('baseList', [])
            for item in items:
                FinanceProduct.objects.update_or_create(product_id=f"FSS_C_{item.get('fin_prdt_cd')}", defaults={'title': f"[신용대출] {item.get('fin_prdt_nm')}", 'bank_nm': item.get('kor_co_nm'), 'raw_data': item})
            self.stdout.write(self.style.SUCCESS(f"  + Finance(FSS-Credit): {len(items)} synced."))
        except Exception as e: self.stderr.write(f"FSS Credit Error: {e}")

    def sync_welfare(self, api_key):
        url = "http://apis.data.go.kr/B554287/NationalWelfareInformationsV001/NationalWelfarelistV001"
        params = {'serviceKey': api_key, 'callTp': 'L', 'srchKeyCode': '001', 'numOfRows': 100, 'pageNo': 1}
        try:
            res = requests.get(url, params=params, headers=self.headers, timeout=10)
            root = ET.fromstring(res.content)
            items = root.findall('.//servList')
            for item in items:
                srv_id = item.findtext('servId')
                raw_str = ET.tostring(item, encoding='unicode')
                reg_date = item.findtext('svcfrstRegTs')
                
                # Bokjiro date formatting (YYYYMMDD -> YYYY-MM-DD)
                def fmt_de(d):
                    if not d or len(str(d)) < 8: return None
                    d_str = str(d)
                    return f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:8]}"

                WelfareProduct.objects.update_or_create(
                    policy_id=f"WELFARE_{srv_id}", 
                    defaults={
                        'title': item.findtext('servNm', '복지정책'), 
                        'org_nm': item.findtext('jurOrgNm', '중앙부처'), 
                        'region': '전국', 
                        'benefit_desc': item.findtext('servDtlNm', '상세내용 없음'), 
                        'target_desc': item.findtext('tgtrNm', '전국민'), 
                        'notice_date': fmt_de(reg_date),
                        'raw_data': {'xml': raw_str}
                    }
                )
            self.stdout.write(self.style.SUCCESS(f"  + Welfare(Bokjiro): {len(items)} synced."))
        except Exception as e: self.stderr.write(f"Welfare Error: {e}")

    def sync_youth_center(self, api_key):
        # [v20] 온통청년 핵심 파라미터 안정화: HTTPS -> HTTP 자동 폴백 적용
        url_https = "https://www.youthcenter.go.kr/opi/youthPlcyList.do"
        url_http = "http://www.youthcenter.go.kr/opi/youthPlcyList.do"
        params = {'openApiVlak': api_key, 'display': 100, 'pageIndex': 1}
        
        res = None
        try:
            # 1차 시도: HTTPS (Timeout 15s)
            res = requests.get(url_https, params=params, headers=self.headers, timeout=15)
        except Exception:
            try:
                # 2차 시도: HTTP 폴백
                res = requests.get(url_http, params=params, headers=self.headers, timeout=15)
            except Exception as e:
                self.stderr.write(self.style.WARNING(f"  ! YouthCenter API Unavailable (v20): {e}"))
                return

        if not res or res.status_code != 200:
            return

        try:
            root = ET.fromstring(res.content)
            items = root.findall('.//youthPolicy')
            for item in items:
                plcy_id = item.findtext('bizId')
                raw_str = ET.tostring(item, encoding='unicode')
                prd_cn = item.findtext('rqutPrdCn', '') # 신청기간내용
                
                # 간단한 날짜 파싱 (기한이 명시된 경우만)
                import re
                end_date_val = None
                date_match = re.findall(r'\d{4}-\d{2}-\d{2}', prd_cn)
                if len(date_match) >= 2:
                    end_date_val = date_match[1] # 종료일 추정

                WelfareProduct.objects.update_or_create(
                    policy_id=f"YOUTH_{plcy_id}", 
                    defaults={
                        'title': item.findtext('polyBizSjnm', '청년정책'), 
                        'org_nm': item.findtext('cnsgNmor', '고용노동부'), 
                        'region': item.findtext('polyReginSeNm', '전국'), 
                        'benefit_desc': item.findtext('polyItcnCn', '청년 혜택'), 
                        'target_desc': item.findtext('ageInfo', '청년'), 
                        'end_date': end_date_val,
                        'raw_data': {'xml': raw_str}
                    }
                )
            self.stdout.write(self.style.SUCCESS(f"  + Welfare(YouthCenter): {len(items)} synced."))
        except Exception as e: 
            self.stderr.write(f"YouthCenter Parsing Error: {e}")

    def sync_all_to_firebase(self):
        h_data = [{ 'manage_no': p.manage_no, 'title': p.title, 'category': p.category, 'region': p.region, 'url': p.url, 'org': p.org } for p in HousingProduct.objects.all()]
        FirebaseManager.sync_data('housing_products', h_data, id_field='manage_no')
        f_data = [{ 'product_id': p.product_id, 'title': p.title, 'bank_nm': p.bank_nm, 'base_rate': p.base_rate, 'limit': p.limit_amt, 'url': p.url } for p in FinanceProduct.objects.all()]
        FirebaseManager.sync_data('finance_products', f_data, id_field='product_id')
        w_data = [{ 'policy_id': p.policy_id, 'title': p.title, 'org': p.org_nm, 'category': p.category, 'region': p.region, 'url': p.url } for p in WelfareProduct.objects.all()]
        FirebaseManager.sync_data('welfare_policies', w_data, id_field='policy_id')
        self.stdout.write(self.style.SUCCESS(f"🏆 ALL SYSTEM INTEGRATION COMPLETE (v20.0 - Hyper-Strict Edition)!"))
