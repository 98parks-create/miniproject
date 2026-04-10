import os
import re
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
from urllib.parse import unquote

from django.core.management.base import BaseCommand
from youth_road.models import HousingProduct, FinanceProduct, WelfareProduct
from youth_road.firebase_service import FirebaseManager


def fmt_date(raw):
    """YYYYMMDD 혹은 YYYY-MM-DD 형식의 문자열을 date 객체로 변환. 실패 시 None."""
    if not raw:
        return None
    digits = re.sub(r'[^0-9]', '', str(raw))
    if len(digits) >= 8:
        try:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
    return None


class Command(BaseCommand):
    help = 'API 전체 수집 및 분야별 DB 저장 엔진 v46.0'

    def handle(self, *args, **options):
        raw_key  = os.getenv('DATA_PORTAL_KEY', '').strip()
        fss_key  = os.getenv('FSS_FINANCE_KEY', '').strip()
        youth_key = os.getenv('YOUTH_CENTER_KEY', '').strip()
        seoul_key = os.getenv('SEOUL_DATA_KEY', '674251785565757339395267676773').strip()

        # 공공데이터포털 키는 URL 인코딩된 형태로 오기도 함
        decoded_key = unquote(raw_key) if raw_key else ''

        lh_key = decoded_key or 'c59c241aebb26596d40484c1946d9af59feb143da8f8b2f23027d7bef61b6e3c'
        odcloud_key = lh_key

        worknet_wanted_key = os.getenv('WORKNET_WANTED_KEY', 'ff512c61-a20a-4429-89ef-b173193177d9')
        worknet_prog_key   = os.getenv('WORKNET_PROG_KEY',   '7e32dd70-50ac-413c-a46a-357fa12439ca')

        self.headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
        }

        self.stdout.write(self.style.SUCCESS(
            f'=== API 전체 수집 엔진 v46.0 시작 ({datetime.now().strftime("%Y-%m-%d %H:%M")}) ==='
        ))

        # ── 주거 ─────────────────────────────────────────────────────────────
        self.sync_lh_announcements(lh_key)
        self.sync_lh_pre_subscription(lh_key)
        self.sync_gh_announcements(odcloud_key)
        self.sync_hug_announcements(lh_key)
        self.sync_housing_apt(odcloud_key)
        self.sync_housing_urbty(odcloud_key)
        self.sync_myhome(decoded_key)
        self.sync_sh_housing(odcloud_key)

        # ── 금융 ─────────────────────────────────────────────────────────────
        self.sync_fss_loans(fss_key)
        self.sync_fss_savings(fss_key)
        self.sync_microfinance(lh_key)

        # ── 복지 ─────────────────────────────────────────────────────────────
        self.sync_bokjiro(decoded_key or lh_key)
        self.sync_youth_center(youth_key)
        self.sync_seoul_youth(seoul_key)
        self.sync_worknet_programs(worknet_prog_key)

        # ── 마감 데이터 비활성화 ────────────────────────────────────────────
        self.purge_expired()

        # ── Firebase 동기화 ──────────────────────────────────────────────────
        self.push_to_firebase()

        self.stdout.write(self.style.SUCCESS('=== 수집 완료 ==='))

    # ──────────────────────────────────────────────────────────────────────────
    # 공통 유틸
    # ──────────────────────────────────────────────────────────────────────────

    def fetch_pages(self, url, key, pages=10, per_page=100, extra=None):
        """공공데이터포털 JSON 페이지네이션 공통 수집기"""
        result = []
        for p in range(1, pages + 1):
            params = {'page': p, 'perPage': per_page, 'serviceKey': key, 'returnType': 'JSON'}
            if extra:
                params.update(extra)
            for attempt in range(3):
                try:
                    res = requests.get(url, params=params, headers=self.headers, timeout=30)
                    data = res.json().get('data', [])
                    if not data:
                        return result  # 마지막 페이지
                    result.extend(data)
                    break
                except Exception as e:
                    if attempt == 2:
                        self.stderr.write(f'  ! fetch_pages page={p} failed: {e}')
                    time.sleep(1)
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # 주거 수집
    # ──────────────────────────────────────────────────────────────────────────

    def sync_lh_announcements(self, key):
        self.stdout.write('  [주거] LH 임대공고 수집...')
        url = 'https://apis.data.go.kr/B552555/lhLeaseNoticeInfo1/getLeaseNoticeInfo1'
        try:
            res = requests.get(url, params={'serviceKey': key, 'PG_SZ': 100, 'PAGE': 1},
                               headers=self.headers, timeout=15)
            root = ET.fromstring(res.content)
            count = 0
            for item in root.findall('.//item'):
                pan_id = item.findtext('PAN_ID', '')
                if not pan_id:
                    continue
                HousingProduct.objects.update_or_create(
                    manage_no=f'LH_{pan_id}',
                    defaults={
                        'title':       f"[LH] {item.findtext('PAN_NM', 'LH 공고')}",
                        'category':    item.findtext('UPP_AIS_TP_NM', '주택공고'),
                        'region':      item.findtext('CNP_CD_NM', '전국'),
                        'org':         '한국토지주택공사',
                        'notice_date': fmt_date(item.findtext('PAN_NT_ST_DT')),
                        'end_date':    fmt_date(item.findtext('CLSG_DT')),
                        'url':         f'https://apply.lh.or.kr/lhapply/apply/panc/pancInfoDetail.do?panId={pan_id}',
                        'is_active':   True,
                        'raw_data':    {child.tag: child.text for child in item},
                    }
                )
                count += 1
            self.stdout.write(f'    + LH 임대공고 {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! LH 공고 수집 오류: {e}')

    def sync_lh_pre_subscription(self, key):
        self.stdout.write('  [주거] LH 사전청약 수집...')
        url = 'https://apis.data.go.kr/B552555/lhLeaseNoticeBfhInfo1/getLeaseNoticeBfhInfo1'
        try:
            res = requests.get(url, params={'serviceKey': key, 'PG_SZ': 50},
                               headers=self.headers, timeout=10)
            root = ET.fromstring(res.content)
            count = 0
            for item in root.findall('.//item'):
                pan_id = item.findtext('PAN_ID', '')
                if not pan_id:
                    continue
                raw = {child.tag: child.text for child in item}
                HousingProduct.objects.update_or_create(
                    manage_no=f'LH_PRE_{pan_id}',
                    defaults={
                        'title':       f"[LH/사전] {item.findtext('PAN_NM', '사전청약')}",
                        'category':    '사전청약',
                        'region':      item.findtext('CNP_CD_NM', '전국'),
                        'org':         '한국토지주택공사',
                        'notice_date': fmt_date(item.findtext('PAN_NT_ST_DT')),
                        'end_date':    fmt_date(item.findtext('CLSG_DT')),
                        'url':         f'https://apply.lh.or.kr/',
                        'is_active':   True,
                        'raw_data':    raw,
                    }
                )
                count += 1
            self.stdout.write(f'    + LH 사전청약 {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! LH 사전청약 오류: {e}')

    def sync_gh_announcements(self, key):
        self.stdout.write('  [주거] GH(경기주택도시공사) 공고 수집...')
        url = 'https://api.odcloud.kr/api/15119391/v1/getGhousePblancInfo'
        try:
            items = self.fetch_pages(url, key, pages=5)
            count = 0
            for item in items:
                title = item.get('PBLANC_NM', 'GH 공고')
                HousingProduct.objects.update_or_create(
                    manage_no=f"GH_{item.get('PBLANC_ID', title[:30])}",
                    defaults={
                        'title':       f'[GH] {title}',
                        'category':    '경기공공주택',
                        'region':      '경기',
                        'org':         '경기주택도시공사',
                        'notice_date': fmt_date(item.get('PBLANC_DE')),
                        'end_date':    fmt_date(item.get('RCRIT_END_DE')),
                        'is_active':   True,
                        'raw_data':    item,
                    }
                )
                count += 1
            self.stdout.write(f'    + GH {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! GH 오류: {e}')

    def sync_hug_announcements(self, key):
        self.stdout.write('  [주거] HUG(주택도시보증공사) 기금 상품 수집...')
        url = 'http://apis.data.go.kr/B551408/rent-housing-loan-rates/getRentHousingLoanRates'
        try:
            res = requests.get(url, params={'serviceKey': key, 'numOfRows': 30},
                               headers=self.headers, timeout=10)
            root = ET.fromstring(res.content)
            count = 0
            for item in root.findall('.//item'):
                title = item.findtext('loanPrdtNm', 'HUG 안심전세')
                raw = {child.tag: child.text for child in item}
                # HUG 기금 상품은 금융(FinanceProduct)으로 저장
                FinanceProduct.objects.update_or_create(
                    product_id=f'HUG_{title}',
                    defaults={
                        'title':       f'[HUG] {title}',
                        'bank_nm':     '주택도시보증공사',
                        'category':    '기금대출/보증',
                        'target_desc': item.findtext('loanTgtNm', ''),
                        'base_rate':   float(item.findtext('loanBaseRt', '0') or 0),
                        'url':         'https://www.khug.or.kr/',
                        'is_active':   True,
                        'raw_data':    raw,
                    }
                )
                count += 1
            self.stdout.write(f'    + HUG 기금상품 {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! HUG 오류: {e}')

    def sync_housing_apt(self, key):
        self.stdout.write('  [주거] 청약홈 APT 분양공고 수집...')
        url = 'https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getAPTLttotPblancDetail'
        try:
            items = self.fetch_pages(url, key, pages=20, per_page=100)
            self._save_housing_items(items, prefix='APT')
            self.stdout.write(f'    + APT 공고 {len(items)}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! APT 공고 오류: {e}')

    def sync_housing_urbty(self, key):
        self.stdout.write('  [주거] 청약홈 도시형·오피스텔 공고 수집...')
        url = 'https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getUrbtyOfctlLttotPblancDetail'
        try:
            items = self.fetch_pages(url, key, pages=10, per_page=100)
            self._save_housing_items(items, prefix='URBTY')
            self.stdout.write(f'    + 도시형/오피스텔 {len(items)}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! 도시형 공고 오류: {e}')

    def _save_housing_items(self, items, prefix='APT'):
        """청약홈 공통 항목 저장 헬퍼"""
        for item in items:
            m_no = item.get('HOUSE_MANAGE_NO') or item.get('PBLANC_NO')
            if not m_no:
                continue
            h_name = item.get('HOUSE_NM', '주택공고')
            tags = []
            if re.search(r'신혼|부부', h_name):   tags.append('신혼부부')
            if re.search(r'생애|최초', h_name):   tags.append('생애최초')
            if re.search(r'청년|대학생', h_name): tags.append('청년전용')

            # 분양가: LTTOT_TOP_AMOUNT 또는 0
            price_raw = item.get('LTTOT_TOP_AMOUNT') or item.get('PARCPRC_UE', 0)
            try:
                price = int(price_raw) if price_raw else 0
            except (ValueError, TypeError):
                price = 0

            HousingProduct.objects.update_or_create(
                manage_no=f'{prefix}_{m_no}',
                defaults={
                    'title':       h_name,
                    'category':    item.get('HOUSE_SECD_NM', prefix),
                    'region':      item.get('SUBSCRPT_AREA_CODE_NM', '전국'),
                    'location':    item.get('HSSPLY_ADRES', ''),
                    'org':         item.get('BSNS_MBY_NM', ''),
                    'sales_price': price,
                    'notice_date': fmt_date(item.get('RCRIT_PBLANC_DE')),
                    'start_date':  fmt_date(item.get('RCEPT_BGNDE')),
                    'end_date':    fmt_date(item.get('RCEPT_ENDDE') or item.get('PBLANC_END_DE')),
                    'url':         item.get('PBLANC_URL') or 'https://www.applyhome.co.kr/',
                    'is_active':   True,
                    'raw_data':    {**item, 'tags': tags},
                }
            )

    def sync_myhome(self, key):
        self.stdout.write('  [주거] MyHome(공공주택) 공고 수집...')
        url = 'http://apis.data.go.kr/1613000/HWSPR02/rsdtRcritNtcList'
        if not key:
            self.stdout.write('    - DATA_PORTAL_KEY 없음, 건너뜀')
            return
        try:
            res = requests.get(url, params={'serviceKey': key, 'numOfRows': 500, 'pageNo': 1},
                               headers=self.headers, timeout=15)
            body = res.json().get('response', {}).get('body', {})
            items = body.get('item', [])
            if isinstance(items, dict):
                items = [items]
            count = 0
            for item in items:
                p_id = item.get('pblancId')
                if not p_id:
                    continue
                HousingProduct.objects.update_or_create(
                    manage_no=f'MYHOME_{p_id}',
                    defaults={
                        'title':       f"[공공임대] {item.get('pblancNm', '공공주택')}",
                        'category':    item.get('suplyTyNm', '공공주택'),
                        'region':      item.get('signguNm', '전국'),
                        'org':         item.get('suplyInsttNm', 'LH/SH'),
                        'notice_date': fmt_date(item.get('pblancDe')),
                        'end_date':    fmt_date(item.get('rcritEndDe')),
                        'url':         'https://www.myhome.go.kr/',
                        'is_active':   True,
                        'raw_data':    item,
                    }
                )
                count += 1
            self.stdout.write(f'    + MyHome {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! MyHome 오류: {e}')

    def sync_sh_housing(self, key):
        self.stdout.write('  [주거] SH(서울주택도시공사) 공고 수집...')
        url = 'https://api.odcloud.kr/api/15008820/v1/uddi:6c80ca2d-dccc-4bd9-8068-feaea3d3d110'
        try:
            items = self.fetch_pages(url, key, pages=5, per_page=100)
            count = 0
            for item in items:
                title = item.get('단지명', 'SH공공분양')
                HousingProduct.objects.update_or_create(
                    manage_no=f'SH_{title[:40]}',
                    defaults={
                        'title':    f'[SH분양] {title}',
                        'category': '공공분양',
                        'region':   '서울',
                        'org':      '서울주택도시공사',
                        'url':      'https://www.i-sh.co.kr/',
                        'is_active': True,
                        'raw_data': item,
                    }
                )
                count += 1
            self.stdout.write(f'    + SH {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! SH 오류: {e}')

    # ──────────────────────────────────────────────────────────────────────────
    # 금융 수집
    # ──────────────────────────────────────────────────────────────────────────

    def sync_fss_loans(self, key):
        """금감원 전세·주담대 상품 수집 → FinanceProduct"""
        self.stdout.write('  [금융] 금감원(FSS) 대출 상품 수집...')
        if not key:
            self.stdout.write('    - FSS_FINANCE_KEY 없음, 건너뜀')
            return
        endpoints = [
            ('전세자금대출', 'http://finlife.fss.or.kr/finlifeapi/rentHouseLoanProductsSearch.json'),
            ('주택담보대출', 'http://finlife.fss.or.kr/finlifeapi/mortgageLoanProductsSearch.json'),
        ]
        for category, url in endpoints:
            try:
                res = requests.get(url,
                    params={'auth': key, 'topFinGrpNo': '020000', 'pageNo': 1},
                    headers=self.headers, timeout=15)
                result = res.json().get('result', {})
                base_list    = result.get('baseList', [])
                option_list  = result.get('optionList', [])

                # 금리 정보를 상품 코드 기준으로 매핑
                rate_map = {}
                for opt in option_list:
                    code = opt.get('fin_prdt_cd')
                    rate = float(opt.get('intr_rate') or 0)
                    rate2 = float(opt.get('intr_rate2') or 0)
                    if code not in rate_map or rate < rate_map[code]['base']:
                        rate_map[code] = {'base': rate, 'max': rate2}

                count = 0
                for item in base_list:
                    code = item.get('fin_prdt_cd', '')
                    rates = rate_map.get(code, {'base': 0.0, 'max': 0.0})
                    FinanceProduct.objects.update_or_create(
                        product_id=f'FSS_{code}',
                        defaults={
                            'title':       item.get('fin_prdt_nm', '금융상품'),
                            'bank_nm':     item.get('kor_co_nm', ''),
                            'category':    category,
                            'base_rate':   rates['base'],
                            'max_rate':    rates['max'],
                            'target_desc': item.get('loan_inci_expn', ''),
                            'url':         'http://finlife.fss.or.kr/',
                            'is_active':   True,
                            'raw_data':    item,
                        }
                    )
                    count += 1
                self.stdout.write(f'    + FSS {category} {count}건 저장')
            except Exception as e:
                self.stderr.write(f'  ! FSS {category} 오류: {e}')

    def sync_fss_savings(self, key):
        """금감원 적금·예금 상품 수집 → FinanceProduct (청년도약계좌 등 포함)"""
        self.stdout.write('  [금융] 금감원(FSS) 적금/예금 상품 수집...')
        if not key:
            self.stdout.write('    - FSS_FINANCE_KEY 없음, 건너뜀')
            return
        endpoints = [
            ('자유적금',  'http://finlife.fss.or.kr/finlifeapi/savingProductsSearch.json'),
            ('정기예금',  'http://finlife.fss.or.kr/finlifeapi/depositProductsSearch.json'),
        ]
        for category, url in endpoints:
            try:
                res = requests.get(url,
                    params={'auth': key, 'topFinGrpNo': '020000', 'pageNo': 1},
                    headers=self.headers, timeout=15)
                result = res.json().get('result', {})
                base_list   = result.get('baseList', [])
                option_list = result.get('optionList', [])

                # 최고 금리 매핑 (납입기간 12개월 기준 우선)
                rate_map = {}
                for opt in option_list:
                    code  = opt.get('fin_prdt_cd')
                    rate  = float(opt.get('intr_rate') or 0)
                    rate2 = float(opt.get('intr_rate2') or 0)
                    trm   = int(opt.get('save_trm') or 0)
                    # 12개월 기준 우선, 없으면 최고 금리
                    if code not in rate_map or trm == 12 or rate2 > rate_map[code]['max']:
                        rate_map[code] = {'base': rate, 'max': rate2}

                count = 0
                for item in base_list:
                    code  = item.get('fin_prdt_cd', '')
                    rates = rate_map.get(code, {'base': 0.0, 'max': 0.0})
                    title = item.get('fin_prdt_nm', '적금상품')
                    FinanceProduct.objects.update_or_create(
                        product_id=f'FSS_SAV_{code}',
                        defaults={
                            'title':       title,
                            'bank_nm':     item.get('kor_co_nm', ''),
                            'category':    category,
                            'base_rate':   rates['base'],
                            'max_rate':    rates['max'],
                            'target_desc': item.get('spcl_cnd', ''),   # 우대조건
                            'url':         'http://finlife.fss.or.kr/',
                            'is_active':   True,
                            'raw_data':    item,
                        }
                    )
                    count += 1
                self.stdout.write(f'    + FSS {category} {count}건 저장')
            except Exception as e:
                self.stderr.write(f'  ! FSS {category} 오류: {e}')

    def sync_microfinance(self, key):
        """서민금융진흥원 서민금융 상품 수집 → FinanceProduct"""
        self.stdout.write('  [금융] 서민금융진흥원 상품 수집...')
        url = 'http://apis.data.go.kr/B552881/loan-product-info/getLoanProductList'
        try:
            res = requests.get(url, params={'serviceKey': key, 'numOfRows': 50},
                               headers=self.headers, timeout=10)
            root = ET.fromstring(res.content)
            count = 0
            for item in root.findall('.//item'):
                name = item.findtext('loanPrdtNm', '')
                if not name:
                    continue
                FinanceProduct.objects.update_or_create(
                    product_id=f'MICRO_{name[:60]}',
                    defaults={
                        'title':       f'[서민금융] {name}',
                        'bank_nm':     '서민금융진흥원',
                        'category':    '서민금융',
                        'target_desc': item.findtext('loanTgtNm', ''),
                        'base_rate':   float(item.findtext('loanRt', '0') or 0),
                        'url':         'https://www.kinfa.or.kr/',
                        'is_active':   True,
                        'raw_data':    {child.tag: child.text for child in item},
                    }
                )
                count += 1
            self.stdout.write(f'    + 서민금융 {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! 서민금융 오류: {e}')

    # ──────────────────────────────────────────────────────────────────────────
    # 복지 수집
    # ──────────────────────────────────────────────────────────────────────────

    def sync_bokjiro(self, key):
        """복지로 국가복지정보 수집 → WelfareProduct"""
        self.stdout.write('  [복지] 복지로 API 수집...')
        url = 'http://apis.data.go.kr/B554287/NationalWelfareInformationsV001/NationalWelfarelistV001'
        # 001 생활안정, 002 주거, 003 의료, 004 교육, 005 고용, 006 문화, 007 기타
        categories = ['001', '002', '003', '004', '005', '006', '007']
        for code in categories:
            try:
                res = requests.get(url, params={
                    'serviceKey': key, 'callTp': 'L',
                    'srchKeyCode': code, 'numOfRows': 200, 'pageNo': 1
                }, headers=self.headers, timeout=15)
                root = ET.fromstring(res.content)
                count = 0
                for item in root.findall('.//servList'):
                    srv_id = item.findtext('servId', '')
                    if not srv_id:
                        continue
                    title = item.findtext('servNm', '')
                    WelfareProduct.objects.update_or_create(
                        policy_id=f'WELFARE_{srv_id}',
                        defaults={
                            'title':       title,
                            'org_nm':      item.findtext('jurOrgNm', '중앙부처'),
                            'category':    item.findtext('lifeNm', '복지정책'),
                            'benefit_desc': item.findtext('servDtlNm', ''),
                            'target_desc': item.findtext('tgtrNm', ''),
                            'region':      item.findtext('ctpvNm', '전국') or '전국',
                            'url':         item.findtext('servUrl') or 'https://www.bokjiro.go.kr/',
                            # 상시 정책은 notice_date/end_date 미설정 (상시모집 태그 부여)
                            'is_active':   True,
                            'raw_data':    {'xml': ET.tostring(item, encoding='unicode')},
                        }
                    )
                    count += 1
                self.stdout.write(f'    + 복지로 카테고리 {code}: {count}건 저장')
            except Exception as e:
                self.stderr.write(f'  ! 복지로 {code} 오류: {e}')

    def sync_youth_center(self, key):
        """온통청년 청년정책 수집 → WelfareProduct"""
        self.stdout.write('  [복지] 온통청년 정책 수집...')
        if not key:
            self.stdout.write('    - YOUTH_CENTER_KEY 없음, 건너뜀')
            return
        url = 'http://www.youthcenter.go.kr/opi/youthPlcyList.do'
        try:
            res = requests.get(url,
                params={'openApiVlak': key, 'display': 1000, 'pageIndex': 1},
                headers=self.headers, timeout=20)
            root = ET.fromstring(res.content)
            items = root.findall('.//youthPolicy')
            count = 0
            for item in items:
                plcy_id = item.findtext('bizId', '')
                if not plcy_id:
                    continue
                WelfareProduct.objects.update_or_create(
                    policy_id=f'YOUTH_{plcy_id}',
                    defaults={
                        'title':       item.findtext('polyBizSjnm', '청년정책'),
                        'org_nm':      item.findtext('cnsgNmor', '국가'),
                        'category':    item.findtext('polyBizTy', '청년정책'),
                        'benefit_desc': item.findtext('polyItcnCn', ''),
                        'target_desc': item.findtext('ageInfo', ''),
                        'region':      item.findtext('ctpvNm', '전국') or '전국',
                        'end_date':    fmt_date(item.findtext('rqutPrdEnd')),
                        'url':         item.findtext('rfcSiteUrla1') or 'https://www.youthcenter.go.kr/',
                        'is_active':   True,
                        'raw_data':    {child.tag: child.text for child in item},
                    }
                )
                count += 1
            self.stdout.write(f'    + 온통청년 {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! 온통청년 오류: {e}')

    def sync_seoul_youth(self, key):
        """서울청년 몽땅정보통 수집 → WelfareProduct"""
        self.stdout.write('  [복지] 서울청년 정책 수집...')
        url = f'http://openapi.seoul.go.kr:8088/{key}/json/youthPolicy/1/100/'
        try:
            res = requests.get(url, headers=self.headers, timeout=12)
            data = res.json().get('youthPolicy', {}).get('row', [])
            count = 0
            for item in data:
                item_id = item.get('ID', '')
                if not item_id:
                    continue
                WelfareProduct.objects.update_or_create(
                    policy_id=f'SEOUL_{item_id}',
                    defaults={
                        'title':       f"[서울] {item.get('TITLE', '서울 청년정책')}",
                        'org_nm':      '서울특별시',
                        'category':    item.get('CATEGORY', '지역특화'),
                        'benefit_desc': item.get('CONTENT', ''),
                        'target_desc': item.get('TARGET', ''),
                        'region':      '서울',
                        'end_date':    fmt_date(item.get('END_DATE')),
                        'url':         item.get('URL') or 'https://youth.seoul.go.kr/',
                        'is_active':   True,
                        'raw_data':    item,
                    }
                )
                count += 1
            self.stdout.write(f'    + 서울청년 {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! 서울청년 오류: {e}')

    def sync_worknet_programs(self, key):
        """워크넷 취업지원프로그램 수집 → WelfareProduct (채용공고 제외)"""
        self.stdout.write('  [복지] 워크넷 취업지원 프로그램 수집...')
        url = 'http://openapi.work.go.kr/opi/opi/opia/empIdpApi.do'
        try:
            res = requests.get(url,
                params={'authKey': key, 'returnType': 'XML', 'display': 100},
                headers=self.headers, timeout=15)
            root = ET.fromstring(res.content)
            count = 0
            for item in root.findall('.//empIdp'):
                p_id = item.findtext('empIdpNo', '')
                if not p_id:
                    continue
                WelfareProduct.objects.update_or_create(
                    policy_id=f'WORKNET_P_{p_id}',
                    defaults={
                        'title':       f"[취업지원] {item.findtext('empIdpNm', '취업프로그램')}",
                        'org_nm':      item.findtext('instNm', '고용노동부'),
                        'category':    '취업역량강화',
                        'benefit_desc': item.findtext('empIdpCn', ''),
                        'target_desc': item.findtext('tgtrNm', ''),
                        'region':      item.findtext('areaNm', '전국') or '전국',
                        'end_date':    fmt_date(item.findtext('endDt')),
                        'url':         'https://www.work.go.kr/',
                        'is_active':   True,
                        'raw_data':    {child.tag: child.text for child in item},
                    }
                )
                count += 1
            self.stdout.write(f'    + 워크넷 취업지원 {count}건 저장')
        except Exception as e:
            self.stderr.write(f'  ! 워크넷 오류: {e}')

    # ──────────────────────────────────────────────────────────────────────────
    # 마감 데이터 비활성화
    # ──────────────────────────────────────────────────────────────────────────

    def purge_expired(self):
        """종료일이 지난 상품 비활성화 + 주거 유령 데이터(6개월 초과) 비활성화"""
        today = date.today()
        ghost_limit = today - timedelta(days=180)

        h = HousingProduct.objects.filter(end_date__lt=today,  is_active=True).update(is_active=False)
        f = FinanceProduct.objects.filter(end_date__lt=today,  is_active=True).update(is_active=False)
        w = WelfareProduct.objects.filter(end_date__lt=today,  is_active=True).update(is_active=False)
        g = HousingProduct.objects.filter(
            end_date__isnull=True, notice_date__lt=ghost_limit, is_active=True
        ).update(is_active=False)

        self.stdout.write(self.style.WARNING(
            f'  [정화] 비활성화: 주거({h}), 금융({f}), 복지({w}), 주거유령({g})'
        ))

    # ──────────────────────────────────────────────────────────────────────────
    # Firebase 동기화
    # ──────────────────────────────────────────────────────────────────────────

    def push_to_firebase(self):
        self.stdout.write('  [Firebase] 동기화 중...')
        h_data = [{'manage_no': p.manage_no, 'title': p.title, 'region': p.region, 'sales_price': p.sales_price}
                  for p in HousingProduct.objects.filter(is_active=True)]
        FirebaseManager.sync_data('housing_products_v46', h_data, id_field='manage_no')

        f_data = [{'product_id': p.product_id, 'title': p.title, 'bank_nm': p.bank_nm, 'base_rate': p.base_rate}
                  for p in FinanceProduct.objects.filter(is_active=True)]
        FirebaseManager.sync_data('finance_products_v46', f_data, id_field='product_id')

        w_data = [{'policy_id': p.policy_id, 'title': p.title, 'org_nm': p.org_nm}
                  for p in WelfareProduct.objects.filter(is_active=True)]
        FirebaseManager.sync_data('welfare_policies_v46', w_data, id_field='policy_id')

        self.stdout.write(
            f'    + Firebase 동기화 완료: 주거 {len(h_data)}건 / 금융 {len(f_data)}건 / 복지 {len(w_data)}건'
        )
