# 🏛️ Ultimate AI 정책 비서 (Expert AI Policy Advisor)

사용자의 프로필을 기반으로 최적화된 정부 지원 정책을 추천하고, 전문적인 AI 상담을 제공하는 지능형 정책 비서 서비스입니다.

## 🚀 주요 기능
- **정밀 정책 진단**: 나이, 소득, 거주지 정보를 바탕으로 0.1점 단위의 적합도 산출
- **6대 카테고리 지원**: 금융, 주택, 복지, 고용, 법률, 청년특허 등 전방위 상담
- **전문가 AI 챗봇**: Gemini AI를 활용한 1:1 맞춤형 정책 상담

## 🛠️ 초기 설정 (Setup)

### 1. 가상 환경 설정 및 패키지 설치
이 프로젝트는 파이썬(Python 3.10+) 환경에서 실행됩니다.
```bash
# 가상 환경 생성
python -m venv venv

# 가상 환경 활성화 (Windows)
call venv\Scripts\activate
# 가상 환경 활성화 (Mac/Linux)
source venv/bin/activate

# 필수 라이브러리 설치
pip install -r requirements.txt
```

### 2. API 키 설정
`.env.example` 파일을 복사하여 `.env` 파일을 생성한 후, 본인의 API 키를 입력하세요.
```bash
# 파일 복사
cp .env.example .env
```
그 후 `.env` 파일을 열어 `GOOGLE_API_KEY` 항목에 본인의 Gemini API 키를 넣습니다.

### 3. 데이터베이스 초기화
데이터베이스 테이블을 생성합니다.
```bash
python manage.py migrate
```

## 🏃 실행 방법 (Run)
서버를 실행하고 브라우저에서 접속합니다.
```bash
python manage.py runserver
```
접속 주소: `http://127.0.0.1:8000/`

---
Copyright © 2026 Ultimate AI Team. All rights reserved.
