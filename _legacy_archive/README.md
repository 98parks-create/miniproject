# 🤖 AI 정책 전문가 챗봇 (Standalone)

이 프로젝트는 '청춘로' 서비스에서 **챗봇 기능만** 따로 떼어낸 독립형 Django 프로젝트입니다. 
상대방에게 챗봇 기능만 가볍게 전달하고 싶을 때 사용하세요.

## 🚀 시작하기

1. **가상환경 설정 및 라이브러리 설치**:
   ```bash
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **API 키 설정**:
   - `.env.example` 파일을 복사하여 `.env` 파일을 만듭니다.
   - `GEMINI_API_KEY` 항목에 본인의 Gemini API 키를 입력합니다.

3. **데이터베이스 초기화**:
   ```bash
   python manage.py migrate
   ```

4. **서버 실행**:
   ```bash
   python manage.py runserver
   ```
   브라우저에서 `http://127.0.0.1:8000`으로 접속하면 하단 우측에 챗봇 버튼이 나타납니다.

## ⚙️ 관리 및 설정

- **AI 인격 수정**: 어드민 페이지(`127.0.0.1:8000/admin`)의 **User Profiles**에서 AI의 페르소나를 변경할 수 있습니다.
- **참고 데이터**: AI가 답변 시 참고할 정책 데이터는 어드민의 **Policies**에서 추가 및 수정이 가능합니다.

---
© 2026 AI Chatbot Module
