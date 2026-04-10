from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
import json
from .services import get_all_policies, ask_expert_ai, generate_expert_report
from .models import UserProfile
from youth_road.models import UserDiagnostic
from youth_road.matching_service import MatchingEngine

def index(request):
    """SPA Main Entry Point"""
    return render(request, 'chatbot/index.html')

def match_policies(request):
    """전체 정책 매칭 API"""
    user_data = {}
    if request.user.is_authenticated:
        profile = request.user.userprofile
        user_data = {'age': profile.age, 'income': profile.income, 'region': profile.region}
    
    policies = get_all_policies(user_data)
    return JsonResponse({'policies': policies})

@csrf_exempt
def chat_gemini(request):
    """전문가 AI 채팅 API"""
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            user_msg = body.get('message', '')
            user_data = {'name': '방문자', 'age': 29, 'income': 3000, 'region': 'seoul'}
            
            user_api_key = None
            if request.user.is_authenticated:
                p = request.user.userprofile
                user_data = {
                    'name': p.name or request.user.username,
                    'age': p.age,
                    'income': p.income,
                    'region': p.region
                }
                user_api_key = p.personal_api_key

            # 🧭 안티그래비티 v20 초정밀 엔진 진단 데이터 추출 강화
            report_data = None
            diagnostic = None
            try:
                if request.user.is_authenticated:
                    # 1순위: 로그인 사용자 최신 진단
                    diagnostic = request.user.diagnostics.order_by('-created_at').first()
                
                # 2순위: 세션에 저장된 최신 진단 (익명 사용자용)
                if not diagnostic:
                    latest_pk = request.session.get('latest_diagnostic_pk')
                    if latest_pk:
                        diagnostic = UserDiagnostic.objects.filter(pk=latest_pk).first()

                if diagnostic:
                    # 진단 데이터가 있으면 user_data를 실제 값으로 업데이트
                    user_data.update({
                        'age': diagnostic.age,
                        'income': diagnostic.total_income,
                        'region': diagnostic.get_region_display(),
                        'sub_region': diagnostic.sub_region or "전체",
                        'marital': diagnostic.get_marital_status_display(),
                        'assets': diagnostic.assets,
                        'debt': diagnostic.debt,
                        'subscription': f"{diagnostic.subscription_count}회 ({diagnostic.subscription_amount}만원)"
                    })
                    report_data = MatchingEngine.get_full_report(diagnostic)
                else:
                    # 3순위: 진단 데이터가 아예 없으면 기본 프로필 기반 mock 생성
                    mock_diag = UserDiagnostic(
                        age=user_data['age'],
                        total_income=user_data['income'],
                        region='Seoul',
                        sub_region='강남구',
                        assets=5000,
                        debt=0
                    )
                    user_data['sub_region'] = '강남구'
                    report_data = MatchingEngine.get_full_report(mock_diag)
            except Exception as e:
                print(f"Engine integration error: {e}")

            # 🧠 [Contextual Memory] 세션 기반 대화 이력 관리
            chat_history = request.session.get('chat_history', [])
            
            # 대화 이력을 포함하여 AI에게 답변 요청
            reply = ask_expert_ai(user_msg, user_data, report_data, user_api_key=user_api_key, history=chat_history)
            
            # 새로운 대화 내용 저장 (사용자 질문 + AI 응답)
            chat_history.append({"role": "user", "parts": [{"text": user_msg}]})
            chat_history.append({"role": "model", "parts": [{"text": reply}]})
            
            # 최근 10개 대화 내용만 유지 (토큰 관리 및 성능 최적화)
            request.session['chat_history'] = chat_history[-10:]
            request.session.modified = True

            return JsonResponse({'reply': reply})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)
    return JsonResponse({'error': 'Invalid request'}, status=400)

@login_required
@csrf_exempt
def get_ai_report(request):
    """정밀 진단 AI 보고서 API"""
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            top_matches = body.get('top_matches', [])
            p = request.user.userprofile
            user_data = {
                'name': p.name or request.user.username,
                'age': p.age,
                'income': p.income,
                'region': p.region
            }
            
            report = generate_expert_report(user_data, top_matches)
            return JsonResponse({'report': report})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)
    return JsonResponse({'error': 'Invalid request'}, status=400)

@login_required
def update_profile(request):
    """사용자 프로필 실시간 업데이트"""
    if request.method == 'POST':
        profile = request.user.userprofile
        profile.name = request.POST.get('name', profile.name)
        profile.age = int(request.POST.get('age', profile.age))
        profile.income = int(request.POST.get('income', profile.income))
        profile.region = request.POST.get('region', profile.region)
        profile.sub_region = request.POST.get('sub_region', profile.sub_region)
        profile.personal_api_key = request.POST.get('personal_api_key', profile.personal_api_key)
        profile.save()
        return redirect('index')
    return redirect('index')
