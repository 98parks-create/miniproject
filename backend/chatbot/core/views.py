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
            
            if request.user.is_authenticated:
                p = request.user.userprofile
                user_data = {
                    'name': p.name or request.user.username,
                    'age': p.age,
                    'income': p.income,
                    'region': p.region
                }
            
            # 🧭 안티그래비티 v15 초정밀 엔진 리포트 추출
            report_data = None
            try:
                # 1순위: youth_road 진단 데이터 확인
                diagnostic = UserDiagnostic.objects.filter(user=request.user).first() if request.user.is_authenticated else None
                if not diagnostic:
                    # 2순위: 진단 데이터가 없으면 chatbot 프로필로 임시 생성
                    diagnostic = UserDiagnostic(
                        age=user_data['age'],
                        total_income=user_data['income'],
                        region=user_data['region'],
                        assets=5000, # 기본값
                        debt=0
                    )
                report_data = MatchingEngine.get_full_report(diagnostic)
            except Exception as e:
                print(f"Engine integration error: {e}")

            reply = ask_expert_ai(user_msg, user_data, report_data)
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
        profile.save()
        return redirect('index')
    return redirect('index')
