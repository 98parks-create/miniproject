from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.forms import UserCreationForm, AuthenticationForm
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from .forms import DiagnosticForm
from .models import UserDiagnostic
from .matching_service import MatchingEngine

def index(request):
    """메인 랜딩 페이지"""
    return render(request, 'index.html')

def diagnose(request):
    """지능형 진단 폼 처리"""
    if request.method == 'POST':
        form = DiagnosticForm(request.POST)
        if form.is_valid():
            # 데이터 영구 저장 및 분석 실행
            diagnostic = form.save(commit=False)
            if request.user.is_authenticated:
                diagnostic.user = request.user
            diagnostic.save()
            
            # 분석 결과 페이지로 즉시 이동
            return redirect('result', pk=diagnostic.pk)
    else:
        form = DiagnosticForm()
    
    return render(request, 'diagnose.html', {'form': form})

def result(request, pk):
    """지능형 분석 결과 리포트 (대시보드)"""
    diagnostic = get_object_or_404(UserDiagnostic, pk=pk)
    
    try:
        # 지능형 하이브리드 매칭 엔진 호출
        report_data = MatchingEngine.get_full_report(diagnostic)
    except Exception as e:
        print(f"Result View Error: {e}")
        # 오류 발생 시 방어적 기본 데이터 생성
        report_data = {
            "housing": MatchingEngine.get_default_item("주거"),
            "finance": MatchingEngine.get_default_item("금융"),
            "welfare": MatchingEngine.get_default_item("복지"),
            "user_summary": {"total_income": 0, "assets": 0, "debt": 0},
            "chart_data": {"assets": 0, "debt": 0, "projected_loan": 0}
        }
    
    context = {
        'diagnostic': diagnostic,
        'report': report_data
    }
    return render(request, 'result.html', context)

# 인증 및 회원 관리 (Auth System)
def signup(request):
    """프리미엄 회원가입"""
    if request.method == 'POST':
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect('index')
    else:
        form = UserCreationForm()
    return render(request, 'signup.html', {'form': form})

def login_view(request):
    """프리미엄 로그인"""
    if request.method == 'POST':
        form = AuthenticationForm(data=request.POST)
        if form.is_valid():
            user = form.get_user()
            login(request, user)
            return redirect('index')
    else:
        form = AuthenticationForm()
    return render(request, 'login.html', {'form': form})

def logout_view(request):
    """세션 로그아웃"""
    logout(request)
    return redirect('index')

@login_required
def my_reports(request):
    """지능형 내 리포트 필터링 보관함"""
    user_reports = UserDiagnostic.objects.filter(user=request.user)
    
    # 지역별 필터링
    region_filter = request.GET.get('region')
    if region_filter:
        user_reports = user_reports.filter(region=region_filter)
        
    # 날짜별 필터링 (생성일 기준)
    date_filter = request.GET.get('date')
    if date_filter:
        user_reports = user_reports.filter(created_at__date=date_filter)
        
    context = {
        'reports': user_reports.order_by('-created_at'),
        'regions': UserDiagnostic.REGION_CHOICES,
        'current_region': region_filter,
        'current_date': date_filter
    }
    return render(request, 'my_reports.html', context)
