from django.shortcuts import render, redirect
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login, logout
from django.http import JsonResponse
from django.contrib import messages

# 아이디 중복 확인 함수
def check_id(request):
    username = request.GET.get('username', None)
    data = {
        'is_taken': User.objects.filter(username__iexact=username).exists()
    }
    return JsonResponse(data)

def index(request):
    return render(request, 'policyapp/index.html')

def youth_home(request):
    return render(request, 'policyapp/youth_page.html')

def newlywed_home(request):
    return render(request, 'policyapp/newlywed_page.html')

def login_view(request):
    return render(request, 'policyapp/login.html')

def id_login_view(request):
    if request.method == "POST":
        uid = request.POST.get('username')
        upw = request.POST.get('password')
        user = authenticate(request, username=uid, password=upw)
        if user is not None:
            login(request, user)
            return redirect('portal_index')
        else:
            return render(request, 'policyapp/id_login.html', {'error': '아이디 또는 비밀번호가 올바르지 않습니다.'})
    return render(request, 'policyapp/id_login.html')

def qr_login_view(request):
    return render(request, 'policyapp/qr_login.html')

def guest_login_view(request):
    # 비회원 로그인은 세션에 표시하거나 익명 처리를 할 수 있음
    return render(request, 'policyapp/guest_login.html')

def register_step1(request):
    if request.method == "POST":
        term1 = request.POST.get('term1')
        term2 = request.POST.get('term2')
        term_sub1 = request.POST.get('term_sub1')

        if term1 and term2 and term_sub1:
            return redirect('register_step2')
        else:
            return render(request, 'policyapp/register_step1.html', {'error': '필수 약관에 동의해주세요.'})
            
    return render(request, 'policyapp/register_step1.html')

def register_step2(request):
    if request.method == "POST":
        uid = request.POST.get('username')
        upw = request.POST.get('password')
        uname = request.POST.get('name')
        
        if User.objects.filter(username=uid).exists():
            return render(request, 'policyapp/register_step2.html', {'error': '이미 존재하는 아이디입니다.'})
        
        # 유저 생성 및 저장
        User.objects.create_user(username=uid, password=upw, last_name=uname)
        messages.success(request, '회원가입이 완료되었습니다. 로그인해주세요.')
        return redirect('login')
        
    return render(request, 'policyapp/register_step2.html')

def logout_view(request):
    logout(request)
    return redirect('home')