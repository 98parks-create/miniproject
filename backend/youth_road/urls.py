from django.urls import path
from . import views

# 🧭 청춘로(路) 순수 장고 뷰 주소 체계
urlpatterns = [
    path('', views.index, name='index'), # 랜딩 페이지
    path('diagnose/', views.diagnose, name='diagnose'), # 지능형 진단
    path('result/<int:pk>/', views.result, name='result'), # 분석 결과 대시보드
    
    # 🔐 인증 및 회원 관리 (Auth System)
    path('signup/', views.signup, name='signup'),
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('my-reports/', views.my_reports, name='my_reports'), # 내 리포트 보관함
]
