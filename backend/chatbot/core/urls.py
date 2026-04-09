from django.urls import path
from . import views, auth_views
from django.contrib.auth import views as auth_login_views

app_name = 'chatbot'

urlpatterns = [
    path('', views.index, name='chat_view'),
    path('api/policies/', views.match_policies, name='match_policies'),
    path('api/chat/', views.chat_gemini, name='chat_gemini'),
    path('api/ai-report/', views.get_ai_report, name='ai_report'),
    
    # Auth & Profile
    path('accounts/login/', auth_login_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
    path('accounts/logout/', auth_login_views.LogoutView.as_view(next_page='/'), name='logout'),
    path('accounts/signup/', auth_views.signup, name='signup'),
    path('accounts/profile/update/', views.update_profile, name='update_profile'),
]
