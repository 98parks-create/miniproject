from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('youth_road.urls')), # 🧭 루트로 즉시 연결
    path('chatbot/', include('chatbot.config.urls')), # 🤖 지능형 챗봇 프로젝트 통합
]
