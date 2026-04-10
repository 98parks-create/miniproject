from django.urls import path
from . import views

urlpatterns = [
    path('index_portal/', views.index, name='portal_index'),
    path('youth/', views.youth_home, name='youth_home'),
    path('newlywed/', views.newlywed_home, name='newlywed_home'),
    path('login/', views.login_view, name='login'),
    path('login/id/', views.id_login_view, name='id_login'),
    path('login/qr/', views.qr_login_view, name='qr_login'),
    path('login/guest/', views.guest_login_view, name='guest_login'),
    path('register/', views.register_step1, name='register_step1'),
    path('register/step2/', views.register_step2, name='register_step2'),
    path('check-id/', views.check_id, name='check_id'),
    path('logout/', views.logout_view, name='logout'),
]
