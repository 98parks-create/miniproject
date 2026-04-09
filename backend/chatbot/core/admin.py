from django.contrib import admin
from .models import Policy, UserProfile

@admin.register(Policy)
class PolicyAdmin(admin.ModelAdmin):
    list_display = ('title', 'category', 'age_max', 'income_limit', 'requires_kids', 'created_at')
    list_filter = ('category', 'requires_kids')
    search_fields = ('title', 'summary')

@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ('name', 'age', 'income', 'region', 'created_at')
    search_fields = ('name',)
