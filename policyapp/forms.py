from django import forms

class RegistrationForm(forms.Form):
    USER_TYPE_CHOICES = [('individual', '개인'), ('corporate', '법인')]
    user_type = forms.ChoiceField(choices=USER_TYPE_CHOICES, widget=forms.RadioSelect)
    
    name = forms.CharField(max_length=100, label="이름 (필수)")
    resident_num_front = forms.CharField(max_length=6)
    resident_num_back = forms.CharField(max_length=7)
    
    username = forms.CharField(max_length=16, label="아이디 (필수)")
    password = forms.CharField(widget=forms.PasswordInput, label="비밀번호 (필수)")
    password_confirm = forms.CharField(widget=forms.PasswordInput, label="비밀번호 확인 (필수)")
    
    auth_method = forms.ChoiceField(
        choices=[('simple', '간편인증'), ('pass', 'PASS(문자)인증')],
        widget=forms.RadioSelect
    )