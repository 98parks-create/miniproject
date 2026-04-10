from django.shortcuts import render

def home(request):
    context = {
        'title': '홈페이지',
    }
    return render(request, 'mainwindow/home.html', context)
