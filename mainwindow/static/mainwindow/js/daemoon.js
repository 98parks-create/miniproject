// static/mainwindow/js/slider.js
let currentSlide = 0;

function moveSlider(direction) {
    const track = document.getElementById('sliderTrack');
    // 현재 페이지에 있는 'slide' 클래스 개수를 자동으로 셉니다 (지금은 4개겠죠?)
    const slides = document.querySelectorAll('.slide');
    const totalSlides = slides.length; 

    if (totalSlides === 0) return; // 슬라이드가 없으면 실행 안 함

    currentSlide += direction;

    // 무한 루프 로직
    if (currentSlide < 0) {
        currentSlide = totalSlides - 1; // 첫 장에서 이전 누르면 마지막 장으로
    } else if (currentSlide >= totalSlides) {
        currentSlide = 0; // 마지막 장에서 다음 누르면 첫 장으로
    }

    // 슬라이드 이동 시각화
    track.style.transform = `translateX(-${currentSlide * 100}%)`;
}