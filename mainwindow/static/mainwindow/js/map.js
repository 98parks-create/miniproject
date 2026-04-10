function searchRegion() {
    const input = document.getElementById('region-search-input');
    const keyword = input ? input.value.trim() : '';

    if (!keyword) {
        // 입력값이 없으면 입력창을 흔들어서 알려줌
        input.classList.add('input-shake');
        input.placeholder = '지역명을 입력해주세요! (예: 성남, 서울, 수원)';
        setTimeout(() => {
            input.classList.remove('input-shake');
            input.placeholder = '내가 찾는 지역 정보를 알아보세요!';
        }, 1500);
        return;
    }

    // 카카오맵에서 "{입력값} 주거복지센터" 검색
    const searchQuery = encodeURIComponent(keyword + ' 주거복지센터');
    const kakaoMapUrl = `https://map.kakao.com/link/search/${searchQuery}`;
    window.open(kakaoMapUrl, '_blank');

    console.log(`[지역 검색] "${keyword} 주거복지센터" 카카오맵 검색 실행`);
}

// 기능 2: 지도찾기 버튼 (메인 카드)
document.addEventListener('DOMContentLoaded', () => {
    const btn = document.getElementById('btn-map-search');
    if (btn) {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            const searchUrl = 'https://map.kakao.com/link/search/주거복지센터';
            window.open(searchUrl, '_blank');
            console.log('[지도 서비스] 카카오맵으로 연결되었습니다.');
        });
    }
});