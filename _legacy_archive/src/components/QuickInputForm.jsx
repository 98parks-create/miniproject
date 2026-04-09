const QuickInputForm = () => {
    return (
        <section style={{ padding: '60px 0' }}>
            {/* Standard Grid Layout: No overlaps, purely aligned */}
            <div className="standard-grid">
                
                {/* Age Input Card */}
                <div className="premium-card">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
                        <div style={{ width: '60px', height: '60px', borderRadius: '50%', background: '#eff6ff', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '24px' }}>🎂</div>
                        <div>
                            <label style={{ fontSize: '11px', fontWeight: 800, color: '#64748b', textTransform: 'uppercase' }}>만 나이</label>
                            <input type="number" defaultValue="28" style={{ display: 'block', width: '100%', fontSize: '28px', fontWeight: 900, color: '#3b82f6', border: 'none', background: 'transparent', outline: 'none' }} />
                        </div>
                    </div>
                </div>

                {/* Region Selection Card */}
                <div className="premium-card">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
                        <div style={{ width: '60px', height: '60px', borderRadius: '50%', background: '#f5f3ff', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '24px' }}>📍</div>
                        <div>
                            <label style={{ fontSize: '11px', fontWeight: 800, color: '#64748b', textTransform: 'uppercase' }}>현재 거주지</label>
                            <select style={{ display: 'block', width: '100%', fontSize: '20px', fontWeight: 800, color: '#1e293b', border: 'none', background: 'transparent', outline: 'none' }}>
                                <option>서울특별시</option>
                                <option>경기도</option>
                                <option>인천광역시</option>
                            </select>
                        </div>
                    </div>
                </div>

                {/* Income Card */}
                <div className="premium-card">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
                        <div style={{ width: '60px', height: '60px', borderRadius: '50%', background: '#f0fdf4', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '24px' }}>💰</div>
                        <div style={{ flex: 1 }}>
                            <label style={{ fontSize: '11px', fontWeight: 800, color: '#64748b', textTransform: 'uppercase' }}>가구 합산 연소득</label>
                            <div style={{ display: 'flex', alignItems: 'baseline', gap: '5px' }}>
                                <input type="number" defaultValue="5500" style={{ width: '100px', fontSize: '28px', fontWeight: 900, color: '#1e293b', border: 'none', background: 'transparent', outline: 'none' }} />
                                <span style={{ fontSize: '14px', fontWeight: 300, color: '#64748b' }}>만원</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Main Action Button: Centered & Wide */}
            <div style={{ display: 'flex', justifyContent: 'center', marginTop: '40px' }}>
                <button style={{ width: '100%', maxWidth: '1120px', height: '80px', borderRadius: '24px', background: 'linear-gradient(90deg, #3b82f6, #4f46e5)', color: 'white', border: 'none', cursor: 'pointer', boxShadow: '0 20px 40px rgba(37, 99, 235, 0.2)', transition: 'all 0.3s ease', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '15px' }}>
                    <span style={{ fontSize: '22px', fontWeight: 900, letterSpacing: '1px' }}>나의 혜택 실시간 매칭 시작 🧭</span>
                    <div style={{ width: '1px', height: '30px', background: 'rgba(255, 255, 255, 0.2)' }}></div>
                    <span style={{ fontSize: '13px', fontWeight: 300, opacity: 0.8 }}>청춘로 AI 분석 15.2 ms</span>
                </button>
            </div>
        </section>
    );
};

export default QuickInputForm;
