const VisualDashboard = () => {
    return (
        <section style={{ paddingBottom: '100px' }}>
            <div style={{ marginBottom: '40px' }}>
                <h3 style={{ fontSize: '24px', fontWeight: 900, color: '#1e293b' }}>지능형 데이터 시나리오 📊</h3>
                <p style={{ fontSize: '14px', color: '#64748b' }}>청춘로 AI 엔진이 도출한 현재 위치 기반의 정책 매칭 결과입니다.</p>
            </div>

            {/* 2-Column Standard Grid: Perfect symmetry */}
            <div className="standard-grid" style={{ marginBottom: '40px' }}>
                
                {/* 3D Map Visualization Card */}
                <div className="premium-card" style={{ height: '400px', display: 'flex', flexDirection: 'column' }}>
                    <div className="flex-between" style={{ marginBottom: '20px' }}>
                        <span style={{ fontSize: '14px', fontWeight: 800, color: '#3b82f6' }}>현위치 기반 주거 혜택 지도</span>
                        <span style={{ fontSize: '11px', padding: '4px 10px', background: 'rgba(59, 130, 246, 0.1)', color: '#3b82f6', borderRadius: '50px', fontWeight: 700 }}>LIVE MATCHING</span>
                    </div>
                    <div style={{ flex: 1, position: 'relative', overflow: 'hidden', background: 'rgba(0,0,0,0.02)', borderRadius: '20px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                         {/* Stylized Map View */}
                         <div style={{ width: '80%', height: '80%', border: '2px solid rgba(59, 130, 246, 0.1)', transform: 'rotateX(50deg) rotateZ(-10deg)', borderRadius: '20px', position: 'relative' }}>
                             <div style={{ position: 'absolute', top: '30%', left: '40%', width: '12px', height: '120px', background: 'linear-gradient(to top, #3b82f6, transparent)', transform: 'translateY(-100%)', borderRadius: '2px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                                 <div style={{ width: '10px', height: '10px', background: '#3b82f6', borderRadius: '50%', boxShadow: '0 0 20px #3b82f6' }}></div>
                             </div>
                             <div style={{ position: 'absolute', bottom: '15px', right: '15px', color: '#3b82f6', fontSize: '9px', fontWeight: 900 }}>3D AREA SIMULATION</div>
                         </div>
                    </div>
                </div>

                {/* Radar Chart Layout View */}
                <div className="premium-card" style={{ height: '400px', display: 'flex', flexDirection: 'column' }}>
                    <div className="flex-between" style={{ marginBottom: '20px' }}>
                        <span style={{ fontSize: '14px', fontWeight: 800, color: '#10b981' }}>금융 금리 시뮬레이션</span>
                        <span style={{ fontSize: '11px', padding: '4px 10px', background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', borderRadius: '50px', fontWeight: 700 }}>AI OPTIMIZED</span>
                    </div>
                    <div className="flex-center" style={{ flex: 1 }}>
                        <svg width="220" height="220" viewBox="0 0 240 240">
                            <circle cx="120" cy="120" r="100" fill="none" stroke="rgba(0,0,0,0.05)" strokeDasharray="4 4" />
                            <circle cx="120" cy="120" r="60" fill="none" stroke="rgba(0,0,0,0.05)" strokeDasharray="4 4" />
                            <polygon points="120,40 190,120 120,200 50,120" fill="rgba(16, 185, 129, 0.15)" stroke="#10b981" strokeWidth="2" />
                            <polygon points="120,70 170,120 120,170 80,120" fill="rgba(59, 130, 246, 0.15)" stroke="#3b82f6" strokeWidth="2" />
                        </svg>
                    </div>
                    <div style={{ display: 'flex', gap: '20px', fontSize: '11px', fontWeight: 700, color: '#64748b' }}>
                        <span>● 신규 정책 금리</span>
                        <span style={{ color: '#3b82f6' }}>● 기존 은행 금리</span>
                    </div>
                </div>
            </div>

            {/* Notification Row: Horizontal scrollable feed */}
            <div style={{ display: 'flex', gap: '20px', overflowX: 'auto', paddingBottom: '10px' }}>
                {[
                    { type: '긴급', text: '동작구 신혼희망타운 잔여세대 긴급 접수 시작 (선착순)', color: '#ef4444' },
                    { type: '추천', text: '가구 합산 소득 기준 완화로 인한 청년 자금 대출 확대', color: '#10b981' },
                    { type: '공지', text: '부모급여 인상안 국회 본회의 최종 통과 내용 확인하기', color: '#3b82f6' }
                ].map((item, i) => (
                    <div key={i} className="premium-card" style={{ padding: '16px 24px', whiteSpace: 'nowrap', display: 'flex', alignItems: 'center', gap: '12px', minWidth: '400px' }}>
                        <span style={{ fontSize: '11px', padding: '2px 8px', borderRadius: '4px', background: `${item.color}1a`, color: item.color, fontWeight: 900 }}>{item.type}</span>
                        <p style={{ fontSize: '13px', fontWeight: 700, color: '#1e293b' }}>{item.text}</p>
                    </div>
                ))}
            </div>
        </section>
    );
};

export default VisualDashboard;
