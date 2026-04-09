import { motion } from 'framer-motion';

const Hero = ({ onStart }) => {
    return (
        <div style={{ width: '100%' }}>
            <section style={{ textAlign: 'center', padding: '120px 0 100px 0', position: 'relative' }}>
                {/* Main Typo: Center Aligned & Premium */}
                <div style={{ maxWidth: '900px', margin: '0 auto' }}>
                    <motion.p 
                    initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}
                    style={{ fontSize: '14px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '3px', marginBottom: '24px', textTransform: 'uppercase' }}
                    >
                    NEW GENERATION POLICY SECRETARY
                    </motion.p>
                    <motion.h2 
                    initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1 }}
                    style={{ fontSize: '56px', fontWeight: 900, color: 'var(--text-dark)', marginBottom: '40px', lineHeight: 1.2, letterSpacing: '-2px' }}
                    >
                        당신이 놓치고 있는 <br/>
                        <span style={{ fontSize: '84px', fontWeight: 900, background: 'var(--primary-gradient)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', letterSpacing: '-4px' }}>평균 4,500만 원</span> 의 혜택,<br/>
                        <span style={{ position: 'relative', display: 'inline-block', padding: '0 15px', marginTop: '10px' }}>
                            1분 만에 찾아드려요.
                            <div style={{ position: 'absolute', bottom: '12px', left: 0, width: '100%', height: '20px', background: 'rgba(45, 244, 192, 0.3)', zIndex: -1 }}></div>
                        </span>
                    </motion.h2>
                    
                    <motion.p 
                    initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.3 }}
                    style={{ fontSize: '18px', color: 'var(--text-muted)', fontWeight: 500, maxWidth: '650px', margin: '0 auto 50px', lineHeight: 1.7 }}
                    >
                        청춘로의 독자적인 정책 데이터 엔진이 현재 거주지, 소득, 혼인 여부를 분석하여<br/>
                        오직 당신만을 위한 맞춤형 주거/금융/복지 정보를 실시간으로 추출합니다.
                    </motion.p>

                    <motion.button 
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={onStart}
                    className="btn-matching"
                    style={{ fontSize: '22px', padding: '24px 60px', borderRadius: '60px' }}
                    >
                    지금 바로 나의 정책 혜택 진단하기 🧭
                    </motion.button>
                </div>
            </section>

            {/* Identity Section: Scrollable & Data-Driven Explanation */}
            <section style={{ padding: '100px 20px', maxWidth: '1200px', margin: '0 auto' }}>
                <div style={{ textAlign: 'center', marginBottom: '80px' }}>
                    <h3 style={{ fontSize: '32px', fontWeight: 900, color: 'var(--text-dark)' }}>왜 청춘로인가요?</h3>
                    <p style={{ color: 'var(--text-muted)', marginTop: '12px', fontSize: '16px' }}>Data-Driven 정책 매칭 엔진의 압도적 정교함을 경험하세요.</p>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '30px' }}>
                    {[
                        { title: '💰 정밀 금융 시뮬레이션', desc: 'DSR/LTV를 반영한 최대 대출 한도와 금리 절감액을 실시간으로 계산합니다.', icon: '🏦' },
                        { title: '🏠 주거 통합 내비게이터', desc: '청약홈 및 LH/SH API를 연동하여 내 가점 대비 당첨 확률을 미터기로 시각화합니다.', icon: '📍' },
                        { title: '🎁 복지 스트림 분석', desc: '월별 수혜 금액 흐름을 물결 모양의 스트림그래프로 실시간 카운팅합니다.', icon: '🎈' }
                    ].map((item, idx) => (
                        <motion.div 
                            key={idx}
                            whileHover={{ y: -10 }}
                            className="glass-panel"
                            style={{ padding: '40px', textAlign: 'left', display: 'flex', flexDirection: 'column', gap: '20px' }}
                        >
                            <div style={{ fontSize: '40px' }}>{item.icon}</div>
                            <h4 style={{ fontSize: '20px', fontWeight: 800 }}>{item.title}</h4>
                            <p style={{ fontSize: '14px', color: 'var(--text-muted)', lineHeight: 1.6 }}>{item.desc}</p>
                        </motion.div>
                    ))}
                </div>
            </section>
        </div>
    );
};

export default Hero;
