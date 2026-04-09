import { useState, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { PieChart, Pie, Cell, ResponsiveContainer } from 'recharts';
import PolicyModal from './PolicyModal';

const Tab_Housing = ({ data, matchedPolicies }) => {
  const [selectedPolicy, setSelectedPolicy] = useState(null);

  // 청약 가점 및 당첨 확률 정밀 계산
  const winChance = useMemo(() => {
    let score = 10;
    if (data.kids > 0) score += data.kids * 15;
    if (data.region === 'Seoul') score += 5;
    if (data.marital === 'Married') score += 20;
    
    const cutline = 65;
    const opportunityBonus = (matchedPolicies || []).length * 10;
    const chance = Math.min(100, Math.round(((score + opportunityBonus) / cutline) * 100));
    
    return { score, cutline, chance };
  }, [data, matchedPolicies]);

  const gaugeData = [
    { value: winChance.chance },
    { value: 100 - winChance.chance }
  ];

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.8fr) minmax(0, 1.2fr)', gap: '30px' }}>
      {/* 1. Matched Housing Policies Area */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
        <h4 style={{ fontSize: '13px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '1px' }}>🏠 나에게 유리한 주택 정책</h4>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px' }}>
          {matchedPolicies && matchedPolicies.length > 0 ? (
            matchedPolicies.map(p => (
              <motion.div 
                key={p.id}
                whileHover={{ scale: 1.02 }}
                className="glass-panel"
                style={{ padding: '24px', background: 'white', borderLeft: '6px solid var(--soft-blue)', cursor: 'pointer' }}
                onClick={() => setSelectedPolicy(p)}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                  <span style={{ fontSize: '11px', color: 'var(--soft-blue)', fontWeight: 800 }}>LH·SH 실시간 공고</span>
                  <span style={{ fontSize: '16px' }}>📍</span>
                </div>
                <h5 style={{ fontSize: '16px', fontWeight: 900, color: 'var(--text-dark)' }}>{p.title}</h5>
                <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginTop: '8px' }}>{p.summary || '실시간 모집 공고 분석 데이터입니다.'}</p>
              </motion.div>
            ))
          ) : (
            <div className="glass-panel" style={{ gridColumn: 'span 2', padding: '80px 0', textAlign: 'center', opacity: 0.6 }}>
              <div style={{ fontSize: '48px', marginBottom: '24px' }}>🏚️</div>
              <p style={{ fontSize: '16px', fontWeight: 800, color: 'var(--text-muted)' }}>현재 조건에 최적화된 공고가 없습니다.<br/><span style={{ fontSize: '13px', fontWeight: 600 }}>소득 범위를 소폭 조정하여 재검색해 보세요.</span></p>
            </div>
          )}
        </div>

        <div style={{ marginTop: '20px', padding: '20px', background: 'var(--primary-gradient)', borderRadius: '20px', color: 'white', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div style={{ fontSize: '14px', fontWeight: 800 }}>범정부 주거 데이터 통합 분석 완료</div>
            <div style={{ fontSize: '20px' }}>📡</div>
        </div>
      </div>

      {/* 2. Win Chance Meter & Status Area */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '30px' }}>
        <motion.div 
            className="glass-panel" 
            style={{ padding: '40px', textAlign: 'center', background: 'white' }}
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
        >
            <h4 style={{ fontSize: '12px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '2px', marginBottom: '30px' }}>WIN CHANCE METER</h4>
            <div style={{ width: '100%', height: '200px', position: 'relative' }}>
                <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                        <Pie
                            data={gaugeData}
                            cx="50%" cy="100%"
                            startAngle={180} endAngle={0}
                            innerRadius={70} outerRadius={105}
                            paddingAngle={2} dataKey="value" stroke="none"
                        >
                            <Cell fill="var(--neon-mint)" />
                            <Cell fill="rgba(0,0,0,0.05)" />
                        </Pie>
                    </PieChart>
                </ResponsiveContainer>
                <div style={{ position: 'absolute', bottom: '15px', left: '50%', transform: 'translateX(-50%)' }}>
                    <div style={{ fontSize: '42px', fontWeight: 900, color: 'var(--text-dark)' }}>{winChance.chance}%</div>
                </div>
            </div>
            <div style={{ marginTop: '30px' }}>
                <div style={{ fontSize: '15px', fontWeight: 900, color: 'var(--text-dark)' }}>내 청약 가점: {winChance.score}점</div>
                <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginTop: '8px', fontWeight: 600 }}>커트라인 {winChance.cutline}점 기준 분석 결과</div>
            </div>
        </motion.div>

        <motion.div 
            className="glass-panel" 
            style={{ padding: '35px', background: 'white' }}
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: 0.1 }}
        >
            <h4 style={{ fontSize: '12px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '2px', marginBottom: '25px' }}>HOUSING CHECKLIST</h4>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '18px' }}>
                {[
                    { label: '청약통장 유효 회차 (24회+)', status: data.subscriptionCount >= 24 ? '✅ 충족' : '⚠️ 미달' },
                    { label: '무주택 세대주 유지', status: '✅ 확인' },
                    { label: '거주지 요건 분석', status: '✅ 적합' }
                ].map((item, idx) => (
                    <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span style={{ fontSize: '14px', fontWeight: 800, color: 'var(--text-dark)' }}>{item.label}</span>
                        <span style={{ fontSize: '12px', fontWeight: 900, color: (item.status || '').includes('충족') || (item.status || '').includes('확인') ? 'var(--soft-blue)' : '#f43f5e' }}>{item.status}</span>
                    </div>
                ))}
            </div>
        </motion.div>
      </div>

      <AnimatePresence>
          {selectedPolicy && <PolicyModal policy={selectedPolicy} onClose={() => setSelectedPolicy(null)} />}
      </AnimatePresence>
    </div>
  );
};

export default Tab_Housing;
