import { useState, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { PieChart, Pie, Cell, ResponsiveContainer } from 'recharts';
import PolicyModal from './PolicyModal';

const Tab_Finance = ({ data, matchedPolicies }) => {
  const [selectedMatch, setSelectedMatch] = useState(null);

  // 대출 한도 시뮬레이션 (간소화)
  const simulation = useMemo(() => {
    const baseLimit = data.income * 5;
    const debtImpact = (data.debt || 0) * 0.5;
    const finalLimit = Math.max(0, baseLimit - debtImpact);
    const policyRate = data.marital === 'Married' ? 1.5 : 2.1;
    const maxLimit = 50000; // 5억 기준

    return {
      limit: finalLimit,
      rate: policyRate,
      percent: Math.min(100, Math.round((finalLimit / maxLimit) * 100))
    };
  }, [data]);

  const gaugeData = [
    { value: simulation.percent },
    { value: 100 - simulation.percent }
  ];

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.2fr) minmax(0, 1.8fr)', gap: '30px' }}>
      {/* 1. Best Match Policy Cards Area */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
        <h4 style={{ fontSize: '13px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '1px' }}>💰 추천 대출 및 금융 상품</h4>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
          {matchedPolicies.length > 0 ? (
            matchedPolicies.map(p => (
              <motion.div 
                key={p.id}
                whileHover={{ scale: 1.02 }}
                className="glass-panel"
                style={{ padding: '24px', background: 'white', borderLeft: '6px solid var(--soft-blue)', cursor: 'pointer' }}
                onClick={() => setSelectedMatch(p)}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                  <span style={{ fontSize: '11px', color: 'var(--soft-blue)', fontWeight: 800 }}>저금리 정책금융</span>
                  <span style={{ fontSize: '16px' }}>🏦</span>
                </div>
                <h5 style={{ fontSize: '16px', fontWeight: 900, color: 'var(--text-dark)' }}>{p.title}</h5>
                <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginTop: '8px' }}>{p.summary || '대출 한도 및 우대 금리 조건을 분석하고 있습니다.'}</p>
              </motion.div>
            ))
          ) : (
            <div className="glass-panel" style={{ padding: '30px', textAlign: 'center', opacity: 0.6 }}>
              <div style={{ fontSize: '24px', marginBottom: '10px' }}>💳</div>
              <p style={{ fontSize: '13px', fontWeight: 700 }}>현재 조건에 최적화된<br/>금융 상품을 분석 중입니다.</p>
            </div>
          )}
        </div>
        <AnimatePresence>
          {selectedMatch && <PolicyModal policy={selectedMatch} onClose={() => setSelectedMatch(null)} />}
        </AnimatePresence>
      </div>

      {/* 2. Simulation Area */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '30px' }}>
        <motion.div 
          className="glass-panel" 
          style={{ padding: '40px', background: 'white', textAlign: 'center' }}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
        >
          <h4 style={{ fontSize: '12px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '2px', marginBottom: '30px' }}>MAX LIMIT GAUGE</h4>
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
                            <Cell fill="var(--soft-blue)" />
                            <Cell fill="rgba(0,0,0,0.05)" />
                        </Pie>
                    </PieChart>
                </ResponsiveContainer>
                <div style={{ position: 'absolute', bottom: '15px', left: '50%', transform: 'translateX(-50%)' }}>
                    <div style={{ fontSize: '36px', fontWeight: 900, color: 'var(--text-dark)' }}>약 {simulation.limit.toLocaleString()}만</div>
                </div>
            </div>
            <div style={{ marginTop: '20px', fontSize: '14px', fontWeight: 700, color: 'var(--text-muted)' }}>
              예상 우대 금리: <span style={{ color: 'var(--soft-blue)' }}>연 {simulation.rate}% ~</span>
            </div>
        </motion.div>

        <motion.div 
          className="glass-panel" 
          style={{ padding: '35px', background: 'var(--primary-gradient)', color: 'white' }}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <h4 style={{ fontSize: '12px', fontWeight: 800, opacity: 0.8, letterSpacing: '1px', marginBottom: '20px' }}>FINANCE ADVISORY</h4>
          <p style={{ fontSize: '15px', fontWeight: 700, lineHeight: 1.6 }}>
            고객님의 소득({data.income.toLocaleString()}만원)과 부채({(data.debt || 0).toLocaleString()}만원)를 기반으로 산출된 예상 한도입니다. <br/><br/>
            신혼부부 전용 버팀목 대출 이용 시 최대 0.7%p의 추가 우대금리 적용이 가능합니다.
          </p>
        </motion.div>
      </div>
    </div>
  );
};

export default Tab_Finance;
