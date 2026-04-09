import { useState, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import PolicyModal from './PolicyModal';

const Tab_Welfare = ({ data, matchedPolicies }) => {
  const [selectedMatch, setSelectedMatch] = useState(null);

  // 수혜 혜택 시뮬레이션 (간소화)
  const totalBenefit = useMemo(() => {
    let cash = matchedPolicies.length > 0 ? 500 : 0; // 매칭된 데이터 기반 (단위: 만원)
    if (data.kids > 0) cash += data.kids * 120;
    if (data.marital === 'Married') cash += 50;
    return cash;
  }, [data, matchedPolicies]);

  const flowData = [
    { name: '1월', benefit: totalBenefit * 0.05 },
    { name: '3월', benefit: totalBenefit * 0.15 },
    { name: '6월', benefit: totalBenefit * 0.45 },
    { name: '9월', benefit: totalBenefit * 0.75 },
    { name: '12월', benefit: totalBenefit },
  ];

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.3fr) minmax(0, 1.7fr)', gap: '30px' }}>
      {/* 1. Best Welfare Matches Area */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
         <h4 style={{ fontSize: '13px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '1px' }}>🎁 나에게 해당되는 복지 혜택</h4>
         <div style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
            {matchedPolicies.length > 0 ? (
                matchedPolicies.map(p => (
                    <motion.div 
                        key={p.id}
                        whileHover={{ scale: 1.02 }}
                        className="glass-panel"
                        style={{ padding: '24px', background: 'white', borderLeft: '6px solid var(--neon-mint)', cursor: 'pointer' }}
                        onClick={() => setSelectedMatch(p)}
                    >
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                            <span style={{ fontSize: '11px', color: 'var(--soft-blue)', fontWeight: 800 }}>즉시 신청가능</span>
                            <span style={{ fontSize: '16px' }}>✨</span>
                        </div>
                        <h5 style={{ fontSize: '16px', fontWeight: 900, color: 'var(--text-dark)' }}>{p.title}</h5>
                        <p style={{ fontSize: '12px', color: 'var(--text-muted)', marginTop: '8px' }}>{p.summary || '복지로 실시간 혜택 기준을 분석하고 있습니다.'}</p>
                    </motion.div>
                ))
            ) : (
                <div className="glass-panel" style={{ padding: '30px', textAlign: 'center', opacity: 0.6 }}>
                    <div style={{ fontSize: '24px', marginBottom: '10px' }}>🎈</div>
                    <p style={{ fontSize: '13px', fontWeight: 700 }}>매칭된 일반 복지 상품이 없으나<br/>상세 분석을 시작합니다.</p>
                </div>
            )}
         </div>

         {/* Yearly Cash Summary Card */}
         <motion.div 
            className="glass-panel" 
            style={{ padding: '30px', background: 'var(--primary-gradient)', color: 'white', marginTop: '10px' }}
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
         >
            <h4 style={{ fontSize: '12px', fontWeight: 800, opacity: 0.8, letterSpacing: '1px', marginBottom: '15px' }}>ANNUAL CASH FLOW</h4>
            <div style={{ fontSize: '38px', fontWeight: 900 }}>{totalBenefit.toLocaleString()}<span style={{ fontSize: '16px', marginLeft: '5px' }}>만원</span></div>
            <p style={{ fontSize: '13px', opacity: 0.9, marginTop: '8px', fontWeight: 700 }}>2024년 수혜 예상 현금성 혜택 총액</p>
         </motion.div>

         <AnimatePresence>
            {selectedMatch && <PolicyModal policy={selectedMatch} onClose={() => setSelectedMatch(null)} />}
         </AnimatePresence>
      </div>

      {/* 2. Visual Analysis Area */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '30px' }}>
         <motion.div 
            className="glass-panel" 
            style={{ padding: '40px', background: 'white', minHeight: '520px' }}
            initial={{ opacity: 0, scale: 0.98 }}
            animate={{ opacity: 1, scale: 1 }}
         >
            <h4 style={{ fontSize: '12px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '2px', marginBottom: '40px' }}>베네핏 수혜 흐름도</h4>
            <div style={{ width: '100%', height: '300px' }}>
               <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={flowData}>
                     <defs>
                        <linearGradient id="colorBenefit" x1="0" y1="0" x2="0" y2="1">
                           <stop offset="5%" stopColor="var(--neon-mint)" stopOpacity={0.3}/>
                           <stop offset="95%" stopColor="var(--neon-mint)" stopOpacity={0}/>
                        </linearGradient>
                     </defs>
                     <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="rgba(0,0,0,0.05)" />
                     <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 12, fontWeight: 700, fill: '#64748b' }} />
                     <YAxis hide domain={[0, totalBenefit * 1.2]} />
                     <Tooltip 
                        contentStyle={{ borderRadius: '15px', border: 'none', boxShadow: '0 10px 30px rgba(0,0,0,0.1)', fontWeight: 800 }}
                     />
                     <Area 
                        type="monotone" 
                        dataKey="benefit" 
                        stroke="var(--neon-mint)" 
                        strokeWidth={4} 
                        fillOpacity={1} 
                        fill="url(#colorBenefit)" 
                     />
                  </AreaChart>
               </ResponsiveContainer>
            </div>
            <div style={{ marginTop: '30px', padding: '24px', background: '#f8fafc', borderRadius: '24px', border: '1px solid rgba(0,0,0,0.02)' }}>
               <h5 style={{ fontSize: '14px', fontWeight: 900, color: 'var(--text-dark)', marginBottom: '10px' }}>💡 복지 큐레이션 가이드</h5>
               <p style={{ fontSize: '13px', color: 'var(--text-muted)', lineHeight: 1.6, fontWeight: 700 }}>
                  사용자님의 가구 구성(자녀 {data.kids}명)을 고려할 때, 6~9월 사이 지자체별 아동수당 및 추가 양육 지원금 집중 수혜기가 형성됩니다.
               </p>
            </div>
         </motion.div>
      </div>
    </div>
  );
};

export default Tab_Welfare;
