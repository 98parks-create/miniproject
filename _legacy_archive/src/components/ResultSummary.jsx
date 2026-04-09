import { motion, AnimatePresence } from 'framer-motion';
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, ResponsiveContainer } from 'recharts';
import { useState, useMemo } from 'react';
import PolicyModal from './PolicyModal';

const ResultSummary = ({ data, matchedPolicies }) => {
  const [selectedPolicy, setSelectedPolicy] = useState(null);

  // 차트 데이터 (사용자 데이터 및 매칭된 정책 기반 동적 생성)
  const chartData = useMemo(() => [
    { subject: '주거 안정성', A: matchedPolicies?.some(p => p.type === 'Housing') ? 85 : 40, fullMark: 100 },
    { subject: '금융 혜택', A: matchedPolicies?.some(p => p.type === 'Finance') ? 90 : 30, fullMark: 100 },
    { subject: '복지 충실도', A: matchedPolicies?.some(p => p.type === 'Welfare') ? 80 : 50, fullMark: 100 },
    { subject: '청년 특화', A: data.age <= 34 ? 95 : 60, fullMark: 100 },
    { subject: '신혼 특화', A: data.marital === 'Married' ? 98 : 20, fullMark: 100 },
  ], [data, matchedPolicies]);

  return (
    <div style={{ position: 'relative' }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1.5fr)', gap: '40px' }}>
        {/* 1. Policy Matching Data Summary Card */}
        <motion.div 
          className="glass-panel" 
          style={{ padding: '40px', display: 'flex', flexDirection: 'column', gap: '30px', background: 'white' }}
          initial={{ opacity: 0, x: -30 }}
          animate={{ opacity: 1, x: 0 }}
        >
          <div>
            <h4 style={{ fontSize: '13px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '2px', marginBottom: '8px' }}>POLICY MATCHING ENGINE</h4>
            <h2 style={{ fontSize: '24px', fontWeight: 900, color: 'var(--text-dark)' }}>나의 정책 매칭 데이터 요약</h2>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px' }}>
               {[
                 { label: '연령 / 거주', value: `${data.age}세 / ${data.region}` },
                 { label: '혼인 / 자녀', value: `${data.marital === 'Married' ? '신혼·예비' : '미혼'} / ${data.kids}명` },
                 { label: '가구 합산소득', value: `${(data.income || 0).toLocaleString()}만원` },
                 { label: '보유 자산액', value: `${(data.assets || 0).toLocaleString()}만원` },
                 { label: '현재 부채규모', value: `${(data.debt || 0).toLocaleString()}만원` },
                 { label: '청약 회차', value: `${data.subscriptionCount || 0}회` },
               ].map((item, i) => (
                  <div key={i} style={{ background: '#f8fafc', padding: '16px', borderRadius: '18px', border: '1px solid rgba(0,0,0,0.03)' }}>
                      <div style={{ fontSize: '10px', color: 'var(--text-muted)', fontWeight: 800, marginBottom: '4px' }}>{item.label}</div>
                      <div style={{ fontSize: '14px', fontWeight: 800, color: 'var(--text-dark)' }}>{item.value}</div>
                  </div>
               ))}
          </div>

          {/* AI Policy Matching Result Integration */}
          <div style={{ marginTop: '20px', padding: '24px', background: 'var(--primary-gradient)', borderRadius: '25px', color: 'white', boxShadow: '0 15px 35px rgba(45, 244, 192, 0.3)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '18px' }}>
                <span style={{ fontSize: '13px', fontWeight: 700, opacity: 0.9 }}>혜택 스마트 AI 매칭 🤖</span>
                <span style={{ fontSize: '11px', fontWeight: 800, padding: '5px 12px', background: 'rgba(255,255,255,0.25)', borderRadius: '12px' }}>{matchedPolicies?.length || 0}개 발견</span>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                 {matchedPolicies && matchedPolicies.length > 0 ? matchedPolicies.slice(0, 3).map(p => (
                   <button 
                      key={p.id}
                      onClick={() => setSelectedPolicy(p)}
                      style={{ 
                        textAlign: 'left', padding: '14px 18px', background: 'rgba(255,255,255,0.18)', border: 'none', borderRadius: '14px', color: 'white', 
                        fontSize: '13px', fontWeight: 800, cursor: 'pointer', transition: '0.3s', display: 'flex', justifyContent: 'space-between'
                      }}
                      onMouseEnter={e => e.currentTarget.style.transform = 'scale(1.03)'}
                      onMouseLeave={e => e.currentTarget.style.transform = 'scale(1)'}
                   >
                     <span>🚀 {p.title}</span>
                     <span style={{ opacity: 0.6 }}>→</span>
                   </button>
                 )) : (
                    <div style={{ fontSize: '12px', opacity: 0.8, textAlign: 'center', padding: '10px' }}>매칭된 특정 정책이 없습니다.</div>
                 )}
              </div>
          </div>
        </motion.div>

        {/* 2. Radar Chart Visualization */}
        <motion.div 
          className="glass-panel" 
          style={{ padding: '40px', height: '100%', minHeight: '600px', background: 'white' }}
          initial={{ opacity: 0, x: 30 }}
          animate={{ opacity: 1, x: 0 }}
        >
          <div style={{ marginBottom: '40px', textAlign: 'center' }}>
            <h3 style={{ fontSize: '22px', fontWeight: 900, color: 'var(--text-dark)' }}>내 조건 맞춤 혜택 지수</h3>
            <p style={{ color: 'var(--text-muted)', fontSize: '13px', marginTop: '6px' }}>사용자님의 데이터를 기반으로 산출된 카테고리별 정책 최적화 점수입니다.</p>
          </div>

          <div style={{ width: '100%', height: '380px' }}>
            <ResponsiveContainer width="100%" height="100%">
              <RadarChart cx="50%" cy="50%" outerRadius="80%" data={chartData}>
                <PolarGrid stroke="rgba(0,0,0,0.05)" />
                <PolarAngleAxis dataKey="subject" tick={{ fill: '#64748b', fontSize: 13, fontWeight: 700 }} />
                <PolarRadiusAxis angle={30} domain={[0, 100]} tick={false} axisLine={false} />
                <Radar
                   name="나의 점수"
                   dataKey="A"
                   stroke="var(--soft-blue)"
                   fill="var(--soft-blue)"
                   fillOpacity={0.15}
                   strokeWidth={4}
                />
              </RadarChart>
            </ResponsiveContainer>
          </div>

          <div style={{ marginTop: '30px', display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '15px' }}>
               {[
                 { icon: '🏠', label: '주거 안정성', score: chartData[0].A >= 80 ? '우수' : '보통' },
                 { icon: '💰', label: '금융 혜택', score: chartData[1].A >= 80 ? '최상' : '양호' },
                 { icon: '🎁', label: '복지 충실도', score: chartData[2].A >= 60 ? '안정' : '집중관리' },
               ].map((box, i) => (
                  <div key={i} style={{ textAlign: 'center', padding: '20px', borderRadius: '20px', background: '#f8fafc' }}>
                      <div style={{ fontSize: '28px', marginBottom: '10px' }}>{box.icon}</div>
                      <div style={{ fontSize: '12px', fontWeight: 800, color: 'var(--text-dark)' }}>{box.label}</div>
                      <div style={{ fontSize: '14px', fontWeight: 900, color: 'var(--soft-blue)', marginTop: '4px' }}>{box.score}</div>
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

export default ResultSummary;
