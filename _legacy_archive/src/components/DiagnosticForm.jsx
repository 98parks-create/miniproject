import { motion } from 'framer-motion';
import { useMemo } from 'react';

const DiagnosticForm = ({ data, onChange, onComplete, policies }) => {
  const regions = ['서울특별시', '경기도', '인천광역시', '부산광역시', '대구광역시', '광주광역시', '대전광역시', '울산광역시', '세종특별자치시'];
  
  // 실시간 정책 매칭 엔진 (부모로부터 전달받은 실제 policies 사용)
  const currentMatchCount = useMemo(() => {
    if (!policies) return 0;
    const isMarriedOrEngaged = data.marital !== 'Single';
    const hasKidsOrPregnant = data.kids > 0 || data.isPregnant;

    return policies.filter(p => {
      if (p.ageMin && data.age < p.ageMin) return false;
      if (p.ageMax && data.age > p.ageMax) return false;
      if (p.incomeLimit && data.income > 0 && data.income > p.incomeLimit) return false;
      if (p.assetLimit && data.assets > 0 && data.assets > p.assetLimit) return false;
      if (p.requiresKids && !hasKidsOrPregnant) return false;
      if (p.maritalStatus === 'Married' && !isMarriedOrEngaged) return false;
      return true;
    }).length;
  }, [data, policies]);
  const maritalStatuses = [
    { value: 'Single', label: '미혼' },
    { value: 'Engaged', label: '예비부부 (1년 이내 결혼 예정)' },
    { value: 'Married_1', label: '신혼부부 (2년 이내)' },
    { value: 'Married_2', label: '신혼부부 (7년 이내)' },
  ];

  const handleUpdate = (field, value) => {
    onChange({ [field]: value });
  };

  return (
    <section className="app-view flex-center">
      <motion.div 
        className="glass-panel" 
        style={{ width: '100%', maxWidth: '850px', padding: '50px' }}
        initial={{ y: 50, opacity: 0 }}
        animate={{ y: 0, opacity: 1 }}
        transition={{ type: 'spring', stiffness: 100 }}
      >
        <div style={{ textAlign: 'center', marginBottom: '40px' }}>
          <h2 style={{ fontSize: '28px', fontWeight: 900, color: 'var(--text-dark)' }}>나의 정책 매칭 데이터 정밀 기입</h2>
          <p style={{ color: 'var(--text-muted)', marginTop: '8px' }}>청춘로 AI 분석기가 최적의 정책을 매칭하기 위해 상세 정보를 수집합니다.</p>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '30px' }}>
          {/* 1. 나이 */}
          <div className="input-group">
            <label style={{ fontSize: '11px', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '10px', display: 'block' }}>만 나이</label>
            <input 
              type="range" min="19" max="49" value={data.age} 
              onChange={e => handleUpdate('age', parseInt(e.target.value))} 
              style={{ width: '100%', accentColor: 'var(--soft-blue)' }}
            />
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '10px', fontSize: '14px', fontWeight: 800, color: 'var(--soft-blue)' }}>
               <span>만 {data.age}세</span>
            </div>
          </div>

          {/* 2. 거주지 */}
          <div className="input-group">
            <label style={{ fontSize: '11px', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '10px', display: 'block' }}>현재 거주지 (시/도)</label>
            <select 
              value={data.region} 
              onChange={e => handleUpdate('region', e.target.value)}
              style={{ width: '100%', padding: '12px 16px', borderRadius: '12px', border: '1px solid rgba(0,0,0,0.05)', background: 'white', fontWeight: 600 }}
            >
              {regions.map(r => <option key={r} value={r}>{r}</option>)}
            </select>
          </div>

          {/* 3. 혼인여부/기간 */}
          <div className="input-group" style={{ gridColumn: 'span 2' }}>
            <label style={{ fontSize: '11px', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '10px', display: 'block' }}>혼인 여부 및 기간</label>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '15px' }}>
               {maritalStatuses.map(status => (
                 <button
                    key={status.value}
                    onClick={() => handleUpdate('marital', status.value)}
                    style={{
                      padding: '12px', border: '1px solid', borderRadius: '12px', fontSize: '13px', fontWeight: 700, cursor: 'pointer', transition: '0.3s',
                      borderColor: data.marital === status.value ? 'var(--soft-blue)' : 'rgba(0,0,0,0.05)',
                      backgroundColor: data.marital === status.value ? 'rgba(59, 130, 246, 0.05)' : 'white',
                      color: data.marital === status.value ? 'var(--soft-blue)' : 'var(--text-dark)'
                    }}
                 >
                    {status.label}
                 </button>
               ))}
            </div>
          </div>

          {/* 4. 경제 데이터 (Economic Data) */}
          <div className="input-group">
            <label style={{ fontSize: '11px', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '10px', display: 'block' }}>합산 연소득 (만원)</label>
            <input 
              type="number" value={data.income} 
              onChange={e => handleUpdate('income', parseInt(e.target.value) || 0)} 
              style={{ width: '100%', padding: '15px', borderRadius: '15px', border: '1px solid #e2e8f0', fontSize: '16px', fontWeight: 700 }} 
            />
          </div>
          <div className="input-group">
            <label style={{ fontSize: '11px', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '10px', display: 'block' }}>보유 자산 (만원)</label>
            <input 
              type="number" value={data.assets} 
              onChange={e => handleUpdate('assets', parseInt(e.target.value) || 0)} 
              style={{ width: '100%', padding: '15px', borderRadius: '15px', border: '1px solid #e2e8f0', fontSize: '16px', fontWeight: 700 }} 
            />
          </div>

          <div className="input-group">
            <label style={{ fontSize: '11px', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '10px', display: 'block' }}>부채 규모 (만원)</label>
            <input 
              type="number" value={data.debt || 0} 
              onChange={e => handleUpdate('debt', parseInt(e.target.value) || 0)} 
              style={{ width: '100%', padding: '15px', borderRadius: '15px', border: '1px solid #e2e8f0', fontSize: '16px', fontWeight: 700 }} 
            />
          </div>
          <div className="input-group">
            <label style={{ fontSize: '11px', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '10px', display: 'block' }}>청약 납입 총액 (만원)</label>
            <input 
              type="number" value={data.subscribedAmount || 0} 
              onChange={e => handleUpdate('subscribedAmount', parseInt(e.target.value) || 0)} 
              style={{ width: '100%', padding: '15px', borderRadius: '15px', border: '1px solid #e2e8f0', fontSize: '16px', fontWeight: 700 }} 
            />
          </div>

          {/* 6. 자녀수/임신여부 */}
          <div className="input-group">
            <label style={{ fontSize: '11px', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '10px', display: 'block' }}>자녀 수 및 임신 여부</label>
            <div style={{ display: 'flex', gap: '15px' }}>
                <input 
                  type="number" min="0" value={data.kids} 
                  onChange={e => handleUpdate('kids', parseInt(e.target.value) || 0)}
                  style={{ width: '80px', padding: '12px', borderRadius: '12px', border: '1px solid rgba(0,0,0,0.05)', textAlign: 'center', fontWeight: 800 }} 
                />
                <button
                    onClick={() => handleUpdate('isPregnant', !data.isPregnant)}
                    style={{
                      flex: 1, padding: '12px', border: '1px solid', borderRadius: '12px', fontSize: '13px', fontWeight: 700, cursor: 'pointer', transition: '0.3s',
                      borderColor: data.isPregnant ? 'var(--neon-mint)' : 'rgba(0,0,0,0.05)',
                      backgroundColor: data.isPregnant ? 'rgba(45, 244, 192, 0.05)' : 'white',
                      color: data.isPregnant ? 'var(--neon-mint)' : 'var(--text-dark)'
                    }}
                >
                    {data.isPregnant ? '✅ 임신 중' : '아동·태아 포함'}
                </button>
            </div>
          </div>

          <div className="input-group">
            <label style={{ fontSize: '11px', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '10px', display: 'block' }}>청약 통장 가입 기간 (개월)</label>
            <input 
              type="number" min="0" value={data.subscriptionCount} 
              onChange={e => handleUpdate('subscriptionCount', parseInt(e.target.value) || 0)}
              style={{ width: '100%', padding: '12px', borderRadius: '12px', border: '1px solid rgba(0,0,0,0.05)', fontWeight: 800, fontSize: '16px' }}
            />
          </div>
        </div>

        {/* Real-time Match Analysis Indicator (로직은 유지하되 심플한 UI로 변경) */}
        <div style={{ marginTop: '30px', padding: '24px', background: '#f8fafc', borderRadius: '25px', border: '1px solid rgba(0,0,0,0.01)', textAlign: 'center' }}>
             <motion.div 
               key={currentMatchCount}
               initial={{ y: 5, opacity: 0 }}
               animate={{ y: 0, opacity: 1 }}
               style={{ fontSize: '15px', fontWeight: 900, color: 'var(--soft-blue)' }}
             >
                🔍 실시간 데이터 분석: <span style={{ fontSize: '20px', color: 'var(--neon-mint)', textDecoration: 'underline' }}>{currentMatchCount}건</span>의 최적 가용 정책 식별됨
             </motion.div>
             <p style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '8px', letterSpacing: '0.5px' }}>
                기입하신 [거주지/소득/가구구성] 데이터에 최적화된 결과입니다.
             </p>
        </div>

        <div style={{ marginTop: '40px', textAlign: 'center' }}>
            <button className="btn-matching" onClick={onComplete} style={{ width: '100%', fontSize: '20px' }}>
                나에게 해당하는 법률/정책 보러가기 🧭
            </button>
            <p style={{ marginTop: '16px', fontSize: '11px', color: 'var(--text-muted)' }}>
                입력하신 정보는 HTTPS 암호화를 통해 안전하게 전송되며 통계 분석 목적으로만 사용됩니다.
            </p>
        </div>
      </motion.div>
    </section>
  );
};

export default DiagnosticForm;
