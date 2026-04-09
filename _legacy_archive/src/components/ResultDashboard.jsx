import { useState, useEffect, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import ResultSummary from './ResultSummary';
import Tab_Finance from './Tab_Finance';
import Tab_Welfare from './Tab_Welfare';
import Tab_Housing from './Tab_Housing';

const ResultDashboard = ({ data, forcedTab, policies }) => {
  const [activeTab, setActiveTab] = useState('Summary'); 

  // 중앙 매칭 엔진: 전체 정책 시트에서 사용자 조건에 맞는 상품만 실시간 선별
  const matchedPolicies = useMemo(() => {
    // 헬퍼 기능: 혼인 여부 통합 판단
    const isMarriedOrEngaged = data.marital !== 'Single';
    const hasKidsOrPregnant = data.kids > 0 || data.isPregnant;

    return policies.filter(p => {
      // 1. 연령 체크
      if (p.ageMin && data.age < p.ageMin) return false;
      if (p.ageMax && data.age > p.ageMax) return false;
      
      // 2. 소득/자산 체크 (사용자 입력이 0보다 클 때만 체크하여 유연성 확보)
      if (p.incomeLimit && data.income > 0 && data.income > p.incomeLimit) return false;
      if (p.assetLimit && data.assets > 0 && data.assets > p.assetLimit) return false;
      
      // 3. 가족 관계 및 혼인 체크
      if (p.requiresKids && !hasKidsOrPregnant) return false;
      if (p.maritalStatus === 'Married' && !isMarriedOrEngaged) return false;
      
      // 4. 특정 태그 및 지역 조건 (추후 확장용)
      if (p.tags.includes('출산가구') && !hasKidsOrPregnant) return false;
      
      return true;
    });
  }, [data, policies]);

  // Sync with external navigation (Header)
  useEffect(() => {
    if (forcedTab) {
      setActiveTab(forcedTab);
    }
  }, [forcedTab]);

  const tabs = [
    { id: 'Summary', label: '종합 리포트', icon: '📊' },
    { id: 'Finance', label: '금융/대출', icon: '💰' },
    { id: 'Welfare', label: '복지 혜택', icon: '🎁' },
    { id: 'Housing', label: '주거 지도', icon: '🏠' },
  ];

  return (
    <div className="app-view" style={{ paddingTop: '50px' }}>
      {/* Tab Navigation: Centered Container */}
      <nav style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        gap: '10px', 
        marginBottom: '40px', 
        background: 'rgba(255,255,255,0.3)', 
        padding: '8px', 
        borderRadius: '20px', 
        backdropFilter: 'blur(10px)',
        width: '100%',
        maxWidth: '1200px',
        margin: '0 auto' 
      }}>
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            style={{
              padding: '12px 24px', border: 'none', borderRadius: '15px', fontSize: '14px', fontWeight: 800, cursor: 'pointer', transition: '0.3s',
              background: activeTab === tab.id ? 'white' : 'transparent',
              color: activeTab === tab.id ? 'var(--soft-blue)' : 'var(--text-muted)',
              boxShadow: activeTab === tab.id ? '0 5px 15px rgba(0,0,0,0.05)' : 'none'
            }}
          >
            <span style={{ marginRight: '8px' }}>{tab.icon}</span>
            {tab.label}
          </button>
        ))}
      </nav>

      {/* Main Content: Centered Container */}
      <div className="container" style={{ width: '100%', maxWidth: '1200px', margin: '0 auto' }}>
        <AnimatePresence mode="wait">
          <motion.div
            key={activeTab}
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            transition={{ duration: 0.4 }}
          >
            {activeTab === 'Summary' && <ResultSummary data={data} matchedPolicies={matchedPolicies} />}
            {activeTab === 'Finance' && <Tab_Finance data={data} matchedPolicies={matchedPolicies.filter(p => p.category === 'Finance' || p.tags.includes('금융'))} />}
            {activeTab === 'Welfare' && <Tab_Welfare data={data} matchedPolicies={matchedPolicies.filter(p => p.category === 'Welfare' || p.tags.includes('복지'))} />}
            {activeTab === 'Housing' && <Tab_Housing data={data} matchedPolicies={matchedPolicies.filter(p => p.category === 'Housing' || p.tags.includes('주거'))} />}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
};

export default ResultDashboard;
