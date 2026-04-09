import { useState, useMemo, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import Header from './components/Header';
import Hero from './components/Hero';
import DiagnosticForm from './components/DiagnosticForm';
import ResultDashboard from './components/ResultDashboard';
import PolicyService from './services/PolicyService';
import { policies as fallbackPolicies } from './data/PolicyData';

function App() {
  const [step, setStep] = useState('Hero'); // 'Hero' | 'Form' | 'Matching' | 'Result'
  const [policies, setPolicies] = useState(fallbackPolicies);
  const [userData, setUserData] = useState({
    // ... (기존 데이터 유지)
    age: 29,
    region: 'Seoul',
    marital: 'Single',
    marriageYears: 0,
    income: 5500,
    assets: 10000,
    debt: 500,
    subscribedAmount: 1200,
    kids: 0,
    isPregnant: false,
    subscriptionCount: 24,
  });

  // ... (bgColor 유입 로직 유지)
  const bgColor = useMemo(() => {
    const hue = (userData.income / 10000) * 120 + (userData.age * 2);
    return `hsl(${hue}, 80%, 65%)`;
  }, [userData.income, userData.age]);

  const [activeTab, setActiveTab] = useState('Summary');

  // 🚀 초기 로드 시 실시간 정책 데이터 미리 확보
  useEffect(() => {
    const initFetch = async () => {
      try {
        const live = await PolicyService.fetchAllPolicies();
        setPolicies(live);
      } catch (e) {
        console.warn('초기 정책 로딩 실패, 기본값 유지');
      }
    };
    initFetch();
  }, []);

  const handleStart = () => setStep('Form');

  const handleHome = () => {
    setStep('Hero');
    setActiveTab('Summary');
  };

  const handleNavigate = (tabId) => {
    setStep('Result');
    setActiveTab(tabId);
  };

  const handleFormChange = (newData) => {
    setUserData(prev => ({ ...prev, ...newData }));
  };

  const handleMatch = async () => {
    setStep('Matching');
    
    try {
      // 📡 실시간 API 데이터 로딩 시작
      const livePolicies = await PolicyService.fetchAllPolicies();
      setPolicies(livePolicies);
    } catch (error) {
      console.warn('실시간 정책 로딩 중 오류 발생, 기존 데이터 사용');
    }

    setTimeout(() => {
      setStep('Result');
      setActiveTab('Summary');
    }, 2500); 
  };

  return (
    <div className="app-main-container">
      {/* Background Interaction Engine */}
      <div style={{ position: 'fixed', inset: 0, zIndex: -1, pointerEvents: 'none' }}>
        <motion.div 
           animate={{ 
              scale: [1, 1.2, 1],
              rotate: [0, 90, 180, 270, 360],
              backgroundColor: bgColor 
           }}
           transition={{ duration: 20, repeat: Infinity, ease: "linear" }}
           style={{ 
              position: 'absolute', 
              top: '-10%', 
              left: '5%', 
              width: '50vw', 
              height: '50vw', 
              filter: 'blur(100px)', 
              opacity: 0.15,
              borderRadius: '60% 40% 30% 70% / 60% 30% 70% 40%' 
           }} 
        />
        <motion.div 
           animate={{ 
              scale: [1.2, 1, 1.2],
              rotate: [360, 270, 180, 90, 0],
              backgroundColor: step === 'Result' ? '#3b82f6' : '#2df4c0' 
           }}
           transition={{ duration: 25, repeat: Infinity, ease: "linear" }}
           style={{ 
              position: 'absolute', 
              bottom: '-10%', 
              right: '-5%', 
              width: '40vw', 
              height: '40vw', 
              filter: 'blur(80px)', 
              opacity: 0.2,
              borderRadius: '40% 60% 70% 30% / 40% 70% 30% 60%' 
           }} 
        />
      </div>

      <Header onNavigate={handleNavigate} onHome={handleHome} />
      
      <AnimatePresence mode="wait">
        {step === 'Hero' && (
          <motion.div key="hero" initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -20 }}>
            <Hero onStart={handleStart} />
          </motion.div>
        )}

        {step === 'Form' && (
          <motion.div key="form" initial={{ opacity: 0, scale: 0.9 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 1.1 }}>
            <DiagnosticForm 
              data={userData} 
              onChange={handleFormChange} 
              onComplete={handleMatch} 
              policies={policies}
            />
          </motion.div>
        )}

        {step === 'Matching' && (
          <motion.div key="matching" className="app-view flex-center" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
             <div style={{ textAlign: 'center' }}>
                <motion.div 
                   animate={{ rotate: 360 }} 
                   transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
                   style={{ width: '80px', height: '80px', border: '8px solid var(--neon-mint)', borderTopColor: 'transparent', borderRadius: '50%', margin: '0 auto 40px' }}
                />
                <h2 style={{ fontSize: '28px', fontWeight: 800 }}>최적의 정책을 매칭 분석 중입니다...</h2>
                <p style={{ color: 'var(--text-muted)', marginTop: '16px' }}>당신의 데이터를 기반으로 주거/금융/복지 혜택을 계산하고 있습니다.</p>
             </div>
          </motion.div>
        )}

        {step === 'Result' && (
          <motion.div key="result" initial={{ opacity: 0, y: 50 }} animate={{ opacity: 1, y: 0 }}>
            <ResultDashboard data={userData} forcedTab={activeTab} policies={policies} />
          </motion.div>
        )}
      </AnimatePresence>

      {/* Floating AI Secretary Chatbot Button */}
      <motion.div 
        initial={{ opacity: 0, scale: 0.8 }}
        animate={{ opacity: 1, scale: 1 }}
        style={{ position: 'fixed', bottom: '40px', right: '40px', zIndex: 9999, cursor: 'pointer' }}
      >
        <div style={{ position: 'relative' }}>
          <motion.div 
            animate={{ y: [0, -10, 0] }}
            transition={{ repeat: Infinity, duration: 3 }}
            style={{ position: 'absolute', bottom: '70px', right: 0, background: 'white', padding: '12px 20px', borderRadius: '20px 20px 0 20px', boxShadow: '0 10px 25px rgba(0,0,0,0.1)', whiteSpace: 'nowrap', fontSize: '13px', fontWeight: 800, color: 'var(--soft-blue)' }}
          >
             궁금한 법 조항이 있나요? 🤖
          </motion.div>
          <motion.div 
            whileHover={{ scale: 1.1 }}
            whileTap={{ scale: 0.9 }}
            className="flex-center"
            style={{ width: '64px', height: '64px', background: 'var(--primary-gradient)', borderRadius: '50%', boxShadow: '0 15px 35px rgba(45, 244, 192, 0.4)', color: 'white', fontSize: '28px' }}
          >
             💬
          </motion.div>
        </div>
      </motion.div>

      <footer className="footer" style={{ textAlign: 'center', padding: '60px 0', opacity: 0.3, fontSize: '12px' }}>
          © 2026 청춘로(路) - POLICY MATCHING ENGINE
      </footer>
    </div>
  );
}

export default App;
