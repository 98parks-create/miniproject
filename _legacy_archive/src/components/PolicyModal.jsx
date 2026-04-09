import { motion } from 'framer-motion';

const PolicyModal = ({ policy, onClose }) => {
  if (!policy) return null;

  return (
    <motion.div 
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      onClick={onClose}
      style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', backdropFilter: 'blur(10px)', zIndex: 2000, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}
    >
      <motion.div 
        initial={{ scale: 0.9, y: 30 }}
        animate={{ scale: 1, y: 0 }}
        exit={{ scale: 0.9, y: 30 }}
        onClick={e => e.stopPropagation()}
        className="glass-panel"
        style={{ width: '100%', maxWidth: '600px', padding: '50px', background: 'white', position: 'relative' }}
      >
        {/* Header: Category & Tags */}
        <div style={{ marginBottom: '30px' }}>
          <div style={{ display: 'flex', gap: '8px', marginBottom: '15px' }}>
             {policy.tags.map(tag => (
               <span key={tag} style={{ background: 'rgba(59, 130, 246, 0.05)', color: 'var(--soft-blue)', fontSize: '11px', fontWeight: 800, padding: '6px 12px', borderRadius: '10px' }}>#{tag}</span>
             ))}
          </div>
          <h2 style={{ fontSize: '32px', fontWeight: 900, marginBottom: '10px', color: 'var(--text-dark)' }}>{policy.title}</h2>
          <p style={{ fontSize: '18px', fontWeight: 800, color: 'var(--soft-blue)' }}>{policy.summary}</p>
        </div>

        {/* Content: Description, Criteria, Benefit */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '25px', marginBottom: '40px' }}>
            <div style={{ background: '#f8fafc', padding: '20px', borderRadius: '20px' }}>
                <h4 style={{ fontSize: '13px', color: 'var(--text-muted)', marginBottom: '8px', fontWeight: 800 }}>📌 사업 상세 내용</h4>
                <p style={{ fontSize: '14px', lineHeight: 1.6, fontWeight: 500 }}>{policy.description}</p>
            </div>
            
            <div style={{ padding: '0 10px' }}>
                <h4 style={{ fontSize: '13px', color: 'var(--text-muted)', marginBottom: '5px', fontWeight: 800 }}>✅ 지원 자격</h4>
                <p style={{ fontSize: '15px', fontWeight: 800 }}>{policy.criteria}</p>
            </div>

            <div style={{ padding: '0 10px' }}>
                <h4 style={{ fontSize: '13px', color: 'var(--text-muted)', marginBottom: '5px', fontWeight: 800 }}>✨ 핵심 혜택</h4>
                <p style={{ fontSize: '18px', fontWeight: 900, color: 'var(--text-dark)' }}>{policy.benefit}</p>
            </div>
        </div>

        {/* Footer: Application Method & Close */}
        <div style={{ display: 'flex', gap: '15px' }}>
            <button 
              className="btn-matching" 
              style={{ flex: 1, fontSize: '15px', padding: '18px' }}
              onClick={() => window.open('https://www.bokjiro.go.kr', '_blank')}
            >
                지금 신청방법 확인하기 🚀
            </button>
            <button 
              onClick={onClose}
              style={{ padding: '18px 30px', borderRadius: '50px', border: '1px solid #e2e8f0', background: 'white', fontWeight: 800, cursor: 'pointer' }}
            >
                닫기
            </button>
        </div>

        <p style={{ textAlign: 'center', marginTop: '20px', fontSize: '11px', color: 'var(--text-muted)' }}>신청 가이드는 {policy.applyMethod}를 통해 공식적으로 제공됩니다.</p>
      </motion.div>
    </motion.div>
  );
};

export default PolicyModal;
