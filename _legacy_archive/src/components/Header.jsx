const Header = ({ onNavigate, onHome }) => {
    return (
        <header className="fixed-glass-header">
            {/* Logo Button: Return to Home */}
            <button className="logo-btn" onClick={onHome}>
                <div style={{ width: '40px', height: '40px', background: 'var(--primary-gradient)', borderRadius: '12px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'white', fontWeight: 900, fontSize: '20px' }}>路</div>
                <div style={{ textAlign: 'left' }}>
                   <h1 style={{ fontSize: '20px', fontWeight: 800, color: 'var(--soft-blue)', letterSpacing: '-0.5px', lineHeight: 1 }}>청춘로(路)</h1>
                   <p style={{ fontSize: '9px', fontWeight: 500, color: 'var(--text-muted)', marginTop: '2px' }}>청년·신혼부부 정부 정책 매칭 🧭</p>
                </div>
            </button>

            {/* Navigation Menu: Centered & Detailed Page Links */}
            <nav style={{ display: 'flex', gap: '30px' }}>
                <button className="nav-link" onClick={() => onNavigate('Housing')}>주거</button>
                <button className="nav-link" onClick={() => onNavigate('Finance')}>대출/금융</button>
                <button className="nav-link" onClick={() => onNavigate('Welfare')}>복지</button>
                <button className="nav-link" onClick={() => onNavigate('Summary')}>마이 리포트</button>
            </nav>

            {/* Login Badge: Right Aligned */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                <button style={{ fontSize: '12px', fontWeight: 700, color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer' }}>회원가입</button>
                <div style={{ width: '44px', height: '44px', borderRadius: '50%', background: 'white', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--soft-blue)', fontSize: '14px', boxShadow: '0 5px 15px rgba(0,0,0,0.05)', border: '1px solid #f1f5f9', cursor: 'pointer' }}>
                   👤
                </div>
            </div>
        </header>
    );
};

export default Header;
