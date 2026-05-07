
// ── Design tokens ────────────────────────────────────────────
const C = {
  bg:           '#0e1513',
  surface:      '#1a211f',
  border:       '#3c4947',
  text:         '#dde4e1',
  textMuted:    '#bbcac6',
  primary:      '#4fdbc8',
  primaryFixed: '#71f8e4',
  onPrimary:    '#003731',
  tertiary:     '#f38764',
  error:        '#ffb4ab',
};

const inputBase = {
  width: '100%', background: 'rgba(26,33,31,0.9)',
  border: `1px solid #3c4947`, borderRadius: 4,
  padding: '12px 16px', fontSize: 14, color: '#dde4e1',
  outline: 'none', boxSizing: 'border-box',
  fontFamily: 'Inter, sans-serif', transition: 'border-color 0.2s, box-shadow 0.2s',
};
const labelBase = {
  fontSize: 12, fontFamily: 'JetBrains Mono, monospace',
  color: '#bbcac6', textTransform: 'uppercase',
  letterSpacing: '0.05em', fontWeight: 500,
  display: 'block', marginBottom: 6,
};

// ── Shared full-page layout ──────────────────────────────────
function AuthShell({ leftContent, rightContent }) {
  return (
    <div style={{ display: 'flex', minHeight: '100vh', fontFamily: 'Inter, sans-serif', position: 'relative', overflow: 'hidden' }}>

      {/* ── Full-bleed background image (left 65%) ── */}
      <div style={{
        position: 'absolute', top: 0, left: 0, bottom: 0,
        width: '65%',
        backgroundImage: "url('https://images.unsplash.com/photo-1635070041078-e363dbe005cb?q=80&w=2070&auto=format&fit=crop')",
        backgroundSize: 'cover', backgroundPosition: 'center',
        zIndex: 0,
      }} />

      {/* Dark overlay on image */}
      <div style={{
        position: 'absolute', top: 0, left: 0, bottom: 0, width: '65%',
        background: 'linear-gradient(to right, rgba(14,21,19,0.45) 0%, rgba(14,21,19,0.6) 70%, rgba(14,21,19,0.95) 100%)',
        zIndex: 1,
      }} />

      {/* Glow orbs */}
      <div style={{ position:'absolute', top:'20%', left:'15%', width:400, height:400, background:'rgba(79,219,200,0.08)', borderRadius:'50%', filter:'blur(120px)', zIndex:1 }} />
      <div style={{ position:'absolute', bottom:'20%', left:'30%', width:280, height:280, background:'rgba(243,135,100,0.07)', borderRadius:'50%', filter:'blur(100px)', zIndex:1 }} />

      {/* ── Left brand panel (65%) ── */}
      <div style={{
        flex: '0 0 65%', position: 'relative', zIndex: 2,
        display: 'flex', flexDirection: 'column', justifyContent: 'center',
        padding: '64px 72px',
      }}>
        {leftContent}
      </div>

      {/* ── Right form panel (35%) ── */}
      <div style={{
        flex: '0 0 35%', position: 'relative', zIndex: 2,
        background: C.bg,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        padding: '48px 40px',
        overflowY: 'auto',
      }}>
        {rightContent}
      </div>

    </div>
  );
}

// ── Login Page ───────────────────────────────────────────────
function LoginPage({ setPage, addToast }) {
  const [username, setUsername] = React.useState('');
  const [password, setPassword] = React.useState('');
  const [showPass, setShowPass] = React.useState(false);
  const [loading, setLoading]   = React.useState(false);
  const [error, setError]       = React.useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    if (!username || !password) { setError('Please fill in all fields.'); return; }
    setLoading(true);
    try {
      const csrf = (window.DJANGO_CTX && window.DJANGO_CTX.csrfToken) || '';
      const res  = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf },
        credentials: 'same-origin',
        body: JSON.stringify({ username, password }),
      });
      const data = await res.json();
      if (!res.ok) { setError(data.error || 'Invalid credentials.'); return; }
      addToast('success', `Welcome back, ${data.username}!`);
      setTimeout(() => setPage('dashboard'), 500);
    } catch {
      setError('Connection error. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const left = (
    <>
      {/* Logo */}
      <div style={{ display:'flex', alignItems:'center', gap:14, marginBottom:40 }}>
        <div style={{
          width:44, height:44, borderRadius:12,
          background:'radial-gradient(circle at 30% 30%, #2DD4BF 0%, #0EA5E9 55%, #6366F1 100%)',
          display:'flex', alignItems:'center', justifyContent:'center',
          boxShadow:'0 6px 22px rgba(20,184,166,0.4), inset 0 0 0 1px rgba(255,255,255,0.15)',
        }}>
          <svg width="26" height="26" viewBox="0 0 24 24" fill="none">
            <path d="M4 17 L9 11 L13 14 L20 6" stroke="#fff" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" />
            <circle cx="20" cy="6" r="1.8" fill="#fff" />
            <circle cx="4" cy="17" r="1.4" fill="#fff" opacity="0.8" />
          </svg>
        </div>
        <span style={{ fontSize:30, fontWeight:700, color:C.text, letterSpacing:'-0.03em', fontFamily:'"Plus Jakarta Sans", "Inter", sans-serif' }}>
          pulpe<span style={{ color:'#2DD4BF' }}>.</span>
        </span>
      </div>

      <h1 style={{ fontSize:46, fontWeight:800, color:C.text, margin:'0 0 18px', lineHeight:1.05, letterSpacing:'-0.035em', fontFamily:'"Plus Jakarta Sans", "Inter", sans-serif' }}>
        Pulp the noise.<br/>Taste the signal.
      </h1>
      <p style={{ fontSize:15, color:C.textMuted, lineHeight:1.7, maxWidth:460, margin:'0 0 48px' }}>
        Stream millions of reviews through a sentiment engine that hands you the verdict before the next batch lands. No dashboards stuck in yesterday — just live pulp, freshly pressed.
      </p>

      {/* Stats */}
      <div style={{ display:'flex', gap:0, alignItems:'stretch' }}>
        <div style={{ paddingRight:32, borderRight:`2px solid rgba(79,219,200,0.4)` }}>
          <div style={{ fontSize:11, fontFamily:'JetBrains Mono,monospace', color:C.primary, textTransform:'uppercase', letterSpacing:'0.08em', marginBottom:6 }}>F1-Score</div>
          <div style={{ fontSize:32, fontWeight:700, color:C.text }}>82.74%</div>
        </div>
        <div style={{ paddingLeft:32, paddingRight:32, borderRight:`2px solid rgba(243,135,100,0.4)` }}>
          <div style={{ fontSize:11, fontFamily:'JetBrains Mono,monospace', color:C.tertiary, textTransform:'uppercase', letterSpacing:'0.08em', marginBottom:6 }}>Reviews</div>
          <div style={{ fontSize:32, fontWeight:700, color:C.text }}>568K</div>
        </div>
        <div style={{ paddingLeft:32 }}>
          <div style={{ fontSize:11, fontFamily:'JetBrains Mono,monospace', color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.08em', marginBottom:6 }}>Products</div>
          <div style={{ fontSize:32, fontWeight:700, color:C.text }}>74K+</div>
        </div>
      </div>
    </>
  );

  const right = (
    <div style={{ width:'100%', maxWidth:340 }}>
      <h2 style={{ fontSize:28, fontWeight:700, color:C.text, margin:'0 0 6px', letterSpacing:'-0.02em' }}>Sign in</h2>
      <p style={{ fontSize:13, color:C.textMuted, margin:'0 0 28px' }}>Access your analytics dashboard.</p>

      {error && (
        <div style={{ background:'rgba(255,180,171,0.08)', border:'1px solid rgba(255,180,171,0.25)', borderRadius:4, padding:'10px 14px', marginBottom:20, color:C.error, fontSize:13, display:'flex', gap:8, alignItems:'center' }}>
          <span className="material-symbols-outlined" style={{ fontSize:16 }}>error</span>{error}
        </div>
      )}

      <form onSubmit={handleSubmit} style={{ display:'flex', flexDirection:'column', gap:18 }}>
        <div>
          <label style={labelBase}>Username</label>
          <input type="text" value={username} onChange={e => setUsername(e.target.value)}
            placeholder="e.g. admin" style={inputBase}
            onFocus={e => { e.target.style.borderColor=C.primary; e.target.style.boxShadow=`0 0 0 1px ${C.primary}`; }}
            onBlur={e =>  { e.target.style.borderColor='#3c4947'; e.target.style.boxShadow='none'; }} />
        </div>

        <div>
          <label style={labelBase}>Password</label>
          <div style={{ position:'relative' }}>
            <input type={showPass ? 'text' : 'password'} value={password} onChange={e => setPassword(e.target.value)}
              placeholder="••••••••" style={{ ...inputBase, paddingRight:44 }}
              onFocus={e => { e.target.style.borderColor=C.primary; e.target.style.boxShadow=`0 0 0 1px ${C.primary}`; }}
              onBlur={e =>  { e.target.style.borderColor='#3c4947'; e.target.style.boxShadow='none'; }} />
            <button type="button" onClick={() => setShowPass(!showPass)}
              style={{ position:'absolute', right:12, top:'50%', transform:'translateY(-50%)', background:'none', border:'none', cursor:'pointer', color:C.textMuted, display:'flex', padding:0 }}>
              <span className="material-symbols-outlined" style={{ fontSize:18 }}>{showPass ? 'visibility_off' : 'visibility'}</span>
            </button>
          </div>
        </div>

        <button type="submit" disabled={loading} style={{
          width:'100%', padding:'13px', borderRadius:4, border:'none',
          background: loading ? 'rgba(79,219,200,0.6)' : C.primary,
          color: C.onPrimary, fontSize:15, fontWeight:700,
          cursor: loading ? 'wait' : 'pointer', fontFamily:'Inter,sans-serif',
          display:'flex', alignItems:'center', justifyContent:'center', gap:8,
          transition:'background 0.2s', boxShadow:'0 0 20px rgba(79,219,200,0.2)',
          marginTop:6,
        }}
        onMouseEnter={e => { if(!loading) e.currentTarget.style.background=C.primaryFixed; }}
        onMouseLeave={e => { if(!loading) e.currentTarget.style.background=C.primary; }}
        >
          {loading
            ? <><span className="material-symbols-outlined" style={{ fontSize:18, animation:'spin 1s linear infinite' }}>refresh</span>Signing in…</>
            : <>Sign in <span className="material-symbols-outlined" style={{ fontSize:18 }}>arrow_forward</span></>
          }
        </button>
      </form>

      <div style={{ marginTop:28, paddingTop:20, borderTop:'1px solid rgba(60,73,71,0.5)', textAlign:'center' }}>
        <p style={{ fontSize:12, color:C.textMuted, margin:0 }}>
          Don't have an account?{' '}
          <button onClick={() => setPage('register')} style={{ background:'none', border:'none', color:C.primary, cursor:'pointer', fontSize:12, fontWeight:600, padding:0, fontFamily:'Inter,sans-serif' }}>
            Create Account
          </button>
        </p>
      </div>
    </div>
  );

  return <AuthShell leftContent={left} rightContent={right} />;
}

// ── Register Page ────────────────────────────────────────────
function RegisterPage({ setPage, addToast }) {
  const [form, setForm]       = React.useState({ username:'', email:'', password:'', confirm:'' });
  const [errors, setErrors]   = React.useState({});
  const [loading, setLoading] = React.useState(false);
  const [terms, setTerms]     = React.useState(false);

  const set = k => e => setForm(f => ({ ...f, [k]: e.target.value }));

  const validate = () => {
    const e = {};
    if (!form.username || form.username.length < 3) e.username = 'At least 3 characters';
    if (!form.email || !form.email.includes('@'))   e.email    = 'Valid email required';
    if (!form.password || form.password.length < 8) e.password = 'At least 8 characters';
    if (form.password !== form.confirm)             e.confirm  = 'Passwords do not match';
    if (!terms)                                     e.terms    = 'You must accept the terms';
    return e;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const errs = validate();
    if (Object.keys(errs).length) { setErrors(errs); return; }
    setErrors({}); setLoading(true);
    try {
      const csrf = (window.DJANGO_CTX && window.DJANGO_CTX.csrfToken) || '';
      const res  = await fetch('/api/auth/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf },
        credentials: 'same-origin',
        body: JSON.stringify(form),
      });
      const data = await res.json();
      if (!res.ok) { setErrors({ username: data.error || 'Registration failed' }); return; }
      addToast('success', 'Account created. Welcome to pulpe.');
      setTimeout(() => setPage('dashboard'), 500);
    } catch {
      setErrors({ username: 'Connection error. Please try again.' });
    } finally {
      setLoading(false);
    }
  };

  const Field = ({ name, label, type = 'text', placeholder }) => (
    <div>
      <label style={labelBase}>{label}</label>
      <input type={type} value={form[name]} onChange={set(name)} placeholder={placeholder}
        style={{ ...inputBase, borderColor: errors[name] ? 'rgba(255,180,171,0.6)' : '#3c4947' }}
        onFocus={e => { e.target.style.borderColor = errors[name] ? C.error : C.primary; e.target.style.boxShadow = `0 0 0 1px ${errors[name] ? C.error : C.primary}`; }}
        onBlur={e =>  { e.target.style.borderColor = errors[name] ? 'rgba(255,180,171,0.6)' : '#3c4947'; e.target.style.boxShadow = 'none'; }}
      />
      {errors[name] && <div style={{ color:C.error, fontSize:11, marginTop:5 }}>{errors[name]}</div>}
    </div>
  );

  const left = (
    <>
      <div style={{ display:'flex', alignItems:'center', gap:14, marginBottom:40 }}>
        <div style={{
          width:44, height:44, borderRadius:12,
          background:'radial-gradient(circle at 30% 30%, #2DD4BF 0%, #0EA5E9 55%, #6366F1 100%)',
          display:'flex', alignItems:'center', justifyContent:'center',
          boxShadow:'0 6px 22px rgba(20,184,166,0.4), inset 0 0 0 1px rgba(255,255,255,0.15)',
        }}>
          <svg width="26" height="26" viewBox="0 0 24 24" fill="none">
            <path d="M4 17 L9 11 L13 14 L20 6" stroke="#fff" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" />
            <circle cx="20" cy="6" r="1.8" fill="#fff" />
            <circle cx="4" cy="17" r="1.4" fill="#fff" opacity="0.8" />
          </svg>
        </div>
        <span style={{ fontSize:30, fontWeight:700, color:C.text, letterSpacing:'-0.03em', fontFamily:'"Plus Jakarta Sans", "Inter", sans-serif' }}>
          pulpe<span style={{ color:'#2DD4BF' }}>.</span>
        </span>
      </div>
      <h1 style={{ fontSize:42, fontWeight:700, color:C.text, margin:'0 0 16px', lineHeight:1.15, letterSpacing:'-0.02em' }}>
        Create Account
      </h1>
      <p style={{ fontSize:15, color:C.textMuted, lineHeight:1.7, maxWidth:440, margin:'0 0 48px' }}>
        Initialize your workspace and start analyzing 568K+ Amazon Fine Food Reviews with ML-powered sentiment intelligence.
      </p>
      <div style={{ display:'flex', gap:0 }}>
        <div style={{ paddingRight:32, borderRight:`2px solid rgba(79,219,200,0.4)` }}>
          <div style={{ fontSize:11, fontFamily:'JetBrains Mono,monospace', color:C.primary, textTransform:'uppercase', letterSpacing:'0.08em', marginBottom:6 }}>Model</div>
          <div style={{ fontSize:20, fontWeight:700, color:C.text }}>LogReg TF-IDF</div>
        </div>
        <div style={{ paddingLeft:32 }}>
          <div style={{ fontSize:11, fontFamily:'JetBrains Mono,monospace', color:C.tertiary, textTransform:'uppercase', letterSpacing:'0.08em', marginBottom:6 }}>Accuracy</div>
          <div style={{ fontSize:20, fontWeight:700, color:C.text }}>F1: 0.827</div>
        </div>
      </div>
    </>
  );

  const right = (
    <div style={{ width:'100%', maxWidth:340 }}>
      <h2 style={{ fontSize:28, fontWeight:700, color:C.text, margin:'0 0 6px', letterSpacing:'-0.02em' }}>Create Account</h2>
      <p style={{ fontSize:13, color:C.textMuted, margin:'0 0 24px' }}>Initialize your workspace and start analyzing.</p>

      <form onSubmit={handleSubmit} style={{ display:'flex', flexDirection:'column', gap:16 }}>
        <Field name="username" label="Username"         placeholder="e.g. sysadmin_01" />
        <Field name="email"    label="Email Address"    placeholder="name@company.com" type="email" />
        <Field name="password" label="Password"         placeholder="••••••••"         type="password" />
        <Field name="confirm"  label="Confirm Password" placeholder="••••••••"         type="password" />

        <div style={{ display:'flex', alignItems:'flex-start', gap:10, marginTop:4 }}>
          <input type="checkbox" id="terms" checked={terms} onChange={e => setTerms(e.target.checked)}
            style={{ width:16, height:16, marginTop:2, accentColor:C.primary, cursor:'pointer', flexShrink:0 }} />
          <label htmlFor="terms" style={{ fontSize:12, color:C.textMuted, cursor:'pointer', lineHeight:1.6 }}>
            I agree to the <span style={{ color:C.primary }}>Terms of Service</span> and <span style={{ color:C.primary }}>Privacy Policy</span>.
          </label>
        </div>
        {errors.terms && <div style={{ color:C.error, fontSize:11, marginTop:-8 }}>{errors.terms}</div>}

        <button type="submit" disabled={loading} style={{
          width:'100%', padding:'13px', borderRadius:4, border:'none',
          background: loading ? 'rgba(79,219,200,0.6)' : C.primary,
          color: C.onPrimary, fontSize:15, fontWeight:700,
          cursor: loading ? 'wait' : 'pointer', fontFamily:'Inter,sans-serif',
          display:'flex', alignItems:'center', justifyContent:'center', gap:8,
          transition:'background 0.2s', boxShadow:'0 0 20px rgba(79,219,200,0.2)',
          marginTop:6,
        }}
        onMouseEnter={e => { if(!loading) e.currentTarget.style.background=C.primaryFixed; }}
        onMouseLeave={e => { if(!loading) e.currentTarget.style.background=C.primary; }}
        >
          {loading
            ? <><span className="material-symbols-outlined" style={{ fontSize:18, animation:'spin 1s linear infinite' }}>refresh</span>Creating…</>
            : <>Create Account <span className="material-symbols-outlined" style={{ fontSize:18 }}>arrow_forward</span></>
          }
        </button>
      </form>

      <div style={{ marginTop:24, paddingTop:20, borderTop:'1px solid rgba(60,73,71,0.5)', textAlign:'center' }}>
        <p style={{ fontSize:12, color:C.textMuted, margin:0 }}>
          Already have an account?{' '}
          <button onClick={() => setPage('login')} style={{ background:'none', border:'none', color:C.primary, cursor:'pointer', fontSize:12, fontWeight:600, padding:0, fontFamily:'Inter,sans-serif' }}>
            Sign in
          </button>
        </p>
      </div>
    </div>
  );

  return <AuthShell leftContent={left} rightContent={right} />;
}

Object.assign(window, { LoginPage, RegisterPage });
