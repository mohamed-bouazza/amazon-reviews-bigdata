
// ============================================================
// Shared Components — Amazon Fine Food Reviews Dashboard
// ============================================================

// ── Sidebar ─────────────────────────────────────────────────
function Sidebar({ currentPage, setPage, collapsed, setCollapsed, darkMode }) {
  const navItems = [
    { id: 'dashboard', icon: 'dashboard', label: 'Dashboard' },
    { id: 'live',      icon: 'sensors',   label: 'Live Feed' },
    { id: 'product',   icon: 'inventory_2', label: 'Product Detail' },
    { id: 'pipeline',  icon: 'settings_ethernet', label: 'Pipeline Control' },
    { id: 'saved',     icon: 'bookmark',  label: 'Saved Dashboards' },
    { id: 'admin',     icon: 'admin_panel_settings', label: 'Admin' },
  ];
  // Sidebar is always dark regardless of theme (like most SaaS apps)
  return (
    <aside style={{
      width: collapsed ? 64 : 240,
      minWidth: collapsed ? 64 : 240,
      background: 'var(--bg-base)',
      borderRight: '1px solid var(--border2)',
      display: 'flex',
      flexDirection: 'column',
      transition: 'width 0.25s ease, min-width 0.25s ease',
      overflow: 'hidden',
      zIndex: 100,
      boxShadow: '2px 0 12px rgba(0,0,0,0.3)',
    }}>
      {/* Logo */}
      <div style={{ padding: '22px 16px 18px', borderBottom: '1px solid var(--border2)', display: 'flex', alignItems: 'center', gap: 12, minHeight: 68 }}>
        <div style={{
          width: 34, height: 34, borderRadius: 10, flexShrink: 0,
          background: 'radial-gradient(circle at 30% 30%, #2DD4BF 0%, #0EA5E9 55%, #6366F1 100%)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          boxShadow: '0 4px 14px rgba(20,184,166,0.35), inset 0 0 0 1px rgba(255,255,255,0.12)',
        }}>
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M4 17 L9 11 L13 14 L20 6" stroke="#fff" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" />
            <circle cx="20" cy="6" r="1.8" fill="#fff" />
            <circle cx="4" cy="17" r="1.4" fill="#fff" opacity="0.8" />
          </svg>
        </div>
        {!collapsed && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <div style={{
              color: 'var(--text-primary)', fontWeight: 700, fontSize: 17, lineHeight: 1.1,
              fontFamily: '"Plus Jakarta Sans", "Inter", sans-serif', letterSpacing: '-0.025em',
            }}>
              pulpe<span style={{ color: '#2DD4BF' }}>.</span>
            </div>
            <div style={{
              color: 'var(--text-secondary)', fontSize: 9, lineHeight: 1.2, fontWeight: 500,
              fontFamily: '"JetBrains Mono", monospace', letterSpacing: '0.14em', textTransform: 'uppercase',
            }}>
              sentiment · live
            </div>
          </div>
        )}
      </div>

      {/* Nav */}
      <nav style={{ flex: 1, padding: '12px 8px', display: 'flex', flexDirection: 'column', gap: 2 }}>
        {navItems.map(item => {
          const active = currentPage === item.id;
          return (
            <button key={item.id} onClick={() => setPage(item.id)} style={{
              display: 'flex', alignItems: 'center', gap: 12,
              padding: collapsed ? '10px 16px' : '10px 12px',
              borderRadius: 8, border: 'none', cursor: 'pointer',
              background: active ? 'rgba(20,184,166,0.15)' : 'transparent',
              color: active ? '#14B8A6' : '#94A3B8',
              fontFamily: 'Inter,sans-serif', fontSize: 14, fontWeight: active ? 600 : 400,
              transition: 'all 0.15s ease',
              justifyContent: collapsed ? 'center' : 'flex-start',
              whiteSpace: 'nowrap', width: '100%',
            }}
            onMouseEnter={e => { if (!active) { e.currentTarget.style.background='rgba(255,255,255,0.05)'; e.currentTarget.style.color='#F1F5F9'; } }}
            onMouseLeave={e => { if (!active) { e.currentTarget.style.background='transparent'; e.currentTarget.style.color='#94A3B8'; } }}
            >
              <span className="material-icons" style={{ fontSize: 20, flexShrink: 0 }}>{item.icon}</span>
              {!collapsed && <span>{item.label}</span>}
            </button>
          );
        })}
      </nav>

      {/* Collapse toggle */}
      <div style={{ padding: '12px 8px', borderTop: '1px solid var(--border2)' }}>
        <button onClick={() => setCollapsed(!collapsed)} style={{
          width: '100%', display: 'flex', alignItems: 'center', justifyContent: collapsed ? 'center' : 'flex-start',
          gap: 12, padding: '8px 12px', borderRadius: 8, border: 'none',
          background: 'transparent', color: 'var(--text-muted)', cursor: 'pointer',
          fontFamily: 'Inter,sans-serif', fontSize: 13,
        }}>
          <span className="material-icons" style={{ fontSize: 18, transition: 'transform 0.25s', transform: collapsed ? 'rotate(180deg)' : 'none' }}>chevron_left</span>
          {!collapsed && <span>Collapse</span>}
        </button>
      </div>
    </aside>
  );
}

// ── Top Navbar ───────────────────────────────────────────────
function Topbar({ darkMode, setDarkMode, currentPage, setPage }) {
  const [userOpen, setUserOpen] = React.useState(false);
  const [notifOpen, setNotifOpen] = React.useState(false);
  const pageTitles = {
    dashboard: 'Dashboard', live: 'Live Feed', product: 'Product Detail',
    pipeline: 'Pipeline Control', saved: 'Saved Dashboards', admin: 'Admin', login: 'Login', register: 'Register'
  };
  return (
    <header style={{
      height: 60, background: 'var(--topbar-bg)', borderBottom: '1px solid var(--border2)',
      display: 'flex', alignItems: 'center', padding: '0 24px', gap: 16, flexShrink: 0, zIndex: 50,
      transition: 'background 0.25s ease',
    }}>
      <div style={{ flex: 1, display: 'flex', alignItems: 'center', gap: 12 }}>
        <span style={{ color: 'var(--text-primary)', fontFamily: 'Inter,sans-serif', fontWeight: 600, fontSize: 16 }}>{pageTitles[currentPage] || 'Dashboard'}</span>
        <span style={{ color: 'var(--border)', fontSize: 14 }}>/</span>
        <span style={{ color: 'var(--text-secondary)', fontSize: 13, fontFamily: 'Inter,sans-serif' }}>Amazon Fine Food Reviews</span>
      </div>

      {/* Search */}
      <div style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
        <span className="material-icons" style={{ position: 'absolute', left: 10, fontSize: 18, color: 'var(--text-muted)' }}>search</span>
        <input placeholder="Search products, reviews..." style={{
          background: 'var(--bg-input)', border: '1px solid var(--border)', borderRadius: 8,
          padding: '7px 12px 7px 34px', color: 'var(--text-primary)', fontSize: 13,
          fontFamily: 'Inter,sans-serif', outline: 'none', width: 220,
          transition: 'background 0.2s, border-color 0.2s',
        }} />
      </div>

      {/* Notif */}
      <div style={{ position: 'relative' }}>
        <button onClick={() => { setNotifOpen(!notifOpen); setUserOpen(false); }} style={{
          background: 'transparent', border: 'none', cursor: 'pointer',
          color: 'var(--text-secondary)', padding: 6, borderRadius: 8, position: 'relative',
          display: 'flex', alignItems: 'center',
        }}>
          <span className="material-icons" style={{ fontSize: 22 }}>notifications</span>
          <span style={{
            position: 'absolute', top: 4, right: 4, width: 8, height: 8,
            background: '#14B8A6', borderRadius: '50%', border: '2px solid var(--topbar-bg)',
          }}></span>
        </button>
        {notifOpen && (
          <div style={{
            position: 'absolute', right: 0, top: 42, background: 'var(--bg-card)',
            border: '1px solid var(--border)', borderRadius: 12, width: 300, padding: 8, zIndex: 200,
            boxShadow: '0 8px 32px rgba(0,0,0,0.3)',
          }}>
            <div style={{ padding: '8px 12px', color: 'var(--text-muted)', fontSize: 12, fontFamily: 'Inter,sans-serif', borderBottom: '1px solid var(--border)', marginBottom: 4 }}>Notifications</div>
            {[
              { icon: 'check_circle', color: '#10B981', msg: 'Pipeline completed: 568,454 records', time: '2h ago' },
              { icon: 'warning', color: '#F59E0B', msg: 'Low confidence predictions detected (batch #158)', time: '3h ago' },
              { icon: 'info', color: '#3B82F6', msg: 'Model evaluation: F1-Score 82.7%', time: '1d ago' },
            ].map((n, i) => (
              <div key={i} style={{ display: 'flex', gap: 10, padding: '8px 12px', borderRadius: 8, alignItems: 'flex-start' }}>
                <span className="material-icons" style={{ fontSize: 16, color: n.color, marginTop: 2 }}>{n.icon}</span>
                <div>
                  <div style={{ color: 'var(--text-primary)', fontSize: 12, fontFamily: 'Inter,sans-serif' }}>{n.msg}</div>
                  <div style={{ color: 'var(--text-muted)', fontSize: 11, fontFamily: 'Inter,sans-serif', marginTop: 2 }}>{n.time}</div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Theme toggle */}
      <button onClick={() => setDarkMode(!darkMode)} style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 8,
        cursor: 'pointer', color: 'var(--text-secondary)', padding: '6px 10px',
        display: 'flex', alignItems: 'center', gap: 6, fontFamily: 'Inter,sans-serif', fontSize: 13,
        transition: 'all 0.2s ease',
      }}>
        <span className="material-icons" style={{ fontSize: 18 }}>{darkMode ? 'light_mode' : 'dark_mode'}</span>
        <span style={{ fontSize: 12 }}>{darkMode ? 'Light' : 'Dark'}</span>
      </button>

      {/* User */}
      <div style={{ position: 'relative' }}>
        <button onClick={() => { setUserOpen(!userOpen); setNotifOpen(false); }} style={{
          display: 'flex', alignItems: 'center', gap: 8, background: 'var(--bg-card)',
          border: '1px solid var(--border)', borderRadius: 8, cursor: 'pointer', padding: '6px 10px',
          transition: 'background 0.2s',
        }}>
          <div style={{
            width: 26, height: 26, borderRadius: '50%', background: 'linear-gradient(135deg,#14B8A6,#0EA5E9)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <span style={{ color: '#fff', fontSize: 12, fontWeight: 700, fontFamily: 'Inter,sans-serif' }}>A</span>
          </div>
          <span style={{ color: 'var(--text-primary)', fontSize: 13, fontFamily: 'Inter,sans-serif' }}>Admin</span>
          <span className="material-icons" style={{ fontSize: 16, color: 'var(--text-muted)' }}>expand_more</span>
        </button>
        {userOpen && (
          <div style={{
            position: 'absolute', right: 0, top: 44, background: 'var(--bg-card)',
            border: '1px solid var(--border)', borderRadius: 12, width: 180, padding: 8, zIndex: 200,
            boxShadow: '0 8px 32px rgba(0,0,0,0.3)',
          }}>
            {[
              { icon: 'person', label: 'Profile' },
              { icon: 'settings', label: 'Settings' },
              { icon: 'logout', label: 'Sign out' },
            ].map((item, i) => (
              <button key={i} onClick={() => {
                setUserOpen(false);
                if (item.label === 'Sign out') {
                  const logoutUrl = (window.DJANGO_CTX && window.DJANGO_CTX.logoutUrl) || '/logout/';
                  const csrf = (window.DJANGO_CTX && window.DJANGO_CTX.csrfToken) || '';
                  fetch(logoutUrl, { method: 'POST', headers: { 'X-CSRFToken': csrf }, credentials: 'same-origin' })
                    .finally(() => { window.location.href = '/'; });
                }
              }} style={{
                display: 'flex', alignItems: 'center', gap: 8, width: '100%',
                padding: '8px 12px', background: 'transparent', border: 'none',
                color: 'var(--text-primary)', fontSize: 13, fontFamily: 'Inter,sans-serif', cursor: 'pointer',
                borderRadius: 8, textAlign: 'left', transition: 'background 0.15s',
              }}
              onMouseEnter={e => e.currentTarget.style.background='rgba(100,116,139,0.12)'}
              onMouseLeave={e => e.currentTarget.style.background='transparent'}
              >
                <span className="material-icons" style={{ fontSize: 16, color: 'var(--text-secondary)' }}>{item.icon}</span>
                {item.label}
              </button>
            ))}
          </div>
        )}
      </div>
    </header>
  );
}

// ── Sentiment Badge ──────────────────────────────────────────
function SentimentBadge({ sentiment, large }) {
  const cfg = {
    positive: { bg: 'rgba(16,185,129,0.15)', color: '#10B981', emoji: '🟢', label: 'POSITIVE' },
    neutral:  { bg: 'rgba(245,158,11,0.15)',  color: '#F59E0B', emoji: '🟡', label: 'NEUTRAL' },
    negative: { bg: 'rgba(239,68,68,0.15)',   color: '#EF4444', emoji: '🔴', label: 'NEGATIVE' },
  };
  const c = cfg[sentiment] || cfg.neutral;
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: large ? 6 : 4,
      padding: large ? '6px 14px' : '3px 8px',
      background: c.bg, color: c.color, borderRadius: 999,
      fontSize: large ? 13 : 11, fontWeight: 700,
      fontFamily: 'Inter,sans-serif', letterSpacing: '0.04em',
      border: `1px solid ${c.color}33`,
    }}>
      {c.emoji} {c.label}
    </span>
  );
}

// ── KPI Card ─────────────────────────────────────────────────
function KpiCard({ icon, label, value, sub, subColor, iconBg, sparkData, trend }) {
  const canvasRef = React.useRef(null);
  React.useEffect(() => {
    if (!sparkData || !canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext('2d');
    const W = canvas.width, H = canvas.height;
    const max = Math.max(...sparkData);
    const min = Math.min(...sparkData);
    ctx.clearRect(0, 0, W, H);
    const pts = sparkData.map((v, i) => ({
      x: (i / (sparkData.length - 1)) * W,
      y: H - ((v - min) / (max - min + 1)) * H * 0.85 - H * 0.075,
    }));
    const grad = ctx.createLinearGradient(0, 0, 0, H);
    grad.addColorStop(0, 'rgba(20,184,166,0.3)');
    grad.addColorStop(1, 'rgba(20,184,166,0)');
    ctx.beginPath();
    ctx.moveTo(pts[0].x, H);
    pts.forEach(p => ctx.lineTo(p.x, p.y));
    ctx.lineTo(pts[pts.length-1].x, H);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();
    ctx.beginPath();
    pts.forEach((p, i) => i === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y));
    ctx.strokeStyle = 'rgba(20,184,166,0.8)';
    ctx.lineWidth = 1.5;
    ctx.stroke();
  }, [sparkData]);

  return (
    <div style={{
      background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12,
      padding: '20px 20px 16px', flex: 1, minWidth: 0, position: 'relative', overflow: 'hidden',
      transition: 'background 0.25s ease, border-color 0.25s ease',
    }}>
      {sparkData && (
        <canvas ref={canvasRef} width={140} height={52} style={{
          position: 'absolute', bottom: 0, right: 0, opacity: 0.6,
        }} />
      )}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
        <div style={{
          width: 36, height: 36, borderRadius: 8,
          background: iconBg || 'rgba(20,184,166,0.15)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <span className="material-icons" style={{ fontSize: 20, color: '#14B8A6' }}>{icon}</span>
        </div>
      </div>
      <div style={{ color: 'var(--text-secondary)', fontSize: 12, fontFamily: 'Inter,sans-serif', marginBottom: 4, fontWeight: 500, letterSpacing: '0.04em', textTransform: 'uppercase' }}>{label}</div>
      <div style={{ color: 'var(--text-primary)', fontSize: 28, fontFamily: 'Inter,sans-serif', fontWeight: 700, lineHeight: 1.1, marginBottom: 6 }}>{value}</div>
      {sub && (
        <div style={{ color: subColor || 'var(--text-secondary)', fontSize: 12, fontFamily: 'Inter,sans-serif', display: 'flex', alignItems: 'center', gap: 4 }}>
          {sub}
        </div>
      )}
    </div>
  );
}

// ── Chart Card container ─────────────────────────────────────
function ChartCard({ title, subtitle, children, style, controls }) {
  return (
    <div style={{
      background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12,
      padding: '20px 20px 16px', transition: 'background 0.25s ease, border-color 0.25s ease', ...style,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
        <div>
          <div style={{ color: 'var(--text-primary)', fontFamily: 'Inter,sans-serif', fontWeight: 600, fontSize: 15 }}>{title}</div>
          {subtitle && <div style={{ color: 'var(--text-muted)', fontSize: 12, fontFamily: 'Inter,sans-serif', marginTop: 2 }}>{subtitle}</div>}
        </div>
        {controls && <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>{controls}</div>}
      </div>
      {children}
    </div>
  );
}

// ── Pill button ──────────────────────────────────────────────
function Pill({ label, active, onClick, color }) {
  const c = color || '#14B8A6';
  return (
    <button onClick={onClick} style={{
      padding: '5px 12px', borderRadius: 999, border: `1px solid ${active ? c : 'var(--border)'}`,
      background: active ? `${c}22` : 'transparent',
      color: active ? c : 'var(--text-secondary)',
      fontSize: 12, fontFamily: 'Inter,sans-serif', fontWeight: active ? 600 : 400,
      cursor: 'pointer', transition: 'all 0.15s ease',
    }}>{label}</button>
  );
}

// ── Status Badge ─────────────────────────────────────────────
function StatusBadge({ status }) {
  const cfg = {
    running: { color: '#10B981', bg: 'rgba(16,185,129,0.12)', label: 'RUNNING' },
    stopped: { color: '#EF4444', bg: 'rgba(239,68,68,0.12)', label: 'STOPPED' },
    error:   { color: '#F59E0B', bg: 'rgba(245,158,11,0.12)', label: 'ERROR' },
  };
  const c = cfg[status] || cfg.stopped;
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 8,
      padding: '8px 20px', background: c.bg, color: c.color,
      borderRadius: 999, fontSize: 18, fontWeight: 700, fontFamily: 'Inter,sans-serif',
      border: `1px solid ${c.color}44`, letterSpacing: '0.08em',
    }}>
      <span style={{
        width: 10, height: 10, borderRadius: '50%', background: c.color,
        boxShadow: `0 0 0 3px ${c.color}33`,
        animation: status === 'running' ? 'pulse 2s infinite' : 'none',
        display: 'inline-block',
      }}></span>
      {c.label}
    </span>
  );
}

// ── Toast ────────────────────────────────────────────────────
function Toast({ toasts, dismiss }) {
  return (
    <div style={{ position: 'fixed', bottom: 24, right: 24, display: 'flex', flexDirection: 'column', gap: 8, zIndex: 9999 }}>
      {toasts.map(t => {
        const cfg = { success:'#10B981', error:'#EF4444', warning:'#F59E0B', info:'#3B82F6' };
        return (
          <div key={t.id} style={{
            background: 'var(--bg-card)', border: `1px solid ${cfg[t.type]}44`,
            borderLeft: `3px solid ${cfg[t.type]}`,
            borderRadius: 8, padding: '12px 16px', display: 'flex', gap: 10,
            alignItems: 'center', minWidth: 280, boxShadow: '0 4px 20px rgba(0,0,0,0.3)',
            animation: 'slideUp 0.3s ease',
          }}>
            <span className="material-icons" style={{ color: cfg[t.type], fontSize: 18 }}>
              {t.type === 'success' ? 'check_circle' : t.type === 'error' ? 'error' : t.type === 'warning' ? 'warning' : 'info'}
            </span>
            <span style={{ color: 'var(--text-primary)', fontSize: 13, fontFamily: 'Inter,sans-serif', flex: 1 }}>{t.msg}</span>
            <button onClick={() => dismiss(t.id)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-muted)' }}>
              <span className="material-icons" style={{ fontSize: 16 }}>close</span>
            </button>
          </div>
        );
      })}
    </div>
  );
}

// ── Confirm Modal ────────────────────────────────────────────
function ConfirmModal({ open, title, message, onConfirm, onCancel, confirmLabel, confirmColor }) {
  if (!open) return null;
  return (
    <div style={{
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)', backdropFilter: 'blur(4px)',
      display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 9000,
    }}>
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 16,
        padding: 32, maxWidth: 420, width: '90%', boxShadow: '0 20px 60px rgba(0,0,0,0.5)',
      }}>
        <div style={{ color: 'var(--text-primary)', fontSize: 18, fontWeight: 700, fontFamily: 'Inter,sans-serif', marginBottom: 12 }}>{title}</div>
        <div style={{ color: 'var(--text-secondary)', fontSize: 14, fontFamily: 'Inter,sans-serif', marginBottom: 28, lineHeight: 1.6 }}>{message}</div>
        <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end' }}>
          <button onClick={onCancel} style={{
            padding: '9px 20px', borderRadius: 8, border: '1px solid var(--border)',
            background: 'transparent', color: 'var(--text-secondary)', cursor: 'pointer',
            fontFamily: 'Inter,sans-serif', fontSize: 14,
          }}>Cancel</button>
          <button onClick={onConfirm} style={{
            padding: '9px 20px', borderRadius: 8, border: 'none',
            background: confirmColor || '#EF4444', color: '#fff', cursor: 'pointer',
            fontFamily: 'Inter,sans-serif', fontSize: 14, fontWeight: 600,
          }}>{confirmLabel || 'Confirm'}</button>
        </div>
      </div>
    </div>
  );
}

// ── Loading Skeleton ─────────────────────────────────────────
function Skeleton({ width, height, style }) {
  return (
    <div style={{
      width: width || '100%', height: height || 20, borderRadius: 6,
      background: 'linear-gradient(90deg, var(--bg-card) 25%, var(--border) 50%, var(--bg-card) 75%)',
      backgroundSize: '200% 100%', animation: 'shimmer 1.5s infinite', ...style,
    }} />
  );
}

// ── Pagination ───────────────────────────────────────────────
function Pagination({ page, total, perPage, onChange }) {
  const pages = Math.ceil(total / perPage);
  return (
    <div style={{ display: 'flex', gap: 6, alignItems: 'center', justifyContent: 'flex-end' }}>
      <button onClick={() => onChange(page - 1)} disabled={page <= 1} style={{
        padding: '5px 10px', borderRadius: 6, border: '1px solid var(--border)',
        background: 'transparent', color: page <= 1 ? 'var(--border)' : 'var(--text-secondary)',
        cursor: page <= 1 ? 'not-allowed' : 'pointer', fontFamily: 'Inter,sans-serif', fontSize: 13,
      }}>
        <span className="material-icons" style={{ fontSize: 16, verticalAlign: 'middle' }}>chevron_left</span>
      </button>
      {Array.from({length: pages}, (_, i) => i + 1).map(p => (
        <button key={p} onClick={() => onChange(p)} style={{
          width: 32, height: 32, borderRadius: 6,
          border: p === page ? 'none' : '1px solid var(--border)',
          background: p === page ? '#14B8A6' : 'transparent',
          color: p === page ? '#fff' : 'var(--text-secondary)',
          cursor: 'pointer', fontFamily: 'Inter,sans-serif', fontSize: 13, fontWeight: p === page ? 600 : 400,
        }}>{p}</button>
      ))}
      <button onClick={() => onChange(page + 1)} disabled={page >= pages} style={{
        padding: '5px 10px', borderRadius: 6, border: '1px solid var(--border)',
        background: 'transparent', color: page >= pages ? 'var(--border)' : 'var(--text-secondary)',
        cursor: page >= pages ? 'not-allowed' : 'pointer', fontFamily: 'Inter,sans-serif', fontSize: 13,
      }}>
        <span className="material-icons" style={{ fontSize: 16, verticalAlign: 'middle' }}>chevron_right</span>
      </button>
    </div>
  );
}

// ── Confidence bar ───────────────────────────────────────────
function ConfidenceBar({ value }) {
  if (value === null || value === undefined || isNaN(value)) {
    return <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>—</span>;
  }
  const pct = Math.round(value * 100);
  const color = pct >= 85 ? '#10B981' : pct >= 65 ? '#F59E0B' : '#EF4444';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <div style={{ flex: 1, height: 6, background: 'var(--bg-main)', borderRadius: 3, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 3, transition: 'width 0.4s ease' }} />
      </div>
      <span style={{ color, fontSize: 11, fontFamily: 'Inter,sans-serif', fontWeight: 600, minWidth: 30 }}>{pct}%</span>
    </div>
  );
}

// Export all to window
Object.assign(window, {
  Sidebar, Topbar, SentimentBadge, KpiCard, ChartCard,
  Pill, StatusBadge, Toast, ConfirmModal, Skeleton,
  Pagination, ConfidenceBar,
});
