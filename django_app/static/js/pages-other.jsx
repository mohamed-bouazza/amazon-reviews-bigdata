
// ── Pipeline Control Page ────────────────────────────────────
function PipelinePage({ addToast }) {
  // ── Real backend state ─────────────────────────────────────
  const [status, setStatus]             = React.useState('stopped');
  const [producerInfo, setProducerInfo] = React.useState({ pid: '—', alive: false });
  const [consumerInfo, setConsumerInfo] = React.useState({ pid: '—', alive: false });
  const [startedAt, setStartedAt]       = React.useState(null);
  const [producerLogs, setProducerLogs] = React.useState([]);
  const [sparkLogs, setSparkLogs]       = React.useState([]);
  const [confirm, setConfirm]           = React.useState(false);
  const [busy, setBusy]                 = React.useState(false);
  const prodLogRef  = React.useRef(null);
  const sparkLogRef = React.useRef(null);

  // Auto-scroll logs
  React.useEffect(() => {
    if (prodLogRef.current)  prodLogRef.current.scrollTop  = prodLogRef.current.scrollHeight;
    if (sparkLogRef.current) sparkLogRef.current.scrollTop = sparkLogRef.current.scrollHeight;
  }, [producerLogs, sparkLogs]);

  const refresh = React.useCallback(async () => {
    if (!window.API) return;
    try {
      const s = await window.API.pipelineStatus();
      setStatus(s.status || 'stopped');
      setProducerInfo({ pid: (s.producer && s.producer.pid) || '—', alive: !!(s.producer && s.producer.alive) });
      setConsumerInfo({ pid: (s.consumer && s.consumer.pid) || '—', alive: !!(s.consumer && s.consumer.alive) });
      setStartedAt(s.started_at || null);
    } catch (e) { /* ignore */ }
    try {
      const logs = await window.API.pipelineLogs(60);
      setProducerLogs(logs.producer || []);
      setSparkLogs(logs.spark || []);
    } catch (e) { /* ignore */ }
  }, []);

  // Initial fetch + poll every 3s
  React.useEffect(() => {
    refresh();
    const tid = setInterval(refresh, 3000);
    return () => clearInterval(tid);
  }, [refresh]);

  const handleAction = async (act) => {
    if (busy) return;
    if (act === 'stop' && status === 'running') { setConfirm(true); return; }
    if (act === 'start' && status !== 'running') {
      setBusy(true);
      try {
        const r = await window.API.pipelineStart();
        addToast('success', `Pipeline started (producer PID ${r.producer && r.producer.pid}, consumer PID ${r.consumer && r.consumer.pid}).`);
        await refresh();
      } catch (e) {
        addToast('error', `Start failed: ${e.message}`);
      } finally { setBusy(false); }
    }
  };
  const handleConfirm = async () => {
    setConfirm(false);
    setBusy(true);
    try {
      await window.API.pipelineStop();
      addToast('warning', 'Pipeline stopped.');
      await refresh();
    } catch (e) {
      addToast('error', `Stop failed: ${e.message}`);
    } finally { setBusy(false); }
  };

  // Compute uptime string from startedAt
  const uptimeStr = React.useMemo(() => {
    if (!startedAt) return '—';
    const ms = Date.now() - new Date(startedAt).getTime();
    if (ms < 0) return '—';
    const s = Math.floor(ms / 1000);
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    return `${h}h ${m}m ${sec}s`;
  }, [startedAt, status]);

  // Adapter for the rest of the JSX which expects D.producer / D.sparkConsumer
  const D = {
    producer:      { pid: producerInfo.pid, uptime: uptimeStr },
    sparkConsumer: { pid: consumerInfo.pid, uptime: uptimeStr },
  };
  const uptime = { producer: uptimeStr, spark: uptimeStr };
  const msgs = producerLogs.length;
  const batches = sparkLogs.length;

  const logStyle = {
    background: '#070F1A', borderRadius: 8, border: '1px solid var(--border2)',
    padding: '12px 14px', height: 340, overflowY: 'auto', fontFamily: 'JetBrains Mono,monospace',
    fontSize: 11.5, lineHeight: 1.8, color: 'var(--text-secondary)',
  };
  const colorLine = (line) => {
    if (line.includes('ERROR')) return '#EF4444';
    if (line.includes('WARN')) return '#F59E0B';
    if (line.includes('INFO')) return '#94A3B8';
    return '#64748B';
  };
  const highlightLine = (line) => {
    const parts = line.match(/^(\[[\d\- :]+\])\s+(INFO|WARN|ERROR|DEBUG)?\s*(.*)$/);
    if (!parts) return <span>{line}</span>;
    const [, ts, level, rest] = parts;
    const lc = level === 'ERROR' ? '#EF4444' : level === 'WARN' ? '#F59E0B' : '#14B8A6';
    return <>
      <span style={{ color: '#334155' }}>{ts} </span>
      {level && <span style={{ color: lc, fontWeight: 700 }}>{level}  </span>}
      <span style={{ color: colorLine(line) }}>{rest}</span>
    </>;
  };

  return (
    <div style={{ padding: '28px 32px', maxWidth: 1100, margin: '0 auto', fontFamily: 'Inter,sans-serif' }}>
      <h1 style={{ color: 'var(--text-primary)', fontSize: 26, fontWeight: 700, margin: '0 0 4px' }}>Pipeline Control</h1>
      <p style={{ color: 'var(--text-muted)', fontSize: 13, margin: '0 0 28px' }}>Manage the Kafka producer and Spark streaming consumer.</p>

      {/* Status + controls */}
      <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12, padding: '28px 32px', marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 32, flexWrap: 'wrap' }}>
          <div>
            <div style={{ color: 'var(--text-muted)', fontSize: 11, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: 10 }}>Pipeline Status</div>
            <StatusBadge status={status} />
          </div>
          <div style={{ flex: 1 }}></div>
          <button onClick={() => handleAction('start')} disabled={status === 'running'} style={{
            padding: '14px 32px', borderRadius: 10, border: 'none',
            background: status === 'running' ? '#1E293B' : 'linear-gradient(135deg,#14B8A6,#0EA5E9)',
            color: status === 'running' ? '#334155' : '#fff',
            fontSize: 15, fontWeight: 700, cursor: status === 'running' ? 'not-allowed' : 'pointer',
            fontFamily: 'Inter,sans-serif', display: 'flex', alignItems: 'center', gap: 8,
            boxShadow: status !== 'running' ? '0 4px 16px rgba(20,184,166,0.3)' : 'none',
            transition: 'all 0.2s',
          }}>
            <span className="material-icons" style={{ fontSize: 20 }}>play_arrow</span>
            Start Pipeline
          </button>
          <button onClick={() => handleAction('stop')} disabled={status !== 'running'} style={{
            padding: '14px 32px', borderRadius: 10, border: 'none',
            background: status !== 'running' ? '#1E293B' : 'rgba(239,68,68,0.15)',
            color: status !== 'running' ? '#334155' : '#EF4444',
            fontSize: 15, fontWeight: 700, cursor: status !== 'running' ? 'not-allowed' : 'pointer',
            fontFamily: 'Inter,sans-serif', display: 'flex', alignItems: 'center', gap: 8,
            border: status === 'running' ? '1px solid rgba(239,68,68,0.4)' : '1px solid #334155',
            transition: 'all 0.2s',
          }}>
            <span className="material-icons" style={{ fontSize: 20 }}>stop</span>
            Stop Pipeline
          </button>
        </div>

        {/* Process details */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginTop: 28 }}>
          {[
            { title: 'Kafka Producer', icon: 'upload', color: '#14B8A6', pid: D.producer.pid, uptime: uptime.producer, extra: { 'Messages Sent': msgs.toLocaleString() } },
            { title: 'Spark Consumer', icon: 'bolt', color: '#6366F1', pid: D.sparkConsumer.pid, uptime: uptime.spark, extra: { 'Batches Processed': batches.toLocaleString() } },
          ].map((proc, i) => (
            <div key={i} style={{ background: 'var(--bg-base)', borderRadius: 10, padding: '16px 20px', border: '1px solid var(--border2)' }}>
              <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 16 }}>
                <div style={{ width: 32, height: 32, borderRadius: 8, background: `${proc.color}22`, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <span className="material-icons" style={{ fontSize: 18, color: proc.color }}>{proc.icon}</span>
                </div>
                <span style={{ color: 'var(--text-primary)', fontSize: 14, fontWeight: 600 }}>{proc.title}</span>
                <span style={{ marginLeft: 'auto', width: 8, height: 8, borderRadius: '50%', background: status === 'running' ? '#10B981' : '#EF4444', display: 'inline-block', animation: status === 'running' ? 'pulse 2s infinite' : 'none' }}></span>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>PID</span>
                  <span style={{ color: 'var(--text-primary)', fontSize: 12, fontFamily: 'JetBrains Mono,monospace' }}>{proc.pid}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>Uptime</span>
                  <span style={{ color: '#14B8A6', fontSize: 12, fontFamily: 'JetBrains Mono,monospace' }}>{proc.uptime}</span>
                </div>
                {Object.entries(proc.extra).map(([k, v]) => (
                  <div key={k} style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>{k}</span>
                    <span style={{ color: 'var(--text-primary)', fontSize: 12, fontFamily: 'JetBrains Mono,monospace' }}>{v}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Logs */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
        <div>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, fontWeight: 600, marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}>
            <span className="material-icons" style={{ fontSize: 15, color: '#14B8A6' }}>upload</span>
            Producer Logs
          </div>
          <div style={logStyle} ref={prodLogRef}>
            {producerLogs.map((line, i) => (
              <div key={i} style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{highlightLine(line)}</div>
            ))}
          </div>
        </div>
        <div>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, fontWeight: 600, marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}>
            <span className="material-icons" style={{ fontSize: 15, color: '#6366F1' }}>bolt</span>
            Spark Consumer Logs
          </div>
          <div style={logStyle} ref={sparkLogRef}>
            {sparkLogs.map((line, i) => (
              <div key={i} style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{highlightLine(line)}</div>
            ))}
          </div>
        </div>
      </div>

      <ConfirmModal
        open={confirm}
        title="Stop the streaming pipeline?"
        message="This will terminate the Kafka producer and Spark consumer. In-flight messages may be lost. This action cannot be undone without restarting the pipeline."
        onConfirm={handleConfirm}
        onCancel={() => setConfirm(false)}
        confirmLabel="Stop Pipeline"
        confirmColor="#EF4444"
      />
    </div>
  );
}

// ── Saved Dashboards Page ────────────────────────────────────
function SavedDashboardsPage({ setPage }) {
  const [dashboards, setDashboards] = React.useState(window.MOCK.savedDashboards);
  const [deleteConfirm, setDeleteConfirm] = React.useState(null);

  const filterPills = (filters) => Object.entries(filters).map(([k,v]) => (
    <span key={k} style={{
      padding: '2px 8px', borderRadius: 999, background: 'var(--bg-base)',
      border: '1px solid var(--border)', color: 'var(--text-secondary)', fontSize: 11,
      fontFamily: 'Inter,sans-serif',
    }}>{k}: {v}</span>
  ));

  const MiniChart = ({ id }) => {
    const colors = ['#10B981','#F59E0B','#EF4444'];
    return (
      <div style={{ height: 64, display: 'flex', gap: 3, alignItems: 'flex-end', padding: '0 4px' }}>
        {[0.7,0.4,0.2,0.55,0.8,0.35,0.6,0.9].map((h,i) => (
          <div key={i} style={{ flex: 1, height: `${h*100}%`, borderRadius: '3px 3px 0 0', background: colors[i%3], opacity: 0.7 }}></div>
        ))}
      </div>
    );
  };

  return (
    <div style={{ padding: '28px 32px', maxWidth: 1440, margin: '0 auto', fontFamily: 'Inter,sans-serif' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', marginBottom: 28 }}>
        <div>
          <h1 style={{ color: 'var(--text-primary)', fontSize: 26, fontWeight: 700, margin: '0 0 4px' }}>Saved Dashboards</h1>
          <p style={{ color: 'var(--text-muted)', fontSize: 13, margin: 0 }}>Your saved filter views and dashboard configurations.</p>
        </div>
        <button style={{
          padding: '9px 18px', borderRadius: 8, border: 'none',
          background: 'linear-gradient(135deg,#14B8A6,#0EA5E9)', color: '#fff',
          fontSize: 13, fontWeight: 600, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6,
          fontFamily: 'Inter,sans-serif',
        }}>
          <span className="material-icons" style={{ fontSize: 16 }}>add</span> Save current view
        </button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: 20 }}>
        {dashboards.map(dash => (
          <div key={dash.id} style={{
            background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12, overflow: 'hidden',
            transition: 'border-color 0.2s, box-shadow 0.2s', cursor: 'pointer',
          }}
          onMouseEnter={e => { e.currentTarget.style.borderColor='#14B8A6'; e.currentTarget.style.boxShadow='0 4px 20px rgba(20,184,166,0.1)'; }}
          onMouseLeave={e => { e.currentTarget.style.borderColor='#334155'; e.currentTarget.style.boxShadow='none'; }}
          >
            {/* Chart thumbnail */}
            <div style={{ background: 'var(--bg-base)', borderBottom: '1px solid var(--border2)', padding: '12px 16px 0' }}>
              <MiniChart id={dash.id} />
            </div>

            <div style={{ padding: '16px 20px' }}>
              <div style={{ color: 'var(--text-primary)', fontSize: 15, fontWeight: 600, marginBottom: 8 }}>{dash.title}</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 12 }}>
                {filterPills(dash.filters)}
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                  <div style={{ color: 'var(--text-muted)', fontSize: 11 }}>Last viewed: <span style={{ color: 'var(--text-secondary)' }}>{dash.lastViewed}</span></div>
                  <div style={{ color: 'var(--text-muted)', fontSize: 11 }}>{dash.count.toLocaleString()} predictions</div>
                </div>
                <div style={{ display: 'flex', gap: 6 }}>
                  <button onClick={() => setPage('dashboard')} style={{
                    padding: '6px 12px', borderRadius: 6, border: 'none',
                    background: 'rgba(20,184,166,0.15)', color: '#14B8A6',
                    cursor: 'pointer', fontSize: 12, fontWeight: 600, fontFamily: 'Inter,sans-serif',
                  }}>Open</button>
                  <button onClick={e => { e.stopPropagation(); setDeleteConfirm(dash.id); }} style={{
                    padding: '6px 10px', borderRadius: 6, border: '1px solid var(--border)',
                    background: 'transparent', color: 'var(--text-muted)', cursor: 'pointer',
                  }}>
                    <span className="material-icons" style={{ fontSize: 14 }}>delete</span>
                  </button>
                </div>
              </div>
            </div>
          </div>
        ))}

        {/* Empty state */}
        {dashboards.length === 0 && (
          <div style={{ gridColumn: '1/-1', textAlign: 'center', padding: '80px 0', color: 'var(--text-muted)' }}>
            <span className="material-icons" style={{ fontSize: 48, display: 'block', marginBottom: 12 }}>bookmark_border</span>
            <div style={{ fontSize: 16, fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 6 }}>No saved dashboards</div>
            <div style={{ fontSize: 13 }}>Save a filtered view from the main dashboard.</div>
          </div>
        )}
      </div>

      <ConfirmModal
        open={!!deleteConfirm}
        title="Delete saved dashboard?"
        message="This will permanently remove the saved dashboard view. This action cannot be undone."
        onConfirm={() => { setDashboards(prev => prev.filter(d => d.id !== deleteConfirm)); setDeleteConfirm(null); }}
        onCancel={() => setDeleteConfirm(null)}
        confirmLabel="Delete"
        confirmColor="#EF4444"
      />
    </div>
  );
}

// ── Admin Page ───────────────────────────────────────────────
function AdminPage({ setPage }) {
  const sections = [
    {
      title: 'User Management',
      icon: 'group',
      color: '#14B8A6',
      desc: 'Manage user accounts, roles, and permissions.',
      stats: [{ label: 'Active Users', value: '8' }, { label: 'Admins', value: '2' }],
      link: 'Django Admin → Users',
    },
    {
      title: 'ML Model Registry',
      icon: 'model_training',
      color: '#6366F1',
      desc: 'View deployed models, metrics, and version history.',
      stats: [{ label: 'Primary Model', value: 'LogReg TF-IDF (F1: 0.827)' }, { label: 'Status', value: 'Active' }],
      link: 'Django Admin → Models',
    },
    {
      title: 'MongoDB Collections',
      icon: 'storage',
      color: '#10B981',
      desc: 'Inspect raw collections, indexes, and document stats.',
      stats: [{ label: 'Total Documents', value: '568,454' }, { label: 'Collections', value: '3' }],
      link: 'Django Admin → MongoDB',
    },
    {
      title: 'Audit Logs',
      icon: 'history',
      color: '#F59E0B',
      desc: 'Track all user actions and system events.',
      stats: [{ label: 'Events (30d)', value: '1,284' }, { label: 'Errors', value: '3' }],
      link: 'Django Admin → Audit',
    },
    {
      title: 'System Health',
      icon: 'monitor_heart',
      color: '#EF4444',
      desc: 'Monitor server health, memory, CPU, and disk usage.',
      stats: [{ label: 'CPU', value: '12%' }, { label: 'Memory', value: '2.1 GB' }],
      link: 'Django Admin → System',
    },
    {
      title: 'Kafka & Spark Config',
      icon: 'settings_ethernet',
      color: '#EC4899',
      desc: 'Configure Kafka brokers, topics, and Spark settings.',
      stats: [{ label: 'Broker', value: 'localhost:9092' }, { label: 'Topic', value: 'amazon_reviews' }],
      link: 'Django Admin → Config',
    },
  ];

  return (
    <div style={{ padding: '28px 32px', maxWidth: 1200, margin: '0 auto', fontFamily: 'Inter,sans-serif' }}>
      <div style={{ marginBottom: 32 }}>
        <h1 style={{ color: 'var(--text-primary)', fontSize: 26, fontWeight: 700, margin: '0 0 4px' }}>Admin Panel</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: 13, margin: 0 }}>System administration and configuration — links to Django admin sections.</p>
      </div>

      {/* Quick stats */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 32 }}>
        {[
          { label: 'Active Users', value: '8', icon: 'group', color: '#14B8A6' },
          { label: 'Pipeline Status', value: 'Running', icon: 'sensors', color: '#10B981' },
          { label: 'Total Records', value: '568,454', icon: 'bar_chart', color: '#6366F1' },
          { label: 'Primary Model', value: 'LogReg TF-IDF', icon: 'model_training', color: '#F59E0B' },
        ].map((s, i) => (
          <div key={i} style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 10, padding: '16px 18px' }}>
            <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 8 }}>
              <div style={{ width: 30, height: 30, borderRadius: 7, background: `${s.color}22`, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <span className="material-icons" style={{ fontSize: 16, color: s.color }}>{s.icon}</span>
              </div>
              <span style={{ color: 'var(--text-muted)', fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{s.label}</span>
            </div>
            <div style={{ color: 'var(--text-primary)', fontSize: 16, fontWeight: 700 }}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* Admin section cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 20 }}>
        {sections.map((sec, i) => (
          <div key={i} style={{
            background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12, padding: '20px 22px',
            transition: 'border-color 0.2s, box-shadow 0.2s',
          }}
          onMouseEnter={e => { e.currentTarget.style.borderColor=sec.color+'66'; e.currentTarget.style.boxShadow=`0 4px 20px ${sec.color}18`; }}
          onMouseLeave={e => { e.currentTarget.style.borderColor='#334155'; e.currentTarget.style.boxShadow='none'; }}
          >
            <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start', marginBottom: 12 }}>
              <div style={{ width: 40, height: 40, borderRadius: 10, background: `${sec.color}22`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                <span className="material-icons" style={{ fontSize: 22, color: sec.color }}>{sec.icon}</span>
              </div>
              <div>
                <div style={{ color: 'var(--text-primary)', fontSize: 14, fontWeight: 600 }}>{sec.title}</div>
                <div style={{ color: 'var(--text-muted)', fontSize: 12, marginTop: 2, lineHeight: 1.5 }}>{sec.desc}</div>
              </div>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginBottom: 16 }}>
              {sec.stats.map((s, j) => (
                <div key={j} style={{ display: 'flex', justifyContent: 'space-between', padding: '6px 10px', background: 'var(--bg-card2)', borderRadius: 6 }}>
                  <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>{s.label}</span>
                  <span style={{ color: 'var(--text-primary)', fontSize: 11, fontWeight: 600, fontFamily: 'JetBrains Mono,monospace' }}>{s.value}</span>
                </div>
              ))}
            </div>

            <button style={{
              width: '100%', padding: '8px', borderRadius: 8, border: `1px solid ${sec.color}44`,
              background: `${sec.color}11`, color: sec.color, cursor: 'pointer',
              fontSize: 12, fontWeight: 600, fontFamily: 'Inter,sans-serif',
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
            }}>
              <span className="material-icons" style={{ fontSize: 14 }}>open_in_new</span>
              {sec.link}
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}

Object.assign(window, { PipelinePage, SavedDashboardsPage, AdminPage });
