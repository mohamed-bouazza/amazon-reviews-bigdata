
// ── Live Feed Page ───────────────────────────────────────────
function LiveFeedPage({ setPage, setSelectedProduct }) {
  const D = window.MOCK;

  const [paused, setPaused]         = React.useState(false);
  const [feeds, setFeeds]           = React.useState(D.recentPredictions ? D.recentPredictions.slice(0, 20) : []);
  const [counters, setCounters]     = React.useState({
    positive: D.distribution ? D.distribution.positive || 0 : 0,
    neutral:  D.distribution ? D.distribution.neutral  || 0 : 0,
    negative: D.distribution ? D.distribution.negative || 0 : 0,
  });
  const [throughput, setThroughput] = React.useState(D.kpi ? D.kpi.throughput || 0 : 0);
  const [wsStatus, setWsStatus]     = React.useState('connecting');
  const [sentFilter, setSentFilter] = React.useState(['positive','neutral','negative']);
  const [sidebarOpen, setSidebarOpen] = React.useState(true);

  const donutRef   = React.useRef(null);
  const lineRef    = React.useRef(null);
  const donutChart = React.useRef(null);
  const lineChart  = React.useRef(null);
  const throughputHistory = React.useRef(Array(60).fill(0));
  const pausedRef  = React.useRef(false);

  // Keep pausedRef in sync so WebSocket callback can check it without stale closure
  React.useEffect(() => { pausedRef.current = paused; }, [paused]);

  // ── WebSocket live feed ──────────────────────────────────────
  React.useEffect(() => {
    if (!window.API) return;
    const ws = window.API.connectLiveFeed({
      onOpen:  () => setWsStatus('connected'),
      onClose: () => setWsStatus('reconnecting'),
      onError: () => setWsStatus('error'),
      onMessage: (msg) => {
        if (msg.type === 'init') {
          if (msg.predictions && msg.predictions.length > 0) {
            setFeeds(msg.predictions);
          }
          if (msg.counters) setCounters(msg.counters);
        } else if (msg.type === 'batch') {
          if (!pausedRef.current && msg.predictions && msg.predictions.length > 0) {
            setFeeds(prev => [...msg.predictions, ...prev].slice(0, 50));
          }
          if (msg.counters) setCounters(msg.counters);
        } else if (msg.type === 'pong') {
          // ignore
        }
      },
    });
    // Ping every 30s to keep connection alive
    const pingId = setInterval(() => ws && ws.send({ type: 'ping' }), 30000);
    return () => { clearInterval(pingId); if (ws) ws.close(); };
  }, []);

  // ── Poll throughput from KPI every 10s ──────────────────────
  React.useEffect(() => {
    if (!window.API) return;
    const poll = async () => {
      try {
        const kpi = await window.API.getKpi();
        if (kpi && kpi.throughput !== undefined) {
          const tp = parseFloat(kpi.throughput) || 0;
          setThroughput(tp);
          throughputHistory.current = [...throughputHistory.current.slice(1), tp];
          if (lineChart.current) {
            lineChart.current.data.datasets[0].data = [...throughputHistory.current];
            lineChart.current.update('none');
          }
        }
        if (kpi) {
          setCounters(prev => {
            const dist = window.MOCK.distribution;
            if (dist && (dist.positive || dist.neutral || dist.negative))
              return { positive: dist.positive || 0, neutral: dist.neutral || 0, negative: dist.negative || 0 };
            return prev;
          });
        }
      } catch (e) {}
    };
    poll();
    const tid = setInterval(poll, 10000);
    return () => clearInterval(tid);
  }, []);

  // ── Mini donut chart ─────────────────────────────────────────
  React.useEffect(() => {
    if (!donutRef.current || !window.Chart) return;
    if (donutChart.current) donutChart.current.destroy();
    donutChart.current = new Chart(donutRef.current.getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: ['Positive','Neutral','Negative'],
        datasets: [{ data: [counters.positive, counters.neutral, counters.negative],
          backgroundColor: ['#10B981','#F59E0B','#EF4444'], borderColor: 'var(--border2)', borderWidth: 2 }],
      },
      options: {
        responsive: true, maintainAspectRatio: false, cutout: '65%',
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        animation: { duration: 300 },
      },
    });
  }, []);

  // Update donut on counter change
  React.useEffect(() => {
    if (!donutChart.current) return;
    donutChart.current.data.datasets[0].data = [counters.positive, counters.neutral, counters.negative];
    donutChart.current.update('none');
  }, [counters]);

  // ── Throughput line chart ────────────────────────────────────
  React.useEffect(() => {
    if (!lineRef.current || !window.Chart) return;
    if (lineChart.current) lineChart.current.destroy();
    lineChart.current = new Chart(lineRef.current.getContext('2d'), {
      type: 'line',
      data: {
        labels: Array(60).fill(''),
        datasets: [{ data: throughputHistory.current, borderColor: '#14B8A6', borderWidth: 1.5, fill: true,
          backgroundColor: 'rgba(20,184,166,0.1)', tension: 0.4, pointRadius: 0 }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        scales: { x: { display: false }, y: { display: false, min: 0 } },
        animation: false,
      },
    });
  }, []);

  const totalAll      = (counters.positive || 0) + (counters.neutral || 0) + (counters.negative || 0);
  const visibleFeeds  = feeds.filter(f => sentFilter.includes(f.sentiment));
  const sentColors    = { positive: '#10B981', neutral: '#F59E0B', negative: '#EF4444' };
  const topProducts   = (D.topPositiveProducts || []).slice(0, 5);

  const wsLabel = wsStatus === 'connected' ? 'WebSocket Connected'
    : wsStatus === 'reconnecting' ? 'Reconnecting…'
    : 'Connecting…';
  const wsColor = wsStatus === 'connected' ? '#10B981' : '#F59E0B';

  const RadialConf = ({ value }) => {
    const pct = Math.round((value || 0) * 100);
    const r = 18, c = 22, stroke = 3;
    const circ = 2 * Math.PI * r;
    const dash = (pct / 100) * circ;
    const color = pct >= 85 ? '#10B981' : pct >= 65 ? '#F59E0B' : '#EF4444';
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2 }}>
        <svg width={c*2} height={c*2} style={{ transform: 'rotate(-90deg)' }}>
          <circle cx={c} cy={c} r={r} fill="none" stroke="#334155" strokeWidth={stroke} />
          <circle cx={c} cy={c} r={r} fill="none" stroke={color} strokeWidth={stroke}
            strokeDasharray={`${dash} ${circ}`} strokeLinecap="round" />
        </svg>
        <span style={{ color, fontSize: 10, fontWeight: 700, fontFamily: 'Inter,sans-serif', marginTop: -28 + c*2 - 12, position: 'relative', zIndex: 1 }}>{pct}%</span>
      </div>
    );
  };

  return (
    <div style={{ display: 'flex', height: '100%', fontFamily: 'Inter,sans-serif', overflow: 'hidden' }}>
      {/* Main feed */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        {/* Live header */}
        <div style={{ padding: '20px 28px 16px', borderBottom: '1px solid var(--border2)', display: 'flex', alignItems: 'center', gap: 20, flexShrink: 0, flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#EF4444', display: 'inline-block', animation: 'pulse 1.2s infinite', boxShadow: '0 0 0 4px rgba(239,68,68,0.2)' }}></span>
            <span style={{ color: '#EF4444', fontSize: 14, fontWeight: 800, letterSpacing: '0.12em' }}>LIVE</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
            <span style={{ width: 7, height: 7, borderRadius: '50%', background: wsColor, display: 'inline-block' }}></span>
            <span style={{ color: wsColor, fontSize: 12, fontWeight: 500 }}>{wsLabel}</span>
          </div>
          <div style={{ fontSize: 22, fontWeight: 700, color: 'var(--text-primary)' }}>
            <span style={{ color: '#14B8A6' }}>{Number(throughput).toFixed(1)}</span>
            <span style={{ color: 'var(--text-muted)', fontSize: 13, fontWeight: 400, marginLeft: 4 }}>reviews/sec</span>
          </div>

          {/* Counters */}
          <div style={{ display: 'flex', gap: 16, marginLeft: 'auto', flexWrap: 'wrap' }}>
            {[
              { label: 'Total',    val: totalAll,          color: 'var(--text-primary)' },
              { label: 'Positive', val: counters.positive, color: '#10B981' },
              { label: 'Neutral',  val: counters.neutral,  color: '#F59E0B' },
              { label: 'Negative', val: counters.negative, color: '#EF4444' },
            ].map(c => (
              <div key={c.label} style={{ textAlign: 'center' }}>
                <div style={{ color: c.color, fontSize: 18, fontWeight: 700 }}>{(c.val || 0).toLocaleString()}</div>
                <div style={{ color: 'var(--text-muted)', fontSize: 10, letterSpacing: '0.05em', textTransform: 'uppercase' }}>{c.label}</div>
              </div>
            ))}
          </div>

          <button onClick={() => setPaused(!paused)} style={{
            padding: '8px 18px', borderRadius: 8, border: `1px solid ${paused ? '#10B981' : '#EF4444'}`,
            background: paused ? 'rgba(16,185,129,0.1)' : 'rgba(239,68,68,0.1)',
            color: paused ? '#10B981' : '#EF4444',
            cursor: 'pointer', fontSize: 13, fontWeight: 600, display: 'flex', alignItems: 'center', gap: 6,
          }}>
            <span className="material-icons" style={{ fontSize: 16 }}>{paused ? 'play_arrow' : 'pause'}</span>
            {paused ? 'Resume' : 'Pause'}
          </button>
          <button onClick={() => setSidebarOpen(!sidebarOpen)} style={{
            padding: '8px 12px', borderRadius: 8, border: '1px solid var(--border)',
            background: 'transparent', color: 'var(--text-muted)', cursor: 'pointer',
          }}>
            <span className="material-icons" style={{ fontSize: 16 }}>analytics</span>
          </button>
        </div>

        {/* Sentiment filter chips */}
        <div style={{ padding: '10px 28px', borderBottom: '1px solid var(--border2)', display: 'flex', gap: 8, alignItems: 'center', flexShrink: 0 }}>
          <span style={{ color: 'var(--text-muted)', fontSize: 12, fontWeight: 500 }}>FILTER:</span>
          {['positive','neutral','negative'].map(s => {
            const active = sentFilter.includes(s);
            const c = sentColors[s];
            return (
              <Pill key={s} label={s.charAt(0).toUpperCase()+s.slice(1)} active={active} color={c}
                onClick={() => setSentFilter(active && sentFilter.length > 1 ? sentFilter.filter(x=>x!==s) : [...new Set([...sentFilter,s])])} />
            );
          })}
          <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 8 }}>{visibleFeeds.length} cards visible</span>
        </div>

        {/* Feed stream */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '16px 28px', display: 'flex', flexDirection: 'column', gap: 10 }}>
          {feeds.length === 0 && (
            <div style={{ textAlign: 'center', padding: 60, color: 'var(--text-muted)' }}>
              <span className="material-icons" style={{ fontSize: 40, marginBottom: 8, display: 'block', animation: 'spin 1s linear infinite' }}>refresh</span>
              En attente de prédictions… Démarrez le pipeline si ce n'est pas déjà fait.
            </div>
          )}
          {visibleFeeds.map((card, idx) => {
            const c = sentColors[card.sentiment] || '#94A3B8';
            return (
              <div key={card._id || card.id || idx} style={{
                background: 'var(--bg-card)', borderRadius: 10, border: '1px solid var(--border)',
                borderLeft: `4px solid ${c}`, display: 'flex', gap: 16, padding: '14px 16px',
                opacity: idx < 15 ? 1 : Math.max(0.35, 1 - (idx - 15) / 35),
                animation: idx === 0 ? 'slideDown 0.35s ease' : 'none',
                transition: 'opacity 0.3s',
              }}>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 6, flexWrap: 'wrap' }}>
                    <span style={{ color: 'var(--text-muted)', fontSize: 11, fontFamily: 'JetBrains Mono,monospace' }}>{card.time}</span>
                    <button onClick={() => { setSelectedProduct(card.productId); setPage('product'); }} style={{
                      background: 'none', border: 'none', color: '#14B8A6', cursor: 'pointer', padding: 0,
                      fontSize: 11, fontFamily: 'JetBrains Mono,monospace', textDecoration: 'underline',
                    }}>{card.productId}</button>
                    <span style={{ color: 'var(--border)', fontSize: 11 }}>·</span>
                    <span style={{ color: 'var(--text-muted)', fontSize: 11, fontFamily: 'JetBrains Mono,monospace' }}>{card.userId}</span>
                  </div>
                  <div style={{ color: 'var(--text-primary)', fontSize: 14, fontWeight: 600, marginBottom: 4 }}>{card.summary}</div>
                  <div style={{ color: 'var(--text-muted)', fontSize: 12, lineHeight: 1.5, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 480 }}>
                    {(card.text || '').slice(0, 100)}{(card.text || '').length > 100 ? '…' : ''}
                  </div>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 8, flexShrink: 0 }}>
                  <SentimentBadge sentiment={card.sentiment} large={true} />
                  <RadialConf value={card.confidence} />
                </div>
              </div>
            );
          })}
          {feeds.length > 0 && visibleFeeds.length === 0 && (
            <div style={{ textAlign: 'center', padding: 60, color: 'var(--text-muted)' }}>
              <span className="material-icons" style={{ fontSize: 40, marginBottom: 8, display: 'block' }}>filter_list_off</span>
              No reviews match the current filter
            </div>
          )}
        </div>
      </div>

      {/* Right sidebar */}
      {sidebarOpen && (
        <div style={{ width: 260, borderLeft: '1px solid var(--border)', padding: '20px 16px', display: 'flex', flexDirection: 'column', gap: 20, overflowY: 'auto', flexShrink: 0 }}>
          {/* Throughput line */}
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 11, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: 8 }}>Last 60s Throughput</div>
            <div style={{ height: 64 }}>
              <canvas ref={lineRef}></canvas>
            </div>
          </div>

          {/* Sentiment ratio donut */}
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 11, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: 8 }}>Sentiment Ratio</div>
            <div style={{ height: 120, position: 'relative' }}>
              <canvas ref={donutRef}></canvas>
              <div style={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%,-50%)', textAlign: 'center' }}>
                <div style={{ color: 'var(--text-primary)', fontSize: 13, fontWeight: 700 }}>
                  {totalAll > 0 ? ((counters.positive / totalAll) * 100).toFixed(1) : '0.0'}%
                </div>
                <div style={{ color: 'var(--text-muted)', fontSize: 9 }}>Positive</div>
              </div>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4, marginTop: 8 }}>
              {[['#10B981','Positive',counters.positive],['#F59E0B','Neutral',counters.neutral],['#EF4444','Negative',counters.negative]].map(([c,l,v],i) => (
                <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <span style={{ width: 8, height: 8, borderRadius: 2, background: c, display: 'inline-block', flexShrink: 0 }}></span>
                  <span style={{ color: 'var(--text-secondary)', fontSize: 11, flex: 1 }}>{l}</span>
                  <span style={{ color: 'var(--text-primary)', fontSize: 11, fontWeight: 600, fontFamily: 'JetBrains Mono,monospace' }}>
                    {totalAll > 0 ? ((v / totalAll) * 100).toFixed(1) : '0.0'}%
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* Top products now */}
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 11, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', marginBottom: 8 }}>Top Products Now</div>
            {topProducts.map((p, i) => (
              <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                <span style={{ color: 'var(--text-muted)', fontSize: 10, minWidth: 14 }}>{i+1}.</span>
                <button onClick={() => { setSelectedProduct(p.id); setPage('product'); }} style={{
                  background: 'none', border: 'none', color: '#14B8A6', cursor: 'pointer', padding: 0,
                  fontSize: 11, fontFamily: 'JetBrains Mono,monospace', textDecoration: 'underline', textAlign: 'left',
                }}>{p.id}</button>
                <span style={{ color: 'var(--text-muted)', fontSize: 10, marginLeft: 'auto' }}>{(p.count || 0).toLocaleString()}</span>
              </div>
            ))}
            {topProducts.length === 0 && (
              <div style={{ color: 'var(--text-muted)', fontSize: 11 }}>Aucune donnée encore.</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
