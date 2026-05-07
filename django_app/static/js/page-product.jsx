
// ── Product Detail Page ──────────────────────────────────────
function ProductDetailPage({ setPage, productId: initialProductId }) {
  const D = window.MOCK;

  const EMPTY_PRODUCT = { id: '', totalReviews: 0, positive: 0, neutral: 0, negative: 0, yearlyBreakdown: [], reviews: [] };

  // Build product list from MOCK top lists (real MongoDB data loaded at bootstrap)
  const buildProductList = () => {
    const seen = new Set();
    const list = [];
    for (const p of [...(D.topPositiveProducts || []), ...(D.topNegativeProducts || [])]) {
      if (!seen.has(p.id)) { seen.add(p.id); list.push(p); }
    }
    return list;
  };

  const [selectedId, setSelectedId]   = React.useState(initialProductId || (buildProductList()[0] || {}).id || 'B001E4KFG0');
  const [product, setProduct]         = React.useState(EMPTY_PRODUCT);
  const [loading, setLoading]         = React.useState(false);
  const [allProducts, setAllProducts] = React.useState(buildProductList);
  const [searchQuery, setSearchQuery] = React.useState('');
  const [searchOpen, setSearchOpen]   = React.useState(false);
  const [sentFilter, setSentFilter]   = React.useState('all');
  const [tablePage, setTablePage]     = React.useState(1);
  const [expandedRow, setExpandedRow] = React.useState(null);
  const PER_PAGE = 5;
  const searchRef = React.useRef(null);

  // Close dropdown on outside click
  React.useEffect(() => {
    const handler = (e) => { if (searchRef.current && !searchRef.current.contains(e.target)) setSearchOpen(false); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  // Fetch full product list (all distinct ProductIds in MongoDB)
  React.useEffect(() => {
    if (!window.API || !window.API.getAllProducts) return;
    window.API.getAllProducts(2000)
      .then(res => {
        if (res && Array.isArray(res.data) && res.data.length > 0) {
          setAllProducts(res.data);
        }
      })
      .catch(() => {});
  }, []);

  // Fetch product detail from real API when selectedId changes
  React.useEffect(() => {
    if (!selectedId || !window.API) return;
    setLoading(true);
    setProduct(EMPTY_PRODUCT);
    setTablePage(1);
    setExpandedRow(null);
    window.API.getProduct(selectedId)
      .then(data => {
        if (data) setProduct({ ...EMPTY_PRODUCT, ...data });
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [selectedId]);

  const selectProduct = (id) => {
    setSelectedId(id);
    setSearchQuery('');
    setSearchOpen(false);
    setSentFilter('all');
    setTablePage(1);
    setExpandedRow(null);
  };

  const donutRef   = React.useRef(null);
  const lineRef    = React.useRef(null);
  const histRef    = React.useRef(null);
  const barRef     = React.useRef(null);
  const donutChart = React.useRef(null);
  const lineChart  = React.useRef(null);
  const histChart  = React.useRef(null);
  const barChart   = React.useRef(null);

  const destroy = r => { if (r.current) { r.current.destroy(); r.current = null; } };

  const total = (product.positive || 0) + (product.neutral || 0) + (product.negative || 0);

  // Donut
  React.useEffect(() => {
    if (!donutRef.current || !window.Chart) return;
    destroy(donutChart);
    donutChart.current = new Chart(donutRef.current.getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: ['Positive','Neutral','Negative'],
        datasets: [{ data: [product.positive || 0, product.neutral || 0, product.negative || 0],
          backgroundColor: ['#10B981','#F59E0B','#EF4444'], borderColor: '#1E293B', borderWidth: 3, hoverOffset: 8 }],
      },
      options: {
        responsive: true, maintainAspectRatio: false, cutout: '72%',
        plugins: {
          legend: { position: 'bottom', labels: { color: 'var(--text-secondary)', font: { family: 'Inter', size: 11 }, padding: 14, boxWidth: 10 } },
          tooltip: { backgroundColor: '#1E293B', titleColor: '#F1F5F9', bodyColor: '#94A3B8', borderColor: 'var(--border)', borderWidth: 1,
            callbacks: { label: ctx => ` ${ctx.label}: ${ctx.parsed} (${total > 0 ? ((ctx.parsed/total)*100).toFixed(1) : 0}%)` }
          },
        },
        animation: { animateRotate: true, duration: 700 },
      },
    });
    return () => destroy(donutChart);
  }, [selectedId, product.positive, product.neutral, product.negative]);

  // Line (evolution)
  React.useEffect(() => {
    if (!lineRef.current || !window.Chart) return;
    destroy(lineChart);
    const yearly = product.yearlyBreakdown || [];
    lineChart.current = new Chart(lineRef.current.getContext('2d'), {
      type: 'line',
      data: {
        labels: yearly.map(d => d.year),
        datasets: [
          { label: 'Positive', data: yearly.map(d => d.positive || 0), borderColor: '#10B981', backgroundColor: 'rgba(16,185,129,0.12)', fill: true, tension: 0.4, borderWidth: 2, pointRadius: 4, pointBackgroundColor: '#10B981' },
          { label: 'Neutral',  data: yearly.map(d => d.neutral  || 0), borderColor: '#F59E0B', backgroundColor: 'rgba(245,158,11,0.08)',   fill: true, tension: 0.4, borderWidth: 2, pointRadius: 4, pointBackgroundColor: '#F59E0B' },
          { label: 'Negative', data: yearly.map(d => d.negative || 0), borderColor: '#EF4444', backgroundColor: 'rgba(239,68,68,0.07)',    fill: true, tension: 0.4, borderWidth: 2, pointRadius: 4, pointBackgroundColor: '#EF4444' },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { labels: { color: 'var(--text-secondary)', font: { family: 'Inter', size: 11 }, boxWidth: 10, padding: 12 } },
          tooltip: { backgroundColor: '#1E293B', titleColor: '#F1F5F9', bodyColor: '#94A3B8', borderColor: 'var(--border)', borderWidth: 1 },
        },
        scales: {
          x: { grid: { color: 'rgba(51,65,85,0.4)' }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 11 } } },
          y: { grid: { color: 'rgba(51,65,85,0.4)' }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 11 } }, beginAtZero: true },
        },
        animation: { duration: 600 },
      },
    });
    return () => destroy(lineChart);
  }, [selectedId, product.yearlyBreakdown]);

  // Confidence histogram
  React.useEffect(() => {
    if (!histRef.current || !window.Chart) return;
    destroy(histChart);
    const reviews = product.reviews || [];
    const bins = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    const counts = bins.slice(0,-1).map((b,i) => reviews.filter(r => (r.confidence || 0) >= b && (r.confidence || 0) < bins[i+1]).length);
    histChart.current = new Chart(histRef.current.getContext('2d'), {
      type: 'bar',
      data: {
        labels: bins.slice(0,-1).map(b => b.toFixed(1)+'+'),
        datasets: [{ label: 'Reviews', data: counts, backgroundColor: 'rgba(20,184,166,0.5)', borderColor: '#14B8A6', borderWidth: 1, borderRadius: 4 }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { backgroundColor: '#1E293B', titleColor: '#F1F5F9', bodyColor: '#94A3B8', borderColor: 'var(--border)', borderWidth: 1 } },
        scales: {
          x: { grid: { display: false }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 11 } } },
          y: { grid: { color: 'rgba(51,65,85,0.4)' }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 11 }, stepSize: 1 }, beginAtZero: true },
        },
        animation: { duration: 500 },
      },
    });
    return () => destroy(histChart);
  }, [selectedId, product.reviews]);

  // Comparison bar (product vs global)
  React.useEffect(() => {
    if (!barRef.current || !window.Chart) return;
    destroy(barChart);
    const globalDist  = D.distribution || { positive: 0, neutral: 0, negative: 0 };
    const globalTotal = (globalDist.positive || 0) + (globalDist.neutral || 0) + (globalDist.negative || 0);
    const globalPct   = globalTotal > 0
      ? [globalDist.positive/globalTotal*100, globalDist.neutral/globalTotal*100, globalDist.negative/globalTotal*100]
      : [0, 0, 0];
    const prodPct = total > 0
      ? [(product.positive||0)/total*100, (product.neutral||0)/total*100, (product.negative||0)/total*100]
      : [0, 0, 0];
    barChart.current = new Chart(barRef.current.getContext('2d'), {
      type: 'bar',
      data: {
        labels: ['Positive','Neutral','Negative'],
        datasets: [
          { label: 'This Product',  data: prodPct,   backgroundColor: ['rgba(16,185,129,0.7)','rgba(245,158,11,0.7)','rgba(239,68,68,0.7)'], borderRadius: 4 },
          { label: 'Global Average', data: globalPct, backgroundColor: ['rgba(16,185,129,0.2)','rgba(245,158,11,0.2)','rgba(239,68,68,0.2)'], borderColor: ['#10B981','#F59E0B','#EF4444'], borderWidth: 1, borderRadius: 4 },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: 'var(--text-secondary)', font: { family: 'Inter', size: 11 }, boxWidth: 10, padding: 12 } },
          tooltip: { backgroundColor: '#1E293B', titleColor: '#F1F5F9', bodyColor: '#94A3B8', borderColor: 'var(--border)', borderWidth: 1,
            callbacks: { label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)}%` }
          },
        },
        scales: {
          x: { grid: { display: false }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 11 } } },
          y: { grid: { color: 'rgba(51,65,85,0.4)' }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 11 }, callback: v => v+'%' }, max: 100 },
        },
        animation: { duration: 600 },
      },
    });
    return () => destroy(barChart);
  }, [selectedId, product.positive, product.neutral, product.negative]);

  const reviews        = product.reviews || [];
  const filteredReviews = sentFilter === 'all' ? reviews : reviews.filter(r => r.sentiment === sentFilter);
  const pageRows       = filteredReviews.slice((tablePage-1)*PER_PAGE, tablePage*PER_PAGE);

  const filteredProducts = allProducts.filter(p => p.id && p.id.toLowerCase().includes(searchQuery.toLowerCase()));

  return (
    <div style={{ padding: '28px 32px', maxWidth: 1440, margin: '0 auto', fontFamily: 'Inter,sans-serif' }}>
      {/* Breadcrumb */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 20 }}>
        <button onClick={() => setPage('dashboard')} style={{ background: 'none', border: 'none', color: '#14B8A6', cursor: 'pointer', fontSize: 13, display: 'flex', alignItems: 'center', gap: 4, padding: 0 }}>
          <span className="material-icons" style={{ fontSize: 16 }}>arrow_back</span>
          Back to Dashboard
        </button>
        <span style={{ color: 'var(--border)' }}>/</span>
        <span style={{ color: 'var(--text-secondary)', fontSize: 13 }}>Product Detail</span>
        <span style={{ color: 'var(--border)' }}>/</span>
        <span style={{ color: 'var(--text-primary)', fontSize: 13, fontFamily: 'JetBrains Mono,monospace' }}>{selectedId}</span>
      </div>

      {/* Product selector + header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 20, flexWrap: 'wrap', gap: 16 }}>
        <div>
          <h1 style={{ color: 'var(--text-primary)', fontSize: 26, fontWeight: 700, margin: '0 0 8px', fontFamily: 'JetBrains Mono,monospace' }}>
            Product {loading ? '…' : selectedId}
          </h1>
          <p style={{ color: 'var(--text-muted)', fontSize: 13, margin: 0 }}>Amazon Fine Food Reviews · Single source · Historical data 2007–2012</p>
        </div>

        {/* Product search/select */}
        <div ref={searchRef} style={{ position: 'relative', minWidth: 280 }}>
          <div style={{ color: 'var(--text-muted)', fontSize: 11, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: 6 }}>Switch Product</div>
          <div style={{
            display: 'flex', alignItems: 'center', gap: 8,
            background: 'var(--bg-card)', border: `1px solid ${searchOpen ? '#14B8A6' : 'var(--border)'}`,
            borderRadius: 8, padding: '9px 12px', cursor: 'pointer', transition: 'border-color 0.2s',
          }} onClick={() => setSearchOpen(!searchOpen)}>
            <span className="material-icons" style={{ fontSize: 16, color: '#14B8A6', flexShrink: 0 }}>inventory_2</span>
            <input
              value={searchQuery || selectedId}
              onChange={e => { setSearchQuery(e.target.value); setSearchOpen(true); }}
              onFocus={() => { setSearchQuery(''); setSearchOpen(true); }}
              placeholder="Search product ID…"
              style={{ flex: 1, background: 'transparent', border: 'none', outline: 'none',
                color: 'var(--text-primary)', fontSize: 13, fontFamily: 'JetBrains Mono,monospace', cursor: 'text' }}
              onClick={e => e.stopPropagation()}
            />
            <span className="material-icons" style={{ fontSize: 16, color: 'var(--text-muted)', transition: 'transform 0.2s', transform: searchOpen ? 'rotate(180deg)' : 'none' }}>expand_more</span>
          </div>

          {/* Dropdown */}
          {searchOpen && (
            <div style={{
              position: 'absolute', top: '100%', left: 0, right: 0, marginTop: 4,
              background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 8,
              boxShadow: '0 8px 24px rgba(0,0,0,0.3)', zIndex: 300, maxHeight: 280, overflowY: 'auto',
            }}>
              {filteredProducts.length === 0 && (
                <div style={{ padding: '12px 16px', color: 'var(--text-muted)', fontSize: 13, textAlign: 'center' }}>No products found</div>
              )}
              {filteredProducts.map(p => {
                const tot    = (p.count || p.positive || 0) + (p.neutral || 0) + (p.negative || 0);
                const pctPos = tot > 0 ? (((p.positive || 0) / tot) * 100).toFixed(0) : '—';
                const pctNeg = tot > 0 ? (((p.negative || 0) / tot) * 100).toFixed(0) : '—';
                const isSelected = p.id === selectedId;
                return (
                  <div key={p.id} onClick={() => selectProduct(p.id)} style={{
                    padding: '10px 14px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 12,
                    background: isSelected ? 'rgba(20,184,166,0.08)' : 'transparent',
                    borderBottom: '1px solid var(--border2)', transition: 'background 0.12s',
                  }}
                  onMouseEnter={e => { if (!isSelected) e.currentTarget.style.background = 'rgba(100,116,139,0.08)'; }}
                  onMouseLeave={e => { if (!isSelected) e.currentTarget.style.background = 'transparent'; }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ color: isSelected ? '#14B8A6' : 'var(--text-primary)', fontSize: 13, fontFamily: 'JetBrains Mono,monospace', fontWeight: isSelected ? 600 : 400 }}>{p.id}</div>
                      <div style={{ color: 'var(--text-muted)', fontSize: 11, marginTop: 2 }}>{(p.count || 0).toLocaleString()} reviews</div>
                    </div>
                    {isSelected && <span className="material-icons" style={{ fontSize: 14, color: '#14B8A6', flexShrink: 0 }}>check</span>}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* Loading state */}
      {loading && (
        <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>
          <div style={{ width: 32, height: 32, border: '3px solid var(--border)', borderTopColor: '#14B8A6', borderRadius: '50%', animation: 'spin 0.8s linear infinite', margin: '0 auto 12px' }}></div>
          Chargement des données produit…
        </div>
      )}

      {/* KPI badges */}
      {!loading && (
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 28 }}>
        <div style={{ padding: '6px 16px', background: 'rgba(16,185,129,0.12)', border: '1px solid rgba(16,185,129,0.3)', borderRadius: 8, color: '#10B981', fontSize: 13, fontWeight: 600 }}>
          {total > 0 ? (((product.positive||0)/total)*100).toFixed(1) : '0.0'}% Positive
        </div>
        <div style={{ padding: '6px 16px', background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.3)', borderRadius: 8, color: '#F59E0B', fontSize: 13, fontWeight: 600 }}>
          {total > 0 ? (((product.neutral||0)/total)*100).toFixed(1) : '0.0'}% Neutral
        </div>
        <div style={{ padding: '6px 16px', background: 'rgba(239,68,68,0.12)', border: '1px solid rgba(239,68,68,0.3)', borderRadius: 8, color: '#EF4444', fontSize: 13, fontWeight: 600 }}>
          {total > 0 ? (((product.negative||0)/total)*100).toFixed(1) : '0.0'}% Negative
        </div>
        <div style={{ padding: '6px 16px', background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 8, color: 'var(--text-secondary)', fontSize: 13 }}>
          {total} total reviews
        </div>
      </div>
      )}

      {!loading && (
      <>
      {/* Charts row 1 */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1.8fr', gap: 20, marginBottom: 20 }}>
        <ChartCard title="Sentiment Distribution" subtitle="Donut breakdown">
          <div style={{ height: 260, position: 'relative' }}>
            <canvas ref={donutRef}></canvas>
            <div style={{ position: 'absolute', top: '42%', left: '50%', transform: 'translate(-50%,-50%)', textAlign: 'center', pointerEvents: 'none' }}>
              <div style={{ color: 'var(--text-primary)', fontSize: 18, fontWeight: 700 }}>{total}</div>
              <div style={{ color: 'var(--text-muted)', fontSize: 10 }}>reviews</div>
            </div>
          </div>
        </ChartCard>
        <ChartCard title="Sentiment Evolution" subtitle="Yearly breakdown">
          <div style={{ height: 260 }}><canvas ref={lineRef}></canvas></div>
        </ChartCard>
      </div>

      {/* Charts row 2 */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 24 }}>
        <ChartCard title="Confidence Distribution" subtitle="Model confidence across reviews">
          <div style={{ height: 200 }}><canvas ref={histRef}></canvas></div>
        </ChartCard>
        <ChartCard title="vs Global Average" subtitle="This product compared to all products">
          <div style={{ height: 200 }}><canvas ref={barRef}></canvas></div>
        </ChartCard>
      </div>

      {/* Reviews table */}
      <ChartCard title="Reviews" subtitle="All reviews for this product">
        <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
          {['all','positive','neutral','negative'].map(s => (
            <Pill key={s} label={s === 'all' ? 'All' : s.charAt(0).toUpperCase()+s.slice(1)}
              active={sentFilter === s}
              color={s === 'all' ? '#14B8A6' : s === 'positive' ? '#10B981' : s === 'neutral' ? '#F59E0B' : '#EF4444'}
              onClick={() => { setSentFilter(s); setTablePage(1); }} />
          ))}
        </div>

        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: 'Inter,sans-serif' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border)' }}>
                {['ID','User ID','Date','Summary','Sentiment','Confidence'].map(h => (
                  <th key={h} style={{ textAlign: 'left', padding: '8px 12px', color: 'var(--text-muted)', fontSize: 11, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {pageRows.map(row => (
                <React.Fragment key={row._id || row.id}>
                  <tr style={{ borderBottom: '1px solid var(--border2)', cursor: 'pointer' }}
                    onClick={() => setExpandedRow(expandedRow === (row._id || row.id) ? null : (row._id || row.id))}
                    onMouseEnter={e => e.currentTarget.style.background='rgba(100,116,139,0.08)'}
                    onMouseLeave={e => e.currentTarget.style.background='transparent'}>
                    <td style={{ padding: '10px 12px', color: 'var(--text-muted)', fontSize: 12, fontFamily: 'JetBrains Mono,monospace' }}>{row.id}</td>
                    <td style={{ padding: '10px 12px', color: 'var(--text-secondary)', fontSize: 11, fontFamily: 'JetBrains Mono,monospace' }}>{row.userId}</td>
                    <td style={{ padding: '10px 12px', color: 'var(--text-muted)', fontSize: 12, fontFamily: 'JetBrains Mono,monospace' }}>{row.time}</td>
                    <td style={{ padding: '10px 12px', color: 'var(--text-primary)', fontSize: 13 }}>{row.summary}</td>
                    <td style={{ padding: '10px 12px' }}><SentimentBadge sentiment={row.sentiment} /></td>
                    <td style={{ padding: '10px 12px', minWidth: 110 }}><ConfidenceBar value={row.confidence} /></td>
                  </tr>
                  {expandedRow === (row._id || row.id) && (
                    <tr style={{ background: 'var(--bg-card2)' }}>
                      <td colSpan={6} style={{ padding: '12px 16px' }}>
                        <div style={{ color: 'var(--text-secondary)', fontSize: 13, lineHeight: 1.7, marginBottom: 10 }}>{row.text}</div>
                        <div style={{ background: 'var(--bg-card)', borderRadius: 8, padding: '10px 14px', fontFamily: 'JetBrains Mono,monospace', fontSize: 11, color: 'var(--text-secondary)', overflowX: 'auto', border: '1px solid var(--border)' }}>
                          {JSON.stringify({ Id: row.id, ProductId: selectedId, UserId: row.userId, true_sentiment: row.trueSentiment, prediction: row.prediction, confidence: row.confidence }, null, 2)}
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
              {pageRows.length === 0 && (
                <tr><td colSpan={6} style={{ padding: '32px 12px', textAlign: 'center', color: 'var(--text-muted)', fontSize: 13 }}>
                  {total === 0 ? 'Aucune donnée pour ce produit — le pipeline n\'a pas encore traité ces messages.' : 'No reviews match this filter.'}
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
        <div style={{ marginTop: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>Showing {filteredReviews.length > 0 ? (tablePage-1)*PER_PAGE+1 : 0}–{Math.min(tablePage*PER_PAGE, filteredReviews.length)} of {filteredReviews.length} reviews</span>
          <Pagination page={tablePage} total={filteredReviews.length} perPage={PER_PAGE} onChange={setTablePage} />
        </div>
      </ChartCard>
      </>
      )}
    </div>
  );
}

Object.assign(window, { ProductDetailPage });
