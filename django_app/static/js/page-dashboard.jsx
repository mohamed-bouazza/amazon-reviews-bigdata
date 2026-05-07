
// ── Dashboard Page ───────────────────────────────────────────
function DashboardPage({ setPage, setSelectedProduct }) {
  const D = window.MOCK;
  const [yearFilter, setYearFilter] = React.useState('All');
  const [sentimentFilter, setSentimentFilter] = React.useState(['positive','neutral','negative']);
  const [tablePage, setTablePage] = React.useState(1);
  const [expandedRow, setExpandedRow] = React.useState(null);
  const PER_PAGE = 8;

  const years = ['All','2007','2008','2009','2010','2011','2012'];

  // Chart refs
  const trendRef   = React.useRef(null);
  const donutRef   = React.useRef(null);
  const negBarRef  = React.useRef(null);
  const posBarRef  = React.useRef(null);
  const heatRef    = React.useRef(null);
  const confRef    = React.useRef(null);
  const histRef    = React.useRef(null);

  const trendChart  = React.useRef(null);
  const donutChart  = React.useRef(null);
  const negBarChart = React.useRef(null);
  const posBarChart = React.useRef(null);
  const confChart   = React.useRef(null);
  const histChart   = React.useRef(null);

  const getTrendData = () => {
    if (yearFilter === 'All') {
      return {
        labels: D.yearlyTrend.map(d => d.year),
        pos: D.yearlyTrend.map(d => d.positive),
        neu: D.yearlyTrend.map(d => d.neutral),
        neg: D.yearlyTrend.map(d => d.negative),
      };
    }
    const monthly = D.monthlyTrend[parseInt(yearFilter)];
    return {
      labels: monthly.map(d => d.month),
      pos: monthly.map(d => d.positive),
      neu: monthly.map(d => d.neutral),
      neg: monthly.map(d => d.negative),
    };
  };

  const destroyChart = (ref) => { if (ref.current) { ref.current.destroy(); ref.current = null; } };

  // Trend chart
  React.useEffect(() => {
    if (!trendRef.current || !window.Chart) return;
    destroyChart(trendChart);
    const data = getTrendData();
    const ctx = trendRef.current.getContext('2d');
    const makeGrad = (c1, c2) => {
      const g = ctx.createLinearGradient(0, 0, 0, 220);
      g.addColorStop(0, c1); g.addColorStop(1, c2); return g;
    };
    trendChart.current = new Chart(ctx, {
      type: 'line',
      data: {
        labels: data.labels,
        datasets: [
          { label: 'Positive', data: data.pos, fill: true, backgroundColor: makeGrad('rgba(16,185,129,0.35)','rgba(16,185,129,0.02)'), borderColor: '#10B981', borderWidth: 2, tension: 0.4, pointRadius: 3, pointBackgroundColor: '#10B981' },
          { label: 'Neutral',  data: data.neu, fill: true, backgroundColor: makeGrad('rgba(245,158,11,0.25)','rgba(245,158,11,0.01)'), borderColor: '#F59E0B', borderWidth: 2, tension: 0.4, pointRadius: 3, pointBackgroundColor: '#F59E0B' },
          { label: 'Negative', data: data.neg, fill: true, backgroundColor: makeGrad('rgba(239,68,68,0.2)','rgba(239,68,68,0.01)'), borderColor: '#EF4444', borderWidth: 2, tension: 0.4, pointRadius: 3, pointBackgroundColor: '#EF4444' },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { labels: { color: 'var(--text-secondary)', font: { family: 'Inter', size: 12 }, boxWidth: 12, boxHeight: 12, padding: 16 } },
          tooltip: { backgroundColor: '#1E293B', titleColor: '#F1F5F9', bodyColor: '#94A3B8', borderColor: 'var(--border)', borderWidth: 1, padding: 12, callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.y.toLocaleString()}`,
          }},
        },
        scales: {
          x: { grid: { color: 'rgba(51,65,85,0.5)' }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 11 } } },
          y: { grid: { color: 'rgba(51,65,85,0.5)' }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 11 }, callback: v => v >= 1000 ? (v/1000).toFixed(0)+'K' : v } },
        },
        animation: { duration: 600, easing: 'easeInOutQuart' },
      },
    });
    return () => destroyChart(trendChart);
  }, [yearFilter, tick]);

  // Donut chart
  React.useEffect(() => {
    if (!donutRef.current || !window.Chart) return;
    destroyChart(donutChart);
    const dist = D.distribution;
    donutChart.current = new Chart(donutRef.current.getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: ['Positive','Neutral','Negative'],
        datasets: [{ data: [dist.positive, dist.neutral, dist.negative], backgroundColor: ['#10B981','#F59E0B','#EF4444'], borderColor: '#1E293B', borderWidth: 3, hoverOffset: 8 }],
      },
      options: {
        responsive: true, maintainAspectRatio: false, cutout: '70%',
        plugins: {
          legend: { position: 'bottom', labels: { color: 'var(--text-secondary)', font: { family: 'Inter', size: 12 }, padding: 16, boxWidth: 12 } },
          tooltip: { backgroundColor: '#1E293B', titleColor: '#F1F5F9', bodyColor: '#94A3B8', borderColor: 'var(--border)', borderWidth: 1,
            callbacks: { label: ctx => ` ${ctx.label}: ${ctx.parsed.toLocaleString()} (${((ctx.parsed/(dist.positive+dist.neutral+dist.negative))*100).toFixed(1)}%)` }
          },
        },
        animation: { animateRotate: true, duration: 800 },
      },
    });
    return () => destroyChart(donutChart);
  }, [tick]);

  // Negative bar
  React.useEffect(() => {
    if (!negBarRef.current || !window.Chart) return;
    destroyChart(negBarChart);
    const data = D.topNegativeProducts;
    negBarChart.current = new Chart(negBarRef.current.getContext('2d'), {
      type: 'bar',
      data: {
        labels: data.map(d => d.id),
        datasets: [{ label: 'Negative Reviews', data: data.map(d => d.count),
          backgroundColor: data.map((_, i) => `rgba(239,68,68,${0.5 + i/data.length*0.5})`),
          borderColor: '#EF4444', borderWidth: 0, borderRadius: 4 }],
      },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { backgroundColor: '#1E293B', titleColor: '#F1F5F9', bodyColor: '#EF4444', borderColor: 'var(--border)', borderWidth: 1 } },
        scales: {
          x: { grid: { color: 'rgba(51,65,85,0.4)' }, ticks: { color: 'var(--text-muted)', font: { family: 'JetBrains Mono,monospace', size: 10 } } },
          y: { grid: { display: false }, ticks: { color: 'var(--text-secondary)', font: { family: 'JetBrains Mono,monospace', size: 10 } } },
        },
        onClick: (evt, els) => {
          if (els.length) { const id = data[els[0].index].id; setSelectedProduct(id); setPage('product'); }
        },
        animation: { duration: 700 },
      },
    });
    return () => destroyChart(negBarChart);
  }, []);

  // Positive bar
  React.useEffect(() => {
    if (!posBarRef.current || !window.Chart) return;
    destroyChart(posBarChart);
    const data = D.topPositiveProducts;
    posBarChart.current = new Chart(posBarRef.current.getContext('2d'), {
      type: 'bar',
      data: {
        labels: data.map(d => d.id),
        datasets: [{ label: 'Positive Reviews', data: data.map(d => d.count),
          backgroundColor: data.map((_, i) => `rgba(16,185,129,${0.4 + i/data.length*0.6})`),
          borderColor: '#10B981', borderWidth: 0, borderRadius: 4 }],
      },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { backgroundColor: '#1E293B', titleColor: '#F1F5F9', bodyColor: '#10B981', borderColor: 'var(--border)', borderWidth: 1 } },
        scales: {
          x: { grid: { color: 'rgba(51,65,85,0.4)' }, ticks: { color: 'var(--text-muted)', font: { family: 'JetBrains Mono,monospace', size: 10 } } },
          y: { grid: { display: false }, ticks: { color: 'var(--text-secondary)', font: { family: 'JetBrains Mono,monospace', size: 10 } } },
        },
        onClick: (evt, els) => {
          if (els.length) { const id = data[els[0].index].id; setSelectedProduct(id); setPage('product'); }
        },
        animation: { duration: 700 },
      },
    });
    return () => destroyChart(posBarChart);
  }, []);

  // Confusion matrix — drawn on canvas manually with labels + numbers
  React.useEffect(() => {
    if (!confRef.current) return;
    const canvas = confRef.current;
    const ctx = canvas.getContext('2d');
    const W = canvas.offsetWidth || 500;
    const H = canvas.offsetHeight || 260;
    canvas.width = W; canvas.height = H;
    const matrix = D.confusionMatrix;
    const flat = matrix.flat();
    const maxVal = Math.max(...flat);
    const labels = ['Negative','Neutral','Positive'];
    const padL = 74, padT = 40, padR = 8, padB = 20;
    const cellW = (W - padL - padR) / 3;
    const cellH = (H - padT - padB) / 3;
    ctx.clearRect(0, 0, W, H);
    // Cells
    matrix.forEach((row, ri) => {
      row.forEach((val, ci) => {
        const x = padL + ci * cellW, y = padT + ri * cellH;
        const intensity = val / maxVal;
        const isDiag = ri === ci;
        ctx.fillStyle = isDiag
          ? `rgba(20,184,166,${0.18 + intensity * 0.72})`
          : `rgba(100,116,139,${0.06 + intensity * 0.32})`;
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(x+2,y+2,cellW-4,cellH-4,4);
        else ctx.rect(x+2,y+2,cellW-4,cellH-4);
        ctx.fill();
        // Count number
        ctx.fillStyle = isDiag ? '#F1F5F9' : (intensity > 0.25 ? '#CBD5E1' : '#64748B');
        ctx.font = `bold ${Math.min(Math.floor(cellW * 0.22), 17)}px Inter,sans-serif`;
        ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.fillText(val.toLocaleString(), x + cellW/2, y + cellH/2);
      });
    });
    // Row labels (True)
    const rColors = ['#EF4444','#F59E0B','#10B981'];
    labels.forEach((l, i) => {
      ctx.fillStyle = rColors[i]; ctx.font = '11px Inter,sans-serif';
      ctx.textAlign = 'right'; ctx.textBaseline = 'middle';
      ctx.fillText(l, padL - 8, padT + i * cellH + cellH/2);
    });
    // Col labels (Predicted)
    labels.forEach((l, i) => {
      ctx.fillStyle = rColors[i]; ctx.font = '11px Inter,sans-serif';
      ctx.textAlign = 'center'; ctx.textBaseline = 'bottom';
      ctx.fillText(l, padL + i * cellW + cellW/2, padT - 6);
    });
    // Axis titles
    ctx.fillStyle = '#64748B'; ctx.font = '10px Inter,sans-serif';
    ctx.textAlign = 'center'; ctx.textBaseline = 'bottom';
    ctx.fillText('← Predicted →', padL + (W - padL - padR)/2, padT - 20);
    ctx.save(); ctx.translate(12, padT + (H - padT - padB)/2);
    ctx.rotate(-Math.PI/2); ctx.textBaseline = 'top';
    ctx.fillText('← True →', 0, 0); ctx.restore();
  }, []);

  // Confidence histogram
  React.useEffect(() => {
    if (!histRef.current || !window.Chart) return;
    destroyChart(histChart);
    const h = D.confidenceHistogram;
    histChart.current = new Chart(histRef.current.getContext('2d'), {
      type: 'bar',
      data: {
        labels: h.bins.map(b => b.toFixed(2)),
        datasets: [
          { label: 'Positive', data: h.positive, backgroundColor: 'rgba(16,185,129,0.5)', borderColor: '#10B981', borderWidth: 1, borderRadius: 3 },
          { label: 'Neutral',  data: h.neutral,  backgroundColor: 'rgba(245,158,11,0.5)',  borderColor: '#F59E0B', borderWidth: 1, borderRadius: 3 },
          { label: 'Negative', data: h.negative, backgroundColor: 'rgba(239,68,68,0.4)',   borderColor: '#EF4444', borderWidth: 1, borderRadius: 3 },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: 'var(--text-secondary)', font: { family: 'Inter', size: 11 }, boxWidth: 10, padding: 12 } },
          tooltip: { backgroundColor: '#1E293B', titleColor: '#F1F5F9', bodyColor: '#94A3B8', borderColor: 'var(--border)', borderWidth: 1 },
        },
        scales: {
          x: { grid: { display: false }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 10 } } },
          y: { grid: { color: 'rgba(51,65,85,0.4)' }, ticks: { color: 'var(--text-muted)', font: { family: 'Inter', size: 10 }, callback: v => v >= 1000 ? (v/1000).toFixed(0)+'K' : v } },
        },
        animation: { duration: 600 },
      },
    });
    return () => destroyChart(histChart);
  }, []);

  // Heatmap (canvas custom)
  React.useEffect(() => {
    if (!heatRef.current) return;
    const canvas = heatRef.current;
    const ctx = canvas.getContext('2d');
    const W = canvas.width, H = canvas.height;
    const days = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
    const padL = 36, padT = 20, padB = 24, padR = 8;
    const cW = (W - padL - padR) / 24;
    const cH = (H - padT - padB) / 7;
    ctx.clearRect(0, 0, W, H);
    const allCounts = D.heatmap.flat().map(c => c.count);
    const maxCount = Math.max(...allCounts);
    D.heatmap.forEach((row, d) => {
      row.forEach((cell, h) => {
        const intensity = cell.count / maxCount;
        const r = Math.round(20 + intensity * (20 - 20));
        const g = Math.round(41 + intensity * (184 - 41));
        const b = Math.round(59 + intensity * (166 - 59));
        const alpha = 0.1 + intensity * 0.9;
        ctx.fillStyle = `rgba(${Math.round(14 + intensity*6)},${Math.round(184*intensity)},${Math.round(166*intensity)},${alpha})`;
        const x = padL + h * cW + 1, y = padT + d * cH + 1;
        ctx.beginPath();
        ctx.roundRect(x, y, cW - 2, cH - 2, 2);
        ctx.fill();
      });
      ctx.fillStyle = '#64748B';
      ctx.font = '10px Inter,sans-serif';
      ctx.textAlign = 'right';
      ctx.fillText(days[d], padL - 4, padT + d * cH + cH * 0.65);
    });
    for (let h = 0; h < 24; h += 4) {
      ctx.fillStyle = '#64748B';
      ctx.font = '10px Inter,sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText(h.toString().padStart(2,'0')+'h', padL + h * cW + cW / 2, H - 6);
    }
  }, []);

  // ── Auto-refresh toutes les 15s quand pipeline actif ──────────
  const [tick, setTick] = React.useState(0);
  React.useEffect(() => {
    const interval = setInterval(async () => {
      try {
        const [kpiData, distData, recentData, trendData] = await Promise.all([
          window.API.getKpi(),
          window.API.getDistribution(),
          window.API.getRecent(50),
          window.API.getTrend('All'),
        ]);
        if (kpiData)    Object.assign(window.MOCK.kpi, kpiData);
        if (distData)   Object.assign(window.MOCK.distribution, distData);
        if (recentData && recentData.data) window.MOCK.recentPredictions = recentData.data;
        if (trendData && trendData.data)   window.MOCK.yearlyTrend = trendData.data;
        setTick(t => t + 1); // force re-render
      } catch(e) {}
    }, 15000);
    return () => clearInterval(interval);
  }, []);

  const dist = D.distribution;
  const total = dist.positive + dist.neutral + dist.negative;
  const pctPos = ((dist.positive / total) * 100).toFixed(1);
  const pctNeu = ((dist.neutral / total) * 100).toFixed(1);
  const pctNeg = ((dist.negative / total) * 100).toFixed(1);

  // Paginate table
  const tableRows = D.recentPredictions;
  const pageRows = tableRows.slice((tablePage - 1) * PER_PAGE, tablePage * PER_PAGE);

  const pipelineRunning = D.kpi.pipelineStatus === 'running';

  return (
    <div style={{ padding: '28px 32px', maxWidth: 1440, margin: '0 auto', fontFamily: 'Inter,sans-serif' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 28 }}>
        <div>
          <h1 style={{ color: 'var(--text-primary)', fontSize: 26, fontWeight: 700, margin: '0 0 4px' }}>Analytics Dashboard</h1>
          <p style={{ color: 'var(--text-muted)', fontSize: 13, margin: 0 }}>Amazon Fine Food Reviews — Historical dataset 2007–2012</p>
        </div>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          <div style={{
            display: 'flex', alignItems: 'center', gap: 7,
            padding: '7px 14px', background: pipelineRunning ? 'rgba(16,185,129,0.1)' : 'rgba(239,68,68,0.1)',
            border: `1px solid ${pipelineRunning ? 'rgba(16,185,129,0.3)' : 'rgba(239,68,68,0.3)'}`,
            borderRadius: 8, fontSize: 12, fontWeight: 600,
            color: pipelineRunning ? '#10B981' : '#EF4444',
          }}>
            <span style={{ width: 7, height: 7, borderRadius: '50%', background: pipelineRunning ? '#10B981' : '#EF4444', display: 'inline-block', animation: pipelineRunning ? 'pulse 2s infinite' : 'none' }}></span>
            Pipeline {pipelineRunning ? 'Running' : 'Stopped'}
          </div>
          <button style={{ padding: '7px 14px', borderRadius: 8, border: '1px solid var(--border)', background: 'var(--bg-card)', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 12, display: 'flex', alignItems: 'center', gap: 6 }}>
            <span className="material-icons" style={{ fontSize: 15 }}>download</span> Export
          </button>
        </div>
      </div>

      {/* KPI Cards */}
      <div style={{ display: 'flex', gap: 16, marginBottom: 24 }}>
        <KpiCard icon="bar_chart" label="Total Predictions" value={D.kpi.totalPredictions.toLocaleString()}
          sub="Test set evaluation" subColor="#64748B"
          sparkData={[41200,44800,47100,45300,49200,51800,53400,50900,55700,58100,62400,68900]} />
        <KpiCard icon="speed" label="Throughput" value="47 / sec"
          sub={<span style={{ display:'flex', alignItems:'center', gap:4 }}><span style={{ width:6,height:6,borderRadius:'50%',background:'#10B981',animation:'pulse 2s infinite',display:'inline-block' }}></span> Live streaming</span>}
          iconBg="rgba(14,165,233,0.15)"
          sparkData={[38,42,44,51,47,45,49,52,46,47,50,47]} />
        <KpiCard icon="model_training" label="Model F1-Score" value="82.7%"
          sub="Test set evaluation"
          subColor="#94A3B8"
          iconBg="rgba(99,102,241,0.15)"
          sparkData={[78.1,79.3,80.5,80.2,81.4,82.0,81.7,82.3,82.5,82.7,82.7,82.7]} />
        <KpiCard icon="inventory_2" label="Unique ProductIds" value={D.kpi.uniqueProducts.toLocaleString()}
          sub="Across full dataset" subColor="#64748B"
          iconBg="rgba(245,158,11,0.15)"
          sparkData={[58000,61000,63500,65200,67800,69400,71000,72300,73100,73900,74100,74258]} />
      </div>

      {/* Filter bar */}
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12,
        padding: '14px 20px', marginBottom: 24, display: 'flex', gap: 16, alignItems: 'center', flexWrap: 'wrap',
        transition: 'background 0.25s ease',
      }}>
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          <span style={{ color: 'var(--text-muted)', fontSize: 12, fontWeight: 500, marginRight: 4 }}>YEAR</span>
          {years.map(y => (
            <Pill key={y} label={y} active={yearFilter === y} onClick={() => setYearFilter(y)} />
          ))}
        </div>
        <div style={{ width: 1, height: 28, background: 'var(--border)' }}></div>
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          <span style={{ color: 'var(--text-muted)', fontSize: 12, fontWeight: 500, marginRight: 4 }}>SENTIMENT</span>
          {['positive','neutral','negative'].map(s => {
            const active = sentimentFilter.includes(s);
            const colors = { positive: '#10B981', neutral: '#F59E0B', negative: '#EF4444' };
            return <Pill key={s} label={s.charAt(0).toUpperCase()+s.slice(1)} active={active} color={colors[s]}
              onClick={() => setSentimentFilter(active && sentimentFilter.length > 1 ? sentimentFilter.filter(x=>x!==s) : [...new Set([...sentimentFilter,s])])} />;
          })}
        </div>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          <button style={{ padding: '6px 14px', borderRadius: 8, border: '1px solid var(--border)', background: 'transparent', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 12, display: 'flex', gap: 6, alignItems: 'center' }}>
            <span className="material-icons" style={{ fontSize: 15 }}>bookmark_add</span> Save view
          </button>
          <button style={{ padding: '6px 14px', borderRadius: 8, border: '1px solid var(--border)', background: 'transparent', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 12, display: 'flex', gap: 6, alignItems: 'center' }}>
            <span className="material-icons" style={{ fontSize: 15 }}>download</span> CSV
          </button>
        </div>
      </div>

      {/* Trend Chart (full width) */}
      <ChartCard title="Sentiment Trend" subtitle={yearFilter === 'All' ? 'Yearly aggregation 2007–2012' : `Monthly breakdown — ${yearFilter}`}
        style={{ marginBottom: 24 }}
        controls={
          <div style={{ display: 'flex', gap: 4 }}>
            {years.map(y => <Pill key={y} label={y} active={yearFilter===y} onClick={() => setYearFilter(y)} />)}
          </div>
        }>
        <div style={{ height: 240 }}>
          <canvas ref={trendRef}></canvas>
        </div>
      </ChartCard>

      {/* 2-col grid */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 20 }}>
        {/* Donut */}
        <ChartCard title="Global Distribution" subtitle={`Total: ${total.toLocaleString()} predictions`}>
          <div style={{ position: 'relative', height: 260 }}>
            <canvas ref={donutRef}></canvas>
            <div style={{ position: 'absolute', top: '38%', left: '50%', transform: 'translate(-50%,-50%)', textAlign: 'center', pointerEvents: 'none' }}>
              <div style={{ color: 'var(--text-primary)', fontSize: 20, fontWeight: 700 }}>{total.toLocaleString()}</div>
              <div style={{ color: 'var(--text-muted)', fontSize: 10 }}>Total</div>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 12, justifyContent: 'center', marginTop: 8 }}>
            {[['#10B981','Positive',pctPos],[' #F59E0B','Neutral',pctNeu],['#EF4444','Negative',pctNeg]].map(([c,l,p],i) => (
              <div key={i} style={{ textAlign: 'center' }}>
                <div style={{ color: c.trim(), fontSize: 18, fontWeight: 700 }}>{p}%</div>
                <div style={{ color: 'var(--text-muted)', fontSize: 11 }}>{l}</div>
              </div>
            ))}
          </div>
        </ChartCard>

        {/* Negative bar */}
        <ChartCard title="Top 10 — Most Negative Reviews" subtitle="Click a bar to view product detail">
          <div style={{ height: 260 }}>
            <canvas ref={negBarRef}></canvas>
          </div>
        </ChartCard>

        {/* Positive bar */}
        <ChartCard title="Top 10 — Most Positive Reviews" subtitle="Click a bar to view product detail">
          <div style={{ height: 260 }}>
            <canvas ref={posBarRef}></canvas>
          </div>
        </ChartCard>

        {/* Confusion matrix */}
        <ChartCard title="Confusion Matrix" subtitle="Predicted vs. true sentiment labels">
          <div style={{ height: 260, position: 'relative' }}>
            <canvas ref={confRef} style={{ width: '100%', height: '100%', display: 'block' }}></canvas>
          </div>
        </ChartCard>
      </div>



      {/* Confidence histogram */}
      <ChartCard title="Confidence Score Distribution" subtitle="Model confidence by sentiment class" style={{ marginBottom: 20 }}>
        <div style={{ height: 200 }}>
          <canvas ref={histRef}></canvas>
        </div>
      </ChartCard>

      {/* Recent predictions table */}
      <ChartCard title="Recent Predictions" subtitle="Last analyzed reviews from the dataset">
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: 'Inter,sans-serif' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border)' }}>
                {['ID','Product ID','Summary','Date','Sentiment','Confidence'].map(h => (
                  <th key={h} style={{ textAlign: 'left', padding: '8px 12px', color: 'var(--text-muted)', fontSize: 11, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {pageRows.map(row => (
                <React.Fragment key={row.id}>
                  <tr style={{ borderBottom: '1px solid var(--border2)', cursor: 'pointer', transition: 'background 0.1s' }}
                    onClick={() => setExpandedRow(expandedRow === row.id ? null : row.id)}
                    onMouseEnter={e => e.currentTarget.style.background='rgba(100,116,139,0.08)'}
                    onMouseLeave={e => e.currentTarget.style.background='transparent'}>
                    <td style={{ padding: '10px 12px', color: 'var(--text-muted)', fontSize: 12, fontFamily: 'JetBrains Mono,monospace' }}>{row.id}</td>
                    <td style={{ padding: '10px 12px' }}>
                      <button onClick={e => { e.stopPropagation(); setSelectedProduct(row.productId); setPage('product'); }} style={{
                        background: 'none', border: 'none', color: '#14B8A6', cursor: 'pointer',
                        fontSize: 12, fontFamily: 'JetBrains Mono,monospace', padding: 0, textDecoration: 'underline',
                      }}>{row.productId}</button>
                    </td>
                    <td style={{ padding: '10px 12px', color: 'var(--text-primary)', fontSize: 13 }}>{row.summary}</td>
                    <td style={{ padding: '10px 12px', color: 'var(--text-muted)', fontSize: 12, fontFamily: 'JetBrains Mono,monospace' }}>{row.time}</td>
                    <td style={{ padding: '10px 12px' }}><SentimentBadge sentiment={row.sentiment} /></td>
                    <td style={{ padding: '10px 12px', minWidth: 120 }}><ConfidenceBar value={row.confidence} /></td>
                  </tr>
                  {expandedRow === row.id && (
                    <tr style={{ background: 'var(--bg-card2)' }}>
                      <td colSpan={6} style={{ padding: '12px 16px' }}>
                        <div style={{ color: 'var(--text-secondary)', fontSize: 13, lineHeight: 1.7, marginBottom: 10 }}>{row.text}</div>
                        <div style={{ background: 'var(--bg-card)', borderRadius: 8, padding: '10px 14px', fontFamily: 'JetBrains Mono,monospace', fontSize: 11, color: 'var(--text-secondary)', border: '1px solid var(--border)' }}>
                          {JSON.stringify({ Id: row.id, ProductId: row.productId, UserId: row.userId, Time: new Date(row.time).getTime()/1000|0, true_sentiment: row.sentiment === 'positive' ? 2 : row.sentiment === 'neutral' ? 1 : 0, prediction: row.sentiment === 'positive' ? 2.0 : row.sentiment === 'neutral' ? 1.0 : 0.0, confidence: row.confidence }, null, 2)}
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
        <div style={{ marginTop: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>Showing {(tablePage-1)*PER_PAGE+1}–{Math.min(tablePage*PER_PAGE,tableRows.length)} of {tableRows.length} predictions</span>
          <Pagination page={tablePage} total={tableRows.length} perPage={PER_PAGE} onChange={setTablePage} />
        </div>
      </ChartCard>
    </div>
  );
}

Object.assign(window, { DashboardPage });
