// SentimentIQ — Real API client
// Fetches all data from Django REST endpoints and populates window.MOCK
// before React renders. No mock data is used.

(function () {
  const ctx = window.DJANGO_CTX || {};

  function getCsrf() {
    const m = document.cookie.match(/(^|;\s*)csrftoken=([^;]+)/);
    return m ? decodeURIComponent(m[2]) : (ctx.csrfToken || '');
  }

  async function jget(path) {
    const r = await fetch(path, { credentials: 'same-origin' });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }

  async function jsend(method, path, body) {
    const r = await fetch(path, {
      method,
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': getCsrf() },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!r.ok) { const e = await r.text(); throw new Error(`${r.status}: ${e}`); }
    return r.json();
  }

  // ── Public API (used by PipelinePage controls, saved dashboards, live feed) ──
  window.API = {
    getKpi:                    () => jget('/api/kpi'),
    getDistribution:           () => jget('/api/distribution'),
    getTrend:                  (year = 'All') => jget(`/api/trend?year=${encodeURIComponent(year)}`),
    getTopProducts:            (sentiment = 'all', limit = 10) => jget(`/api/top-products?sentiment=${sentiment}&limit=${limit}`),
    getAllProducts:            (limit = 2000) => jget(`/api/products?limit=${limit}`),
    getWordFrequencies:        (limit = 25) => jget(`/api/words?limit=${limit}`),
    getConfusionMatrix:        () => jget('/api/confusion-matrix'),
    getConfidenceDistribution: () => jget('/api/confidence-distribution'),
    getRecent:                 (limit = 50, sentiment) => jget(`/api/recent?limit=${limit}${sentiment ? '&sentiment=' + sentiment : ''}`),
    getProduct:                (pid) => jget(`/api/product/${encodeURIComponent(pid)}`),
    getProductReviews:         (pid, page = 1, sentiment = 'all') => jget(`/api/product/${encodeURIComponent(pid)}/reviews?page=${page}&sentiment=${sentiment}`),
    search:                    (q) => jget(`/api/search?q=${encodeURIComponent(q)}`),
    pipelineStatus:            () => jget('/api/pipeline/status'),
    pipelineLogs:              (lines = 50) => jget(`/api/pipeline/logs?lines=${lines}`),
    pipelineStart:             () => jsend('POST', '/api/pipeline/start'),
    pipelineStop:              () => jsend('POST', '/api/pipeline/stop'),
    listDashboards:            () => jget('/api/dashboards'),
    saveDashboard:             (data) => jsend('POST', '/api/dashboards', data),
    deleteDashboard:           (id) => jsend('DELETE', `/api/dashboards/${id}`),
    logout:                    () => { window.location.href = ctx.logoutUrl || '/logout/'; },

    connectLiveFeed(handlers = {}) {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      let ws, retry = 0, closed = false;
      function open() {
        ws = new WebSocket(`${proto}://${location.host}/ws/live/`);
        ws.onopen    = () => { retry = 0; handlers.onOpen && handlers.onOpen(); };
        ws.onmessage = (ev) => { try { handlers.onMessage && handlers.onMessage(JSON.parse(ev.data)); } catch(e) {} };
        ws.onerror   = (e) => { handlers.onError && handlers.onError(e); };
        ws.onclose   = () => {
          handlers.onClose && handlers.onClose();
          if (closed) return;
          retry = Math.min(retry + 1, 6);
          setTimeout(open, 1000 * Math.pow(2, retry));
        };
      }
      open();
      return {
        close() { closed = true; if (ws) ws.close(); },
        send(obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); },
      };
    },
  };

  // ── Initialize window.MOCK with empty structure so JSX `const D = window.MOCK` is safe ──
  window.MOCK = {
    kpi:                  { totalPredictions: 0, throughput: 0, f1Score: 0, uniqueProducts: 0, pipelineStatus: 'stopped' },
    distribution:         { positive: 0, neutral: 0, negative: 0 },
    yearlyTrend:          [],
    monthlyTrend:         {},
    topPositiveProducts:  [],
    topNegativeProducts:  [],
    confusionMatrix:      [[0,0,0],[0,0,0],[0,0,0]],
    confidenceHistogram:  { bins: [0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.85,0.9,0.95], positive: Array(10).fill(0), neutral: Array(10).fill(0), negative: Array(10).fill(0) },
    recentPredictions:    [],
    liveFeedReviews:      [],
    productDetail:        { id: '', totalReviews: 0, positive: 0, neutral: 0, negative: 0, yearlyBreakdown: [], reviews: [] },
    pipeline:             { status: 'stopped', producer: {}, sparkConsumer: {}, producerLogs: [], sparkLogs: [] },
    savedDashboards:      [],
    wordCloud:            { positive: [], neutral: [], negative: [] },
    heatmap:              [],
  };

  // ── Load all real data, then signal React to render ──
  async function bootstrap() {
    try {
      const [kpi, dist, yearly, conf, confidence, topPos, topNeg, recent, pipeline, dashboards, words] =
        await Promise.all([
          API.getKpi().catch(() => null),
          API.getDistribution().catch(() => null),
          API.getTrend('All').catch(() => null),
          API.getConfusionMatrix().catch(() => null),
          API.getConfidenceDistribution().catch(() => null),
          API.getTopProducts('positive', 10).catch(() => null),
          API.getTopProducts('negative', 10).catch(() => null),
          API.getRecent(50).catch(() => null),
          API.pipelineStatus().catch(() => null),
          API.listDashboards().catch(() => null),
          API.getWordFrequencies(30).catch(() => null),
        ]);

      if (words && words.data) {
        // Backend returns {positive:[{text,value}], ...}. Component expects {text,size:10–42}.
        const scaled = {};
        for (const k of ['positive', 'neutral', 'negative']) {
          const arr = words.data[k] || [];
          const max = arr.reduce((m, w) => Math.max(m, w.value || 0), 1);
          scaled[k] = arr.map(w => ({
            text: w.text,
            size: 10 + Math.round(((w.value || 0) / max) * 32),
          }));
        }
        Object.assign(window.MOCK.wordCloud, scaled);
      }

      if (kpi) Object.assign(window.MOCK.kpi, kpi, {
        pipelineStatus: pipeline ? pipeline.status : 'stopped',
      });

      if (dist) Object.assign(window.MOCK.distribution, dist);

      if (yearly && yearly.data) {
        window.MOCK.yearlyTrend = yearly.data;
        // Build monthlyTrend cache keyed by year (fetch lazily on demand via API.getTrend)
      }

      if (conf  && conf.matrix)  window.MOCK.confusionMatrix       = conf.matrix;
      if (confidence)            Object.assign(window.MOCK.confidenceHistogram, confidence);
      if (topPos && topPos.data) window.MOCK.topPositiveProducts    = topPos.data;
      if (topNeg && topNeg.data) window.MOCK.topNegativeProducts    = topNeg.data;

      if (recent && recent.data) {
        window.MOCK.recentPredictions = recent.data;
        window.MOCK.liveFeedReviews   = recent.data.slice(0, 8);
      }

      if (pipeline) {
        Object.assign(window.MOCK.pipeline, {
          status:        pipeline.status,
          producer:      pipeline.producer      || {},
          sparkConsumer: pipeline.consumer      || {},
          producerLogs:  [],
          sparkLogs:     [],
        });
        // Logs fetched non-blocking after render
        API.pipelineLogs(30).then(logs => {
          if (logs) {
            window.MOCK.pipeline.producerLogs = logs.producer || [];
            window.MOCK.pipeline.sparkLogs    = logs.spark    || [];
          }
        }).catch(() => {});
      }

      if (dashboards && dashboards.data) {
        window.MOCK.savedDashboards = dashboards.data.map(d => ({
          id:         d.id,
          title:      d.title || d.name,
          filters:    d.filters || {},
          lastViewed: d.lastViewed || '',
          count:      d.count || 0,
        }));
      }

    } catch (err) {
      console.error('API bootstrap error:', err);
    } finally {
      // Signal React to render — whether data loaded or not
      window._apiBootstrapped = true;
      window.dispatchEvent(new CustomEvent('api:ready'));
      if (typeof window.renderApp === 'function') window.renderApp();
    }
  }

  bootstrap();
})();
