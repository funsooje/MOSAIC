/* GeoMetrics Viewer — MapLibre only */
"use strict";

// ── Globals ──────────────────────────────────────────────────────────────────
let map;
let sourceMeta = [];
let selectedSource    = null;
let selectedVariable  = null;
let selectedTimestamp = null;
let layerMeta         = null;
let selectedColormap  = 'viridis';
let colorFlipped      = false;
let currentOpacity    = 0.75;

// ── Colormaps ─────────────────────────────────────────────────────────────────
const COLORMAPS = {
  viridis:  [[0,'#440154'],[0.1,'#482475'],[0.2,'#414487'],[0.3,'#355f8d'],[0.4,'#2a788e'],[0.5,'#21918c'],[0.6,'#22a884'],[0.7,'#44bf70'],[0.8,'#7ad151'],[0.9,'#bddf26'],[1,'#fde725']],
  greens:   [[0,'#ffffe5'],[0.2,'#d9f0a3'],[0.4,'#addd8e'],[0.6,'#78c679'],[0.8,'#31a354'],[1,'#006837']],
  rdylgn:   [[0,'#d73027'],[0.2,'#fc8d59'],[0.4,'#fee08b'],[0.6,'#d9ef8b'],[0.8,'#91cf60'],[1,'#1a9641']],
  blues:    [[0,'#f7fbff'],[0.2,'#c6dbef'],[0.4,'#9ecae1'],[0.6,'#6baed6'],[0.8,'#2171b5'],[1,'#08306b']],
  plasma:   [[0,'#0d0887'],[0.2,'#6a00a8'],[0.4,'#b12a90'],[0.6,'#e16462'],[0.8,'#fca636'],[1,'#f0f921']],
  inferno:  [[0,'#000004'],[0.2,'#420a68'],[0.4,'#932667'],[0.6,'#dd513a'],[0.8,'#fca50a'],[1,'#fcffa4']],
  magma:    [[0,'#000004'],[0.2,'#3b0f70'],[0.4,'#8c2981'],[0.6,'#de4968'],[0.8,'#fe9f6d'],[1,'#fcfdbf']],
  ylorrd:   [[0,'#ffffcc'],[0.2,'#fed976'],[0.4,'#fd8d3c'],[0.6,'#e31a1c'],[0.8,'#bd0026'],[1,'#800026']],
  spectral: [[0,'#9e0142'],[0.2,'#f46d43'],[0.4,'#fee08b'],[0.6,'#abdda4'],[0.8,'#3288bd'],[1,'#5e4fa2']],
  cividis:  [[0,'#00204d'],[0.2,'#31446b'],[0.4,'#666970'],[0.6,'#958f78'],[0.8,'#cbbe6e'],[1,'#fee838']],
  hot:      [[0,'#000000'],[0.33,'#cc0000'],[0.66,'#ffaa00'],[1,'#ffffff']],
  greys:    [[0,'#ffffff'],[0.5,'#969696'],[1,'#000000']],
};

function activeStops() {
  const stops = COLORMAPS[selectedColormap] || COLORMAPS.viridis;
  if (!colorFlipped) return stops;
  return stops.map(([t, c]) => [1 - t, c]).reverse();
}

function colorExpr() {
  return ['interpolate', ['linear'], ['get', 't'], ...activeStops().flatMap(([t, c]) => [t, c])];
}

function circleColorProp() {
  return ['case', ['<', ['get', 't'], 0], 'rgba(180,180,180,0.4)', colorExpr()];
}

// Circle radius from native pixel resolution (mid-latitude correction ~cos 45°)
function radiusExpr(resolutionM) {
  if (!resolutionM || resolutionM <= 0) {
    return ['interpolate', ['linear'], ['zoom'], 3, 2, 8, 4, 14, 8];
  }
  const stops = [];
  for (let z = 0; z <= 18; z++) {
    const metersPerPx = (40075017 * 0.707) / (Math.pow(2, z) * 256);
    const r = Math.max(1.5, Math.min(40, resolutionM / (2 * metersPerPx)));
    stops.push(z, r);
  }
  return ['interpolate', ['linear'], ['zoom'], ...stops];
}

// ── Map init ─────────────────────────────────────────────────────────────────
function initMap(view = { center: [-120.5, 47.5], zoom: 6 }) {
  map = new maplibregl.Map({
    container: 'map',
    preserveDrawingBuffer: true,
    style: {
      version: 8,
      sources: {
        osm: {
          type: 'raster',
          tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
          tileSize: 256,
          attribution: '© OpenStreetMap contributors',
        },
      },
      layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
    },
    center: view.center,
    zoom: view.zoom,
  });

  map.on('load', () => {
    map.addSource('geo-data', {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    });

    map.addLayer({
      id: 'geo-layer',
      type: 'circle',
      source: 'geo-data',
      paint: {
        'circle-radius':  radiusExpr(null),
        'circle-color':   circleColorProp(),
        'circle-opacity': currentOpacity,
        'circle-stroke-width': 0,
      },
    });

    const tooltip = document.getElementById('tooltip');
    map.on('mousemove', 'geo-layer', (e) => {
      map.getCanvas().style.cursor = 'crosshair';
      const f = e.features[0];
      if (!f) { tooltip.classList.add('hidden'); return; }
      const val = f.properties.value;
      const label = (val !== null && val !== undefined && val !== 'null')
        ? `${Number(Number(val).toFixed(4))} ${layerMeta?.unit || ''}`.trim()
        : 'No data';
      tooltip.style.left = `${e.point.x + 240 + 12}px`;
      tooltip.style.top  = `${e.point.y - 10}px`;
      tooltip.innerHTML  = `
        <div><b>${selectedSource?.name || ''}</b></div>
        <div>${selectedVariable || ''}: <b>${label}</b></div>
        <div style="color:#888">${e.lngLat.lat.toFixed(4)}° N, ${e.lngLat.lng.toFixed(4)}° E</div>
      `;
      tooltip.classList.remove('hidden');
    });
    map.on('mouseleave', 'geo-layer', () => {
      map.getCanvas().style.cursor = '';
      tooltip.classList.add('hidden');
    });
  });

  map.addControl(new maplibregl.NavigationControl(), 'top-right');
  map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-right');
}

// ── Sidebar population ────────────────────────────────────────────────────────
async function loadSources() {
  setStatus('Connecting to GeoMetrics…', 'loading');
  let resp;
  try {
    resp = await fetch('/api/sources');
  } catch (err) {
    setStatus(`Network error: ${err.message}`, 'error');
    return;
  }
  if (!resp.ok) {
    const detail = await resp.text().catch(() => resp.statusText);
    setStatus(`Failed to load sources: ${detail}`, 'error');
    return;
  }
  sourceMeta = await resp.json();

  const sel = document.getElementById('source-select');
  sel.innerHTML = '';
  for (const src of sourceMeta) {
    const opt = document.createElement('option');
    opt.value = src.name;
    opt.textContent = src.name + (src.description ? ` — ${src.description.slice(0, 40)}` : '');
    sel.appendChild(opt);
  }
  sel.disabled = false;
  setStatus('Ready.');
  onSourceChange();
}

function onSourceChange() {
  const name = document.getElementById('source-select').value;
  selectedSource = sourceMeta.find(s => s.name === name) || null;
  if (!selectedSource) return;

  const varSel = document.getElementById('variable-select');
  varSel.innerHTML = '';
  varSel.disabled = false;
  for (const v of selectedSource.variables) {
    const opt = document.createElement('option');
    opt.value = v.name;
    opt.textContent = v.name + (v.unit ? ` (${v.unit})` : '');
    varSel.appendChild(opt);
  }
  selectedVariable = selectedSource.variables[0]?.name || null;

  const wrap = document.getElementById('year-buttons');
  wrap.innerHTML = '';
  selectedTimestamp = null;
  for (const ts of selectedSource.timestamps) {
    const btn = document.createElement('button');
    btn.className = 'year-btn';
    btn.textContent = ts.slice(0, 4);
    btn.dataset.ts = ts;
    btn.addEventListener('click', () => {
      wrap.querySelectorAll('.year-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      selectedTimestamp = ts;
      updateLoadBtn();
    });
    wrap.appendChild(btn);
  }
  if (selectedSource.timestamps.length > 0) wrap.firstChild.click();
  updateLoadBtn();
}

function onVariableChange() {
  selectedVariable = document.getElementById('variable-select').value;
  updateLoadBtn();
}

function onColormapChange() {
  selectedColormap = document.getElementById('colormap-select').value;
  if (map.getLayer('geo-layer')) {
    map.setPaintProperty('geo-layer', 'circle-color', circleColorProp());
  }
  updateLegendBar();
}

function onFlipColors() {
  colorFlipped = document.getElementById('flip-colors').checked;
  if (map.getLayer('geo-layer')) {
    map.setPaintProperty('geo-layer', 'circle-color', circleColorProp());
  }
  updateLegendBar();
}

function onOpacityChange() {
  currentOpacity = parseFloat(document.getElementById('opacity-slider').value);
  document.getElementById('opacity-val').textContent = Math.round(currentOpacity * 100) + '%';
  if (map.getLayer('geo-layer')) {
    map.setPaintProperty('geo-layer', 'circle-opacity', currentOpacity);
  }
}

function updateLoadBtn() {
  const ready = selectedSource && selectedVariable && selectedTimestamp;
  document.getElementById('load-btn').disabled = !ready;
  document.getElementById('focus-btn').disabled = !ready;
}

// ── Data loading ──────────────────────────────────────────────────────────────
async function loadData(bbox = null) {
  if (!selectedSource || !selectedVariable || !selectedTimestamp) return;

  const btn       = document.getElementById('load-btn');
  const focusBtn  = document.getElementById('focus-btn');
  btn.disabled = focusBtn.disabled = true;

  const label = bbox ? `focus area [${bbox}]` : selectedTimestamp.slice(0, 4);
  setStatus(`Loading ${selectedSource.name} / ${selectedVariable} / ${label}…`, 'loading');

  let url = `/api/data` +
    `?source=${encodeURIComponent(selectedSource.name)}` +
    `&variable=${encodeURIComponent(selectedVariable)}` +
    `&timestamp=${encodeURIComponent(selectedTimestamp)}`;
  if (bbox) url += `&bbox=${encodeURIComponent(bbox)}`;

  let payload;
  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ detail: resp.statusText }));
      throw new Error(err.detail || resp.statusText);
    }
    payload = await resp.json();
  } catch (e) {
    setStatus(`Error: ${e.message}`, 'error');
    btn.disabled = focusBtn.disabled = false;
    return;
  }

  const { lats, lons, values, meta } = payload;
  layerMeta = meta;

  if (!lats.length) {
    setStatus('No data for this selection.', 'error');
    btn.disabled = focusBtn.disabled = false;
    return;
  }

  setStatus(`Rendering ${meta.count.toLocaleString()} cells…`, 'loading');

  const { min, max } = meta;
  const range = (max != null && min != null && max !== min) ? (max - min) : 1;

  const features = lats.map((lat, i) => {
    const v = values[i];
    const t = (v !== null && v !== undefined && isFinite(v))
      ? Math.max(0, Math.min(1, (v - min) / range))
      : -1;
    return {
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [lons[i], lat] },
      properties: { value: v, t },
    };
  });

  const apply = () => {
    map.getSource('geo-data').setData({ type: 'FeatureCollection', features });
    map.setPaintProperty('geo-layer', 'circle-radius',  radiusExpr(meta.pixel_resolution_m));
    map.setPaintProperty('geo-layer', 'circle-color',   circleColorProp());
    map.setPaintProperty('geo-layer', 'circle-opacity', currentOpacity);
  };

  if (map.getSource('geo-data')) {
    apply();
  } else {
    map.once('load', apply);
  }

  updateLegend(meta);
  const sampleNote = meta.sampled ? ` (sampled from ${meta.total.toLocaleString()})` : '';
  const areaNote   = bbox ? ' · focus area' : '';
  setStatus(`${meta.count.toLocaleString()} cells · ${meta.unit}${sampleNote}${areaNote}`);
  btn.disabled = focusBtn.disabled = false;
}

function loadFocusArea() {
  const b = map.getBounds();
  const w = b.getWest().toFixed(4), s = b.getSouth().toFixed(4);
  const e = b.getEast().toFixed(4),  n = b.getNorth().toFixed(4);
  const bbox = `${w},${s},${e},${n}`;
  console.log('[focus] bbox:', bbox);
  // Clear existing data immediately so user sees the reset
  if (map.getSource('geo-data')) {
    map.getSource('geo-data').setData({ type: 'FeatureCollection', features: [] });
  }
  loadData(bbox);
}

// ── Legend ────────────────────────────────────────────────────────────────────
function updateLegend(meta) {
  const legend = document.getElementById('legend');
  if (meta.min === null || meta.max === null) { legend.classList.add('hidden'); return; }
  document.getElementById('legend-title').textContent =
    `${selectedVariable} (${meta.unit || '—'})`;
  document.getElementById('legend-min').textContent = fmt(meta.min);
  document.getElementById('legend-max').textContent = fmt(meta.max);
  updateLegendBar();
  legend.classList.remove('hidden');
}

function updateLegendBar() {
  const stops = activeStops()
    .map(([t, c]) => `${c} ${(t * 100).toFixed(0)}%`)
    .join(', ');
  document.getElementById('legend-bar').style.background =
    `linear-gradient(to right, ${stops})`;
}

function fmt(n) {
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  if (Math.abs(n) >= 1)    return n.toFixed(2);
  return n.toFixed(4);
}

// ── Status ───────────────────────────────────────────────────────────────────
function setStatus(msg, cls = '') {
  const el = document.getElementById('status-box');
  el.textContent = msg;
  el.className = cls;
}

// ── Save / restore view ───────────────────────────────────────────────────────
async function saveView() {
  const c = map.getCenter();
  const btn = document.getElementById('save-view-btn');
  try {
    const resp = await fetch('/api/view', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ center: [c.lng, c.lat], zoom: map.getZoom() }),
    });
    btn.textContent = resp.ok ? 'Saved ✓' : 'Error';
  } catch (_) {
    btn.textContent = 'Error';
  }
  setTimeout(() => { btn.textContent = 'Save view'; }, 2000);
}

async function loadSavedView() {
  try {
    const resp = await fetch('/api/view');
    if (!resp.ok) { setStatus('No saved view.', 'error'); return; }
    const view = await resp.json();
    map.flyTo({ center: view.center, zoom: view.zoom, essential: true });
  } catch (_) {
    setStatus('Could not load saved view.', 'error');
  }
}

// ── Screenshot ───────────────────────────────────────────────────────────────
function takeScreenshot() {
  const canvas = map.getCanvas();
  const url = canvas.toDataURL('image/png');
  const parts = [
    selectedSource?.name || 'map',
    selectedVariable || '',
    selectedTimestamp?.slice(0, 4) || '',
  ].filter(Boolean).join('_');
  const a = document.createElement('a');
  a.href = url;
  a.download = `geometrics_${parts}.png`;
  a.click();
}

// ── Boot ─────────────────────────────────────────────────────────────────────
fetch('/api/view')
  .then(r => r.json())
  .catch(() => ({ center: [-120.5, 47.5], zoom: 6 }))
  .then(view => {
    try {
      initMap(view);
    } catch (err) {
      console.error('Map init failed:', err);
      setStatus('Map failed to load — check console.', 'error');
    }
    loadSources();
  });

document.getElementById('source-select').addEventListener('change', onSourceChange);
document.getElementById('variable-select').addEventListener('change', onVariableChange);
document.getElementById('colormap-select').addEventListener('change', onColormapChange);
document.getElementById('flip-colors').addEventListener('change', onFlipColors);
document.getElementById('opacity-slider').addEventListener('input', onOpacityChange);
document.getElementById('load-btn').addEventListener('click', () => loadData());
document.getElementById('focus-btn').addEventListener('click', loadFocusArea);
document.getElementById('save-view-btn').addEventListener('click', saveView);
document.getElementById('load-saved-view-btn').addEventListener('click', loadSavedView);
document.getElementById('screenshot-btn').addEventListener('click', takeScreenshot);
