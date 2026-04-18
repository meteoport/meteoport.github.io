// ============================
// RESIZE HANDLER (MAP + CHART)
// ============================

window.addEventListener("load", () => {
  setupResponsiveFixes();
});

function setupResponsiveFixes() {
  let resizeTimeout;

  function handleResize() {
    clearTimeout(resizeTimeout);

    resizeTimeout = setTimeout(() => {
      if (window.map) {
        window.map.invalidateSize();
      }

      if (window.chart) {
        window.chart.resize();
      }
    }, 200);
  }

  window.addEventListener("resize", handleResize);

  window.addEventListener("orientationchange", () => {
    setTimeout(() => {
      handleResize();
    }, 300);
  });
}

// TOUCH FIX
document.addEventListener("touchstart", function () {}, { passive: true });


// ============================
// CONFIG
// ============================

const THRESHOLDS = {
  greenMax: 1.5,
  yellowMax: 2.5,
  orangeMax: 3.5
};

let selectedHour = 0;
let selectedLocation = null;
let selectedRoute = null;
let waveChart = null;
let suppressNextMapClickForDepth = false;

let locations = [];
let markers = [];
let routes = [];
let routeLayers = [];

// DOM
const infoPanel = document.getElementById("info-panel");
const hourSlider = document.getElementById("hour-slider");
const hourLabel = document.getElementById("hour-label");
const waveChartCanvas = document.getElementById("wave-chart");
const chartTitle = document.getElementById("chart-title");

// ============================
// MAPA
// ============================

const map = L.map("map").setView([39.5, 0], 5);
window.map = map;

map.createPane("routesPane");
map.getPane("routesPane").style.zIndex = 350;

// MAPA BASE BATIMETRÍA (ETOPO1 COLOR SHADED RELIEF)
const baseMap = L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "&copy; OpenStreetMap &copy; CARTO",
  subdomains: "abcd",
  maxZoom: 19
}).addTo(map);

// CLICK PARA PROFUNDIDAD (GEBCO sigue funcionando igual)
map.on("click", (e) => {
  if (suppressNextMapClickForDepth) {
    suppressNextMapClickForDepth = false;
    return;
  }

  showDepthAtClick(e.latlng);
});

// ============================
// HELPERS
// ============================

function buildGebcoFeatureInfoUrl(latlng) {
  const size = map.getSize();
  const bounds = map.getBounds();
  const sw = bounds.getSouthWest();
  const ne = bounds.getNorthEast();

  const point = map.latLngToContainerPoint(latlng, map.getZoom());

  const params = new URLSearchParams({
    service: "WMS",
    request: "GetFeatureInfo",
    version: "1.1.1",
    layers: "GEBCO_Grid",
    query_layers: "GEBCO_Grid",
    styles: "",
    bbox: `${sw.lng},${sw.lat},${ne.lng},${ne.lat}`,
    width: Math.round(size.x),
    height: Math.round(size.y),
    srs: "EPSG:4326",
    format: "image/png",
    info_format: "text/html",
    x: Math.round(point.x),
    y: Math.round(point.y)
  });

  return `https://wms.gebco.net/mapserv?${params.toString()}`;
}

function extractDepthFromGebcoHtml(html) {
  if (!html) return null;

  const clean = html.replace(/\s+/g, " ");

  const patterns = [
    /-?\d+(?:\.\d+)?(?=\s*m\b)/i,
    /value[^-0-9]*(-?\d+(?:\.\d+)?)/i,
    /elevation[^-0-9]*(-?\d+(?:\.\d+)?)/i,
    /gray_index[^-0-9]*(-?\d+(?:\.\d+)?)/i
  ];

  for (const re of patterns) {
    const match = clean.match(re);
    if (match) {
      const num = Number(match[0].match(/-?\d+(?:\.\d+)?/)[0]);
      if (!Number.isNaN(num)) return num;
    }
  }

  return null;
}

async function showDepthAtClick(latlng) {
  try {
    const url = buildGebcoFeatureInfoUrl(latlng);
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const html = await res.text();
    const depth = extractDepthFromGebcoHtml(html);

    const depthLabel = depth === null
      ? "Sin dato"
      : depth < 0
        ? `${Math.abs(depth).toFixed(0)} m`
        : `${depth.toFixed(0)} m`;

    const prefix = depth === null
      ? "Profundidad"
      : depth < 0
        ? "Profundidad"
        : "Elevación";

    L.popup()
      .setLatLng(latlng)
      .setContent(`
        <div style="min-width:160px;">
          <div style="font-weight:700; margin-bottom:6px;">Batimetría</div>
          <div><strong>${prefix}:</strong> ${depthLabel}</div>
          <div style="margin-top:4px; color:#6b7280; font-size:12px;">
            ${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}
          </div>
        </div>
      `)
      .openOn(map);

  } catch (err) {
    console.error("Error consultando profundidad GEBCO:", err);

    L.popup()
      .setLatLng(latlng)
      .setContent(`
        <div style="min-width:160px;">
          <div style="font-weight:700; margin-bottom:6px;">Batimetría</div>
          <div>No se pudo consultar la profundidad</div>
        </div>
      `)
      .openOn(map);
  }
}

function formatNumber(val, decimals = 2) {
  if (val === null || val === undefined || Number.isNaN(val)) return "-";
  return Number(val).toFixed(decimals);
}

function formatTimeLabel(isoTime) {
  if (!isoTime) return "--";

  const d = new Date(isoTime);
  const months = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];

  const month = months[d.getUTCMonth()];
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");

  return `${month}-${day}-${hour}h`;
}

function formatDateTimeLong(isoTime) {
  if (!isoTime) return "--";

  const d = new Date(isoTime);
  const months = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];

  const year = d.getUTCFullYear();
  const month = months[d.getUTCMonth()];
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  const min = String(d.getUTCMinutes()).padStart(2, "0");

  return `${day} ${month} ${year} ${hour}:${min} UTC`;
}

function getColor(hs) {
  if (hs === null || hs === undefined || Number.isNaN(hs)) return "#9ca3af";

  if (hs < THRESHOLDS.greenMax) return "green";
  if (hs < THRESHOLDS.yellowMax) return "yellow";
  if (hs < THRESHOLDS.orangeMax) return "orange";
  return "red";
}

function getHexColorFromHs(hs, redThreshold = THRESHOLDS.orangeMax) {
  if (hs === null || hs === undefined || Number.isNaN(hs)) return "#94a3b8";

  const greenThreshold = redThreshold * 0.33;
  const yellowThreshold = redThreshold * 0.66;

  if (hs < greenThreshold) return "#16a34a";
  if (hs < yellowThreshold) return "#eab308";
  if (hs < redThreshold) return "#f97316";
  return "#dc2626";
}

function getRouteStatus(hs, redThreshold = THRESHOLDS.orangeMax) {
  if (hs === null || hs === undefined || Number.isNaN(hs)) {
    return {
      label: "Sin datos",
      color: "#64748b"
    };
  }

  const greenThreshold = redThreshold * 0.33;
  const yellowThreshold = redThreshold * 0.66;

  if (hs < greenThreshold) {
    return {
      label: "Operativo",
      color: "#16a34a"
    };
  }

  if (hs < yellowThreshold) {
    return {
      label: "Precaución",
      color: "#eab308"
    };
  }

  if (hs < redThreshold) {
    return {
      label: "Restricción",
      color: "#f97316"
    };
  }

  return {
    label: "No recomendable",
    color: "#dc2626"
  };
}

function escapeHtml(text) {
  return String(text ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function isValidNumber(v) {
  return v !== null && v !== undefined && !Number.isNaN(Number(v));
}

function getPointCoords(point) {
  const lat = isValidNumber(point.lat) ? Number(point.lat) : (
    isValidNumber(point.requested_lat) ? Number(point.requested_lat) : null
  );
  const lon = isValidNumber(point.lon) ? Number(point.lon) : (
    isValidNumber(point.requested_lon) ? Number(point.requested_lon) : null
  );

  if (!isValidNumber(lat) || !isValidNumber(lon)) return null;
  return [lat, lon];
}

function getOperationalWave(f) {
  const hasPde = isValidNumber(f?.hs_pde);
  const hasPort = isValidNumber(f?.hs_puerto);
  const hasCop = isValidNumber(f?.hs_cop);

  if (hasPde) {
    return {
      wave: Number(f.hs_pde),
      tp: isValidNumber(f.tp_pde) ? Number(f.tp_pde) : null,
      dir: isValidNumber(f.di_pde) ? Number(f.di_pde) : null,
      source: "PdE"
    };
  }

  if (hasPort) {
    return {
      wave: Number(f.hs_puerto),
      tp: null,
      dir: null,
      source: "Puerto"
    };
  }

  if (hasCop) {
    return {
      wave: Number(f.hs_cop),
      tp: isValidNumber(f.tp_cop) ? Number(f.tp_cop) : null,
      dir: isValidNumber(f.di_cop) ? Number(f.di_cop) : null,
      source: "Copernicus"
    };
  }

  return {
    wave: null,
    tp: null,
    dir: null,
    source: "Sin datos"
  };
}

function buildMergedForecast(point) {
  return (point.forecast || []).map((f, i) => {
    const op = getOperationalWave(f);

    return {
      hour: i,
      time: f.time,
      wave: op.wave,
      tp: op.tp,
      dir: op.dir,
      waveSource: op.source,

      wavePde: isValidNumber(f.hs_pde) ? Number(f.hs_pde) : null,
      tpPde: isValidNumber(f.tp_pde) ? Number(f.tp_pde) : null,
      dirPde: isValidNumber(f.di_pde) ? Number(f.di_pde) : null,

      waveCopernicus: isValidNumber(f.hs_cop) ? Number(f.hs_cop) : null,
      tpCopernicus: isValidNumber(f.tp_cop) ? Number(f.tp_cop) : null,
      dirCopernicus: isValidNumber(f.di_cop) ? Number(f.di_cop) : null,

      wavePort: isValidNumber(f.hs_puerto) ? Number(f.hs_puerto) : null,

      waveObs: isValidNumber(f.hs_obs) ? Number(f.hs_obs) : null,
      windObs: isValidNumber(f.wspeed_obs) ? Number(f.wspeed_obs) : null,

      windSpeed: isValidNumber(f.wspeed_mod) ? Number(f.wspeed_mod) : null,
      windDir: isValidNumber(f.wsdir_mod) ? Number(f.wsdir_mod) : null
    };
  });
}

function getForecastLength() {
  if (!locations.length) return 0;
  return locations[0].forecast.length;
}

function findLocationByName(name) {
  return locations.find(loc => loc.name === name) || null;
}

// ============================
// RUTAS
// ============================

function buildRoutes(rawRoutes) {
  return (rawRoutes || []).map(route => {
    const resolvedPoints = (route.points || [])
      .map(pointName => {
        const loc = findLocationByName(pointName);
        return {
          name: pointName,
          loc
        };
      });

    const validLocations = resolvedPoints
      .filter(p => p.loc && Array.isArray(p.loc.coords))
      .map(p => p.loc);

    return {
      ...route,
      resolvedPoints,
      locations: validLocations
    };
  });
}

function calculateRouteSummary(route) {
  if (!route || !route.locations || route.locations.length < 2) {
    return { hasData: false, reason: "Ruta sin puntos válidos" };
  }

  const startMs = new Date(route.departure_time).getTime();
  const endMs = new Date(route.arrival_time).getTime();

  if (Number.isNaN(startMs) || Number.isNaN(endMs) || endMs <= startMs) {
    return { hasData: false, reason: "Ventana temporal inválida" };
  }

  const segments = [];
  let totalMeters = 0;

  for (let i = 0; i < route.locations.length - 1; i++) {
    const a = route.locations[i];
    const b = route.locations[i + 1];

    if (!a?.coords || !b?.coords) continue;

    const segMeters = map.distance(a.coords, b.coords);
    if (!Number.isFinite(segMeters) || segMeters <= 0) continue;

    segments.push({
      startLoc: a,
      endLoc: b,
      startIndex: i,
      endIndex: i + 1,
      lengthMeters: segMeters,
      cumStart: totalMeters,
      cumEnd: totalMeters + segMeters
    });

    totalMeters += segMeters;
  }

  if (!segments.length || totalMeters <= 0) {
    return { hasData: false, reason: "No se pudo calcular la geometría de la ruta" };
  }

  const refForecast = route.locations[0].forecast || [];
  if (!refForecast.length) {
    return { hasData: false, reason: "Ruta sin datos de forecast" };
  }

  let best = null;
  let recordsInWindow = 0;

  for (let i = 0; i < refForecast.length; i++) {
    const refTime = refForecast[i]?.time;
    const t = new Date(refTime).getTime();

    if (Number.isNaN(t)) continue;
    if (t < startMs || t > endMs) continue;

    recordsInWindow += 1;

    const progress = (t - startMs) / (endMs - startMs);
    const traveledMeters = progress * totalMeters;

    let activeSegment = segments[segments.length - 1];
    for (const seg of segments) {
      if (traveledMeters >= seg.cumStart && traveledMeters <= seg.cumEnd) {
        activeSegment = seg;
        break;
      }
    }

    const segProgress = activeSegment.lengthMeters > 0
      ? (traveledMeters - activeSegment.cumStart) / activeSegment.lengthMeters
      : 0;

    const activeLoc = segProgress < 0.5
      ? activeSegment.startLoc
      : activeSegment.endLoc;

    const f = activeLoc?.forecast?.[i];
    if (!f || !isValidNumber(f.wave)) continue;

    if (!best || f.wave > best.wave) {
      best = {
        locationName: activeLoc.name,
        time: f.time,
        wave: f.wave,
        tp: f.tp,
        dir: f.dir,
        waveSource: f.waveSource
      };
    }
  }

  if (!recordsInWindow) {
    return { hasData: false, reason: "No hay datos en la ventana temporal de la ruta" };
  }

  if (!best) {
    return { hasData: false, reason: "Hay registros en la ventana, pero sin oleaje válido" };
  }

  return { hasData: true, ...best };
}

function calculateRouteDistanceNm(route) {
  if (!route?.locations || route.locations.length < 2) return null;

  let totalMeters = 0;

  for (let i = 0; i < route.locations.length - 1; i++) {
    const a = route.locations[i].coords;
    const b = route.locations[i + 1].coords;

    if (!a || !b) continue;

    totalMeters += map.distance(a, b);
  }

  return totalMeters / 1852;
}

function getRouteDisplayColor(route) {
  const summary = calculateRouteSummary(route);
  if (!summary.hasData) return "#64748b";

  const redThreshold = route?.hs_threshold ?? THRESHOLDS.orangeMax;
  return getHexColorFromHs(summary.wave, redThreshold);
}

function getCanonicalRouteKey(route) {
  const names = (route?.points || []).filter(Boolean);

  if (!names.length) return route?.id || Math.random().toString(36).slice(2);

  const forward = names.join(">");
  const backward = [...names].reverse().join(">");

  return forward < backward ? forward : backward;
}

function buildRouteGroups(routesList) {
  const groupsMap = new Map();

  (routesList || []).forEach(route => {
    const key = getCanonicalRouteKey(route);

    if (!groupsMap.has(key)) {
      groupsMap.set(key, []);
    }

    groupsMap.get(key).push(route);
  });

  return Array.from(groupsMap.values());
}

function getRouteGroupLabel(groupRoutes) {
  if (!groupRoutes || !groupRoutes.length) return "Ruta";
  if (groupRoutes.length === 1) return groupRoutes[0].name || "Ruta";
  return "Seleccionar ruta";
}

function showRouteChoicePopup(latlng, groupRoutes) {
  if (!groupRoutes || !groupRoutes.length) return;

  const popupId = `route-popup-${Date.now()}`;

  window._routePopupOptions = window._routePopupOptions || {};
  window._routePopupOptions[popupId] = Object.fromEntries(
    groupRoutes.map(route => [route.id, route])
  );

  window.selectRouteFromPopup = function(pId, routeId) {
    const route = window._routePopupOptions?.[pId]?.[routeId];
    if (!route) return;

    selectedRoute = route;
    updateRouteStyles();
    updateInfoPanel();

    if (selectedLocation) {
      renderChart();
    }

    map.closePopup();
    delete window._routePopupOptions[pId];
  };

  const buttonsHtml = groupRoutes.map(route => `
    <button
      onclick="window.selectRouteFromPopup('${popupId}', '${route.id}')"
      style="
        display:block;
        width:100%;
        margin:6px 0;
        padding:8px 10px;
        border:1px solid #d1d5db;
        border-radius:8px;
        background:#ffffff;
        color:#111827;
        text-align:left;
        cursor:pointer;
        font-size:13px;
      "
    >
      ${escapeHtml(route.name)}
    </button>
  `).join("");

  L.popup()
    .setLatLng(latlng)
    .setContent(`
      <div style="min-width:190px;">
        <div style="font-weight:700; margin-bottom:8px; color:#111827;">
          Selecciona ruta
        </div>
        ${buttonsHtml}
      </div>
    `)
    .openOn(map);
}

function updateRouteStyles() {
  routeLayers.forEach(({ routes: groupedRoutes, polyline }) => {
    const containsSelected = !!selectedRoute &&
      (groupedRoutes || []).some(route => route.id === selectedRoute.id);

    const color = containsSelected && selectedRoute
      ? getRouteDisplayColor(selectedRoute)
      : getRouteDisplayColor(groupedRoutes?.[0]);

    polyline.setStyle({
      color,
      weight: containsSelected ? 5 : 3,
      opacity: containsSelected ? 0.95 : 0.75
    });
  });
}

function initRoutes() {
  routeLayers.forEach(({ polyline }) => map.removeLayer(polyline));
  routeLayers = [];

  const groupedRoutesList = buildRouteGroups(routes);

  console.log("RUTAS CARGADAS:", routes.map(r => ({
    id: r.id,
    name: r.name,
    points: r.points
  })));

  console.log("GRUPOS DE RUTAS:", groupedRoutesList.map(group => ({
    count: group.length,
    names: group.map(r => r.name)
  })));

  groupedRoutesList.forEach(groupRoutes => {
    const baseRoute = groupRoutes[0];

    const latlngs = (baseRoute.locations || [])
      .map(loc => loc.coords)
      .filter(coords => Array.isArray(coords) && coords.length === 2);

    if (latlngs.length < 2) return;

    const polyline = L.polyline(latlngs, {
      color: getRouteDisplayColor(baseRoute),
      weight: 3,
      opacity: 0.75,
      pane: "routesPane"
    }).addTo(map);

    polyline.bringToBack();

    polyline.bindTooltip(
      groupRoutes.length > 1
        ? "Seleccionar sentido"
        : (baseRoute.name || "Ruta"),
      {
        direction: "top",
        sticky: true
      }
    );

    polyline.on("click", (e) => {
  suppressNextMapClickForDepth = true;

  if (e.originalEvent) {
    L.DomEvent.stopPropagation(e.originalEvent);
    L.DomEvent.preventDefault(e.originalEvent);
  }

  console.log("CLICK EN RUTA:", groupRoutes.map(r => r.name));

  if (groupRoutes.length === 1) {
    selectedRoute = groupRoutes[0];
    updateRouteStyles();
    updateInfoPanel();

    if (selectedLocation) {
      renderChart();
    }
    return;
  }

  showRouteChoicePopup(e.latlng, groupRoutes);
});
    routeLayers.push({
      routes: groupRoutes,
      polyline
    });
  });

  updateRouteStyles();
}
// ============================
// HELPERS RUTA <-> PUNTO
// ============================

function findRoutesForLocation(location) {
  if (!location) return [];

  return routes.filter(route =>
    (route.locations || []).some(loc => loc?.name === location.name)
  );
}

function calculateRouteTotalMeters(route) {
  if (!route?.locations || route.locations.length < 2) return null;

  let totalMeters = 0;

  for (let i = 0; i < route.locations.length - 1; i++) {
    const a = route.locations[i]?.coords;
    const b = route.locations[i + 1]?.coords;

    if (!a || !b) continue;

    const segMeters = map.distance(a, b);
    if (!Number.isFinite(segMeters) || segMeters <= 0) continue;

    totalMeters += segMeters;
  }

  return totalMeters > 0 ? totalMeters : null;
}

function calculateDistanceToPointAlongRoute(route, locationName) {
  if (!route?.locations || route.locations.length < 1) return null;

  let cumMeters = 0;

  for (let i = 0; i < route.locations.length; i++) {
    const loc = route.locations[i];
    if (loc?.name === locationName) {
      return cumMeters;
    }

    if (i < route.locations.length - 1) {
      const a = route.locations[i]?.coords;
      const b = route.locations[i + 1]?.coords;

      if (!a || !b) continue;

      const segMeters = map.distance(a, b);
      if (!Number.isFinite(segMeters) || segMeters <= 0) continue;

      cumMeters += segMeters;
    }
  }

  return null;
}

function calculateBoatPassageTime(route, location) {
  if (!route || !location) return null;

  const startMs = new Date(route.departure_time).getTime();
  const endMs = new Date(route.arrival_time).getTime();

  if (Number.isNaN(startMs) || Number.isNaN(endMs) || endMs <= startMs) {
    return null;
  }

  const totalMeters = calculateRouteTotalMeters(route);
  if (!totalMeters) return null;

  const metersToPoint = calculateDistanceToPointAlongRoute(route, location.name);
  if (metersToPoint === null || metersToPoint < 0) return null;

  const ratio = Math.max(0, Math.min(1, metersToPoint / totalMeters));
  const passageMs = startMs + ratio * (endMs - startMs);

  return new Date(passageMs).toISOString();
}

function getRouteForecastTimeBounds(route) {
  if (!route?.locations?.length) return null;

  let minMs = null;
  let maxMs = null;

  route.locations.forEach(loc => {
    (loc.forecast || []).forEach(f => {
      const t = new Date(f?.time).getTime();
      if (Number.isNaN(t)) return;

      if (minMs === null || t < minMs) minMs = t;
      if (maxMs === null || t > maxMs) maxMs = t;
    });
  });

  if (minMs === null || maxMs === null) return null;
  return { minMs, maxMs };
}

function calculateRecommendedDelay(route, redThreshold = 3.5) {
  if (!route || !route.locations || route.locations.length < 2) return null;

  const originalStartMs = new Date(route.departure_time).getTime();
  const originalEndMs = new Date(route.arrival_time).getTime();

  if (Number.isNaN(originalStartMs) || Number.isNaN(originalEndMs) || originalEndMs <= originalStartMs) {
    return null;
  }

  const durationMs = originalEndMs - originalStartMs;
  const forecastBounds = getRouteForecastTimeBounds(route);
  if (!forecastBounds) return null;

  const { maxMs: forecastMaxMs } = forecastBounds;

  // buscamos retraso en pasos de 1 hora
  for (let delayHours = 1; delayHours <= 240; delayHours++) {
    const shiftedStartMs = originalStartMs + delayHours * 3600000;
    const shiftedEndMs = shiftedStartMs + durationMs;

    // no probamos salidas que ya se salen del horizonte de forecast
    if (shiftedEndMs > forecastMaxMs) break;

    const shiftedRoute = {
      ...route,
      departure_time: new Date(shiftedStartMs).toISOString(),
      arrival_time: new Date(shiftedEndMs).toISOString()
    };

    const shiftedSummary = calculateRouteSummary(shiftedRoute);

    if (
      shiftedSummary?.hasData &&
      isValidNumber(shiftedSummary.wave) &&
      Number(shiftedSummary.wave) <= redThreshold
    ) {
      return {
        found: true,
        delayHours,
        suggestedDepartureTime: shiftedRoute.departure_time,
        suggestedArrivalTime: shiftedRoute.arrival_time,
        maxWaveOnShiftedRoute: shiftedSummary.wave,
        criticalLocationName: shiftedSummary.locationName,
        criticalTime: shiftedSummary.time
      };
    }
  }

  return {
    found: false,
    delayHours: null,
    suggestedDepartureTime: null,
    suggestedArrivalTime: null,
    maxWaveOnShiftedRoute: null,
    criticalLocationName: null,
    criticalTime: null
  };
}

function getLocationRouteTimingMarkers(location) {
  if (!location || !selectedRoute) return [];

  const pointBelongsToRoute = (selectedRoute.locations || []).some(
    loc => loc?.name === location.name
  );

  if (!pointBelongsToRoute) return [];

  const departureTime = selectedRoute?.departure_time || null;
  const arrivalTime = selectedRoute?.arrival_time || null;
  const passageTime = calculateBoatPassageTime(selectedRoute, location);

  const routeLabel = selectedRoute.name || "Ruta";
  const markers = [];

  if (departureTime) {
    markers.push({
      type: "departure",
      routeId: selectedRoute.id ?? routeLabel,
      routeName: routeLabel,
      pointName: location.name,
      time: departureTime,
      shortLabel: "S",
      color: "#8b5cf6",
      lineDash: [],
      lineWidth: 2.2
    });
  }

  if (passageTime) {
    markers.push({
      type: "passage",
      routeId: selectedRoute.id ?? routeLabel,
      routeName: routeLabel,
      pointName: location.name,
      time: passageTime,
      shortLabel: "P",
      color: "#374151",
      lineDash: [],
      lineWidth: 2.2
    });
  }

  if (arrivalTime) {
    markers.push({
      type: "arrival",
      routeId: selectedRoute.id ?? routeLabel,
      routeName: routeLabel,
      pointName: location.name,
      time: arrivalTime,
      shortLabel: "L",
      color: "#8b5cf6",
      lineDash: [],
      lineWidth: 2.2
    });
  }

  return markers;
}
// ============================
// CARGA DATOS
// ============================

Promise.all([
  fetch("./meteo_points_merged.json").then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status} cargando meteo_points_merged.json`);
    return res.json();
  }),
  fetch("./routes.json").then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status} cargando routes.json`);
    return res.json();
  })
])
  .then(([meteoData, routesData]) => {
    const rawPoints = Array.isArray(meteoData) ? meteoData : (meteoData.points || []);

    locations = rawPoints
      .map(point => {
        const coords = getPointCoords(point);
        if (!coords) return null;

        return {
          pointId: point.point_id,
          name: point.name,
          coords,
          thresholds: { ...THRESHOLDS },
          forecast: buildMergedForecast(point),
          lon: isValidNumber(point.lon) ? Number(point.lon) : null,
          lat: isValidNumber(point.lat) ? Number(point.lat) : null,
          requestedLon: isValidNumber(point.requested_lon) ? Number(point.requested_lon) : null,
          requestedLat: isValidNumber(point.requested_lat) ? Number(point.requested_lat) : null
        };
      })
      .filter(Boolean);

    if (!locations.length) {
      throw new Error("No hay puntos válidos en meteo_points_merged.json");
    }

    routes = buildRoutes(routesData);

    const maxHour = Math.max(0, getForecastLength() - 1);
    hourSlider.max = maxHour;
    hourSlider.value = selectedHour;

    initMarkers();
    initRoutes();

    const defaultLocation = locations.find(
      loc => loc.name?.toLowerCase() === "boya2"
    );

    if (defaultLocation) {
      selectedLocation = defaultLocation;
      selectedRoute = null;

      if (defaultLocation.coords) {
        map.setView(defaultLocation.coords, 7);
      }
    }

    updateHourLabel();
    updateInfoPanel();
    renderChart();
  })
  .catch(err => {
    console.error(err);
    infoPanel.innerHTML = `
      <p><strong>Error cargando datos</strong></p>
      <p>${escapeHtml(err.message)}</p>
    `;
  });

// ============================
// MARKERS
// ============================

function createObsIcon() {
  return L.divIcon({
    className: "obs-marker-icon",
    html: `
      <div class="obs-marker">
        <div class="obs-dot"></div>
        <div class="obs-stick"></div>
      </div>
    `,
    iconSize: [14, 18],
    iconAnchor: [7, 15],
    popupAnchor: [0, -18]
  });
}

function initMarkers() {
  markers.forEach(({ marker }) => map.removeLayer(marker));
  markers = [];

  locations.forEach(loc => {
    const isObsPoint = loc.name?.toLowerCase().startsWith("boya");

    let marker;

    if (isObsPoint) {
      marker = L.marker(loc.coords, {
        icon: createObsIcon()
      }).addTo(map);
    } else {
      marker = L.circleMarker(loc.coords, {
        radius: 3,
        color: "#4b5563",
        fillColor: "#4b5563",
        fillOpacity: 0.8,
        weight: 1
      }).addTo(map);
    }

    marker.bindTooltip(loc.name, { direction: "top", offset: [0, -6] });

  marker.on("click", (e) => {
  suppressNextMapClickForDepth = true;

  if (e?.originalEvent) {
    L.DomEvent.stopPropagation(e.originalEvent);
    L.DomEvent.preventDefault(e.originalEvent);
  }

  selectedLocation = loc;
  updateRouteStyles();
  updateInfoPanel();
  renderChart();
});

    markers.push({ marker, loc, isObsPoint });
  });
}

function updateMarkers() {
  markers.forEach(({ marker, isObsPoint }) => {
    const color = "#374151";

    if (!isObsPoint && marker.setStyle) {
      marker.setStyle({ color, fillColor: color });
    }
  });
}

// ============================
// PANEL
// ============================

function renderLocationInfoPanel() {
  if (!selectedLocation) return;

  const f = selectedLocation.forecast[selectedHour];

  if (!f) {
    infoPanel.innerHTML = `
      <p><strong>Name:</strong> ${escapeHtml(selectedLocation.name)}</p>
      <p><strong>No data for this time</strong></p>
    `;
    return;
  }

  const status = getRouteStatus(f.wave);

  infoPanel.innerHTML = `
    <h3>${escapeHtml(selectedLocation.name)}</h3>
    <p><strong>Time:</strong> ${formatTimeLabel(f.time)}</p>
    <p><strong>Hs:</strong> ${formatNumber(f.wave)} m (${escapeHtml(f.waveSource)})</p>
    <p><strong>Tp:</strong> ${formatNumber(f.tp)} s</p>
    <p><strong>Wave direction:</strong> ${formatNumber(f.dir)}°</p>
    <p><strong>Wind model:</strong> ${formatNumber(f.windSpeed)} m/s</p>
    <p><strong>Wind dir model:</strong> ${formatNumber(f.windDir)}°</p>
    <p><strong>Hs obs:</strong> ${formatNumber(f.waveObs)} m</p>
    <p><strong>Wind obs:</strong> ${formatNumber(f.windObs)} m/s</p>
    <p><strong>Status:</strong> <span style="color:${status.color}; font-weight:700;">${escapeHtml(status.label)}</span></p>
  `;
}

function renderRouteInfoPanel() {
  if (!selectedRoute) return;

  const summary = calculateRouteSummary(selectedRoute);
  const redThreshold = selectedRoute?.hs_threshold ?? THRESHOLDS.orangeMax;

  const startMs = new Date(selectedRoute.departure_time).getTime();
  const endMs = new Date(selectedRoute.arrival_time).getTime();

  let durationHours = null;
  if (!Number.isNaN(startMs) && !Number.isNaN(endMs) && endMs >= startMs) {
    durationHours = (endMs - startMs) / 3600000;
  }

  const hoursLabel = durationHours === null
    ? "-"
    : Number.isInteger(durationHours)
      ? `${durationHours} h`
      : `${durationHours.toFixed(1)} h`;

  const distanceNm = calculateRouteDistanceNm(selectedRoute);

  let avgSpeed = null;
  if (distanceNm !== null && durationHours !== null && durationHours > 0) {
    avgSpeed = distanceNm / durationHours;
  }

  const distanceLabel = distanceNm === null
    ? "-"
    : `${formatNumber(distanceNm, 0)} nm`;

  const speedLabel = avgSpeed === null
    ? "-"
    : `${formatNumber(avgSpeed, 1)} kn`;

  if (!summary.hasData) {
    infoPanel.innerHTML = `
      <h3>${escapeHtml(selectedRoute.name)}</h3>
      <p><strong>Salida:</strong> ${formatDateTimeLong(selectedRoute.departure_time)}</p>
      <p><strong>Llegada:</strong> ${formatDateTimeLong(selectedRoute.arrival_time)}</p>
      <p><strong>Horas:</strong> ${escapeHtml(hoursLabel)}</p>
      <p><strong>Distancia:</strong> ${distanceLabel}</p>
      <p><strong>Velocidad media:</strong> ${speedLabel}</p>
      <p><strong>Umbral Hs:</strong> ${formatNumber(redThreshold)} m</p>
      <hr style="margin:10px 0;">
      <p>${escapeHtml(summary.reason)}</p>
    `;
    return;
  }

  const status = getRouteStatus(summary.wave, redThreshold);
  const recommendation = isValidNumber(summary.wave) && Number(summary.wave) >= redThreshold
    ? calculateRecommendedDelay(selectedRoute, redThreshold)
    : null;

  infoPanel.innerHTML = `
    <h3>${escapeHtml(selectedRoute.name)}</h3>
    <p><strong>Salida:</strong> ${formatDateTimeLong(selectedRoute.departure_time)}</p>
    <p><strong>Llegada:</strong> ${formatDateTimeLong(selectedRoute.arrival_time)}</p>
    <p><strong>Horas:</strong> ${escapeHtml(hoursLabel)}</p>
    <p><strong>Distancia:</strong> ${distanceLabel}</p>
    <p><strong>Velocidad media:</strong> ${speedLabel}</p>
    <hr style="margin:10px 0;">
    <p><strong>Hsmax ruta:</strong> ${formatNumber(summary.wave)} m (${escapeHtml(summary.waveSource)})</p>
    <p><strong>Umbral Hs:</strong> ${formatNumber(redThreshold)} m</p>
    <p><strong>Tp asociado:</strong> ${formatNumber(summary.tp)} s</p>
    <p><strong>Dirección asociada:</strong> ${formatNumber(summary.dir)}°</p>
    <p><strong>Ocurre en:</strong> ${escapeHtml(summary.locationName)}</p>
    <p><strong>Hora:</strong> ${formatDateTimeLong(summary.time)}</p>
    <p><strong>Estado:</strong> <span style="color:${status.color}; font-weight:700;">${escapeHtml(status.label)}</span></p>

    ${recommendation && recommendation.found ? `
      <hr style="margin:10px 0;">
      <p><strong>Retraso recomendado:</strong> ${recommendation.delayHours} h</p>
      <p><strong>Nueva salida sugerida:</strong> ${formatDateTimeLong(recommendation.suggestedDepartureTime)}</p>
      <p><strong>Nueva llegada estimada:</strong> ${formatDateTimeLong(recommendation.suggestedArrivalTime)}</p>
      <p><strong>Hsmax con retraso:</strong> ${formatNumber(recommendation.maxWaveOnShiftedRoute)} m</p>
    ` : ""}

    ${recommendation && !recommendation.found ? `
      <hr style="margin:10px 0;">
      <p><strong>Retraso recomendado:</strong> no se encuentra una salida segura dentro del forecast disponible</p>
    ` : ""}
  `;
}

function updateInfoPanel() {
  if (selectedRoute) {
    renderRouteInfoPanel();
    return;
  }

  if (selectedLocation) {
    renderLocationInfoPanel();
    return;
  }

  infoPanel.innerHTML = `<p><strong>Selecciona un punto o una ruta</strong></p>`;
}



// ============================
// CHART PLUGIN: VERTICAL LINE
// ============================

const verticalCursorPlugin = {
  id: "verticalCursorPlugin",
  afterDraw(chart, args, options) {
    const selectedIndex = options?.selectedIndex ?? 0;
    const xScale = chart.scales.x;
    const yScale = chart.scales.y;

    if (!xScale || !yScale) return;

    const refForecast = chart.config.options?.plugins?.daySeparatorPlugin?.forecast || [];
    if (selectedIndex < 0 || selectedIndex >= refForecast.length) return;

    const selectedTime = refForecast[selectedIndex]?.time;
    if (!selectedTime) return;

    const x = xScale.getPixelForValue(selectedTime);
    const topY = chart.chartArea.top;
    const bottomY = chart.chartArea.bottom;
    const ctx = chart.ctx;

    ctx.save();
    ctx.beginPath();
    ctx.moveTo(x, topY);
    ctx.lineTo(x, bottomY);
    ctx.lineWidth = 1.5;
    ctx.strokeStyle = "#9ca3af";
    ctx.stroke();
    ctx.restore();
  }
};

// ============================
// CHART PLUGIN: TIMING MARKERS
// ============================

const routeTimingMarkersPlugin = {
  id: "routeTimingMarkersPlugin",
  afterDraw(chart, args, options) {
    const markers = options?.markers || [];
    if (!markers.length) return;

    const xScale = chart.scales.x;
    const chartArea = chart.chartArea;
    const ctx = chart.ctx;

    if (!xScale || !chartArea || !ctx) return;

    const xMin = xScale.min;
    const xMax = xScale.max;
    if (xMin == null || xMax == null) return;

    ctx.save();
    ctx.textBaseline = "top";
    ctx.font = "600 10px sans-serif";

    markers.forEach((marker, idx) => {
      const t = new Date(marker.time).getTime();
      if (Number.isNaN(t)) return;
      if (t < xMin || t > xMax) return;

      const x = xScale.getPixelForValue(t);
      const labelY = chartArea.top + 4 + ((idx % 3) * 11);
      const label = marker.shortLabel;

      ctx.save();
      ctx.strokeStyle = marker.color || "#111827";
      ctx.lineWidth = marker.lineWidth ?? 1.2;
      ctx.setLineDash(marker.lineDash || []);

      ctx.beginPath();
      ctx.moveTo(x, chartArea.top);
      ctx.lineTo(x, chartArea.bottom);
      ctx.stroke();

      ctx.setLineDash([]);
      ctx.fillStyle = marker.color || "#111827";
      ctx.fillText(label, x + 3, labelY);

      ctx.restore();
    });

    ctx.restore();
  }
};

// ============================
// CHART PLUGIN: WAVE ARROWS
// ============================

const pdeWaveArrowsPlugin = {
  id: "pdeWaveArrowsPlugin",
  afterDatasetsDraw(chart, args, options) {
    const datasetIndex = options?.datasetIndex ?? 2;
    const directions = options?.directions ?? [];
    const topPaddingPx = options?.topPaddingPx ?? 10;
    const arrowLengthPx = options?.arrowLengthPx ?? 14;
    const arrowHeadPx = options?.arrowHeadPx ?? 5;
    const lineWidth = options?.lineWidth ?? 1.4;
    const color = options?.color ?? "#4b5563";
    const minPixelGap = options?.minPixelGap ?? 18;

    const meta = chart.getDatasetMeta(datasetIndex);
    const dataset = chart.data.datasets?.[datasetIndex];
    const ctx = chart.ctx;
    const chartArea = chart.chartArea;
    const yScale = chart.scales.y;

    if (!meta || !dataset || meta.hidden) return;
    if (!meta.data || !meta.data.length) return;
    if (!chartArea || !yScale) return;

    ctx.save();
    ctx.strokeStyle = color;
    ctx.fillStyle = color;
    ctx.lineWidth = lineWidth;

    const yFixed = chartArea.top + topPaddingPx;
    let lastDrawnX = null;

    meta.data.forEach((pointEl, i) => {
      const hsValue = dataset.data?.[i]?.y ?? null;
      const dirFrom = directions[i];

      if (hsValue === null || hsValue === undefined || Number.isNaN(hsValue)) return;
      if (dirFrom === null || dirFrom === undefined || Number.isNaN(dirFrom)) return;

      const x = pointEl.x;
      const y = yFixed;

      if (lastDrawnX !== null && Math.abs(x - lastDrawnX) < minPixelGap) return;
      lastDrawnX = x;

      const arrowBearing = (dirFrom + 180) % 360;
      const rad = arrowBearing * Math.PI / 180;
      const dx = arrowLengthPx * Math.sin(rad);
      const dy = -arrowLengthPx * Math.cos(rad);

      const x1 = x - dx / 2;
      const y1 = y - dy / 2;
      const x2 = x + dx / 2;
      const y2 = y + dy / 2;

      ctx.beginPath();
      ctx.moveTo(x1, y1);
      ctx.lineTo(x2, y2);
      ctx.stroke();

      const angle = Math.atan2(y2 - y1, x2 - x1);
      const a1 = angle + Math.PI * 0.82;
      const a2 = angle - Math.PI * 0.82;

      ctx.beginPath();
      ctx.moveTo(x2, y2);
      ctx.lineTo(x2 + arrowHeadPx * Math.cos(a1), y2 + arrowHeadPx * Math.sin(a1));
      ctx.moveTo(x2, y2);
      ctx.lineTo(x2 + arrowHeadPx * Math.cos(a2), y2 + arrowHeadPx * Math.sin(a2));
      ctx.stroke();
    });

    ctx.restore();
  }
};

// ============================
// GRÁFICA
// ============================

function renderChart() {
  if (!selectedLocation || !waveChartCanvas) return;

  if (chartTitle) {
    chartTitle.textContent = selectedLocation?.name
      ? selectedLocation.name
      : "Marine forecast";
  }

  const forecast = selectedLocation.forecast;
  const routeTimingMarkers = getLocationRouteTimingMarkers(selectedLocation);
  const firstTimeMs = forecast.length ? new Date(forecast[0].time).getTime() : null;

  let xAxisMin = null;
  let xAxisMax = null;

  if (firstTimeMs !== null) {
    const firstDate = new Date(firstTimeMs);

    xAxisMin = Date.UTC(
      firstDate.getUTCFullYear(),
      firstDate.getUTCMonth(),
      firstDate.getUTCDate(),
      0, 0, 0, 0
    );

    xAxisMax = Date.UTC(
      firstDate.getUTCFullYear(),
      firstDate.getUTCMonth(),
      firstDate.getUTCDate() + 5,
      21, 0, 0, 0
    );
  }

  const hsPort = forecast.map(f => ({ x: f.time, y: f.wavePort }));
  const hsPde = forecast.map(f => ({ x: f.time, y: f.wavePde }));
  const hsCop = forecast.map(f => ({ x: f.time, y: f.waveCopernicus }));
  const hsObs = forecast.map(f => ({ x: f.time, y: f.waveObs }));

  const dirPde = forecast.map(f => f.dirPde);
  const dirCop = forecast.map(f => f.dirCopernicus);

  if (waveChart) {
    waveChart.destroy();
  }

  const daySeparatorPlugin = {
    id: "daySeparatorPlugin",
    afterDraw(chart) {
      const { ctx, chartArea, scales } = chart;
      const xScale = scales.x;
      const forecast = chart.config.options?.plugins?.daySeparatorPlugin?.forecast || [];

      if (!xScale || !forecast.length) return;

      const dayGroups = [];
      let currentGroup = null;

      for (let i = 0; i < forecast.length; i++) {
        const t = forecast[i]?.time;
        if (!t) continue;

        const d = new Date(t);
        const dayKey = `${d.getUTCFullYear()}-${d.getUTCMonth()}-${d.getUTCDate()}`;

        if (!currentGroup || currentGroup.dayKey !== dayKey) {
          currentGroup = {
            dayKey,
            startIndex: i,
            endIndex: i,
            date: new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()))
          };
          dayGroups.push(currentGroup);
        } else {
          currentGroup.endIndex = i;
        }
      }

      if (!dayGroups.length) return;

      const xMin = xScale.min;
      const xMax = xScale.max;
      if (xMin == null || xMax == null) return;

      const todayRef = new Date();
      const todayUtc = new Date(Date.UTC(
        todayRef.getUTCFullYear(),
        todayRef.getUTCMonth(),
        todayRef.getUTCDate()
      ));

      function diffDays(dateA, dateB) {
        return Math.round((dateA - dateB) / 86400000);
      }

      function getDayLabel(dayDate) {
        const d = diffDays(dayDate, todayUtc);
        if (d === -1) return "ayer";
        if (d === 0) return "hoy";
        if (d > 0) return `+${d}d`;
        return `${d}d`;
      }

      function clamp(val, min, max) {
        return Math.max(min, Math.min(max, val));
      }

      ctx.save();

      const startToday = Date.UTC(
        todayRef.getUTCFullYear(),
        todayRef.getUTCMonth(),
        todayRef.getUTCDate(),
        0, 0, 0, 0
      );

      const startTomorrow = Date.UTC(
        todayRef.getUTCFullYear(),
        todayRef.getUTCMonth(),
        todayRef.getUTCDate() + 1,
        0, 0, 0, 0
      );

      const shadeStart = Math.max(startToday, xMin);
      const shadeEnd = Math.min(startTomorrow, xMax);

      if (shadeEnd > shadeStart) {
        const leftEdge = xScale.getPixelForValue(shadeStart);
        const rightEdge = xScale.getPixelForValue(shadeEnd);

        ctx.fillStyle = "rgba(37, 99, 235, 0.05)";
        ctx.fillRect(
          leftEdge,
          chartArea.top,
          rightEdge - leftEdge,
          chartArea.bottom - chartArea.top
        );
      }

      ctx.strokeStyle = "rgba(70, 70, 70, 0.22)";
      ctx.lineWidth = 1.1;
      ctx.setLineDash([4, 4]);

      for (let i = 1; i < dayGroups.length; i++) {
        const boundaryTime = dayGroups[i].date.getTime();

        if (boundaryTime < xMin || boundaryTime > xMax) continue;

        const x = xScale.getPixelForValue(boundaryTime);
        ctx.beginPath();
        ctx.moveTo(x, chartArea.top);
        ctx.lineTo(x, chartArea.bottom);
        ctx.stroke();
      }

      ctx.setLineDash([]);
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.font = "600 11px sans-serif";

      dayGroups.forEach((group, idx) => {
        const startMs = Math.max(group.date.getTime(), xMin);

        const nextGroup = dayGroups[idx + 1];
        const naturalEndMs = nextGroup
          ? nextGroup.date.getTime()
          : (new Date(group.date.getTime() + 24 * 3600 * 1000)).getTime();

        const endMs = Math.min(naturalEndMs, xMax);

        if (endMs <= startMs) return;

        const x1 = xScale.getPixelForValue(startMs);
        const x2 = xScale.getPixelForValue(endMs);
        const xMid = (x1 + x2) / 2;

        const label = getDayLabel(group.date);

        ctx.fillStyle = label === "hoy" ? "#111827" : "#6b7280";
        ctx.fillText(
          label,
          clamp(xMid, chartArea.left + 18, chartArea.right - 18),
          chartArea.bottom + 6
        );
      });

      ctx.restore();
    }
  };

  const allHs = [
    ...hsPort.map(p => p.y),
    ...hsPde.map(p => p.y),
    ...hsCop.map(p => p.y),
    ...hsObs.map(p => p.y)
  ].filter(v => v != null && !Number.isNaN(v));

  const maxHs = allHs.length ? Math.max(...allHs) : 2;
  const yMaxChart = maxHs + 0.8;

  waveChart = new Chart(waveChartCanvas, {
    type: "line",
    data: {
      datasets: [
        {
          label: "Puerto",
          data: hsPort,
          borderColor: "#16a34a",
          backgroundColor: "transparent",
          borderWidth: 2.2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.25,
          spanGaps: true
        },
        {
          label: "PdE",
          data: hsPde,
          borderColor: "#dc2626",
          backgroundColor: "transparent",
          borderWidth: 2.2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.25,
          spanGaps: true
        },
        {
          label: "Copernicus",
          data: hsCop,
          borderColor: "#2563eb",
          backgroundColor: "transparent",
          borderWidth: 2,
          borderDash: [6, 4],
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.25,
          spanGaps: true
        },
        {
          label: "Obs",
          data: hsObs,
          borderColor: "rgba(0,0,0,0.6)",
          backgroundColor: "rgba(0,0,0,0.3)",
          borderWidth: 1.2,
          pointRadius: 1.5,
          pointHoverRadius: 3,
          tension: 0.2,
          borderDash: [],
          spanGaps: true,
          order: -10
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false
      },
      layout: {
        padding: {
          top: 26,
          bottom: 28
        }
      },
      plugins: {
        daySeparatorPlugin: {
          forecast
        },
        tooltip: {
          callbacks: {
            title: items => {
              if (!items.length) return "";
              const idx = items[0].dataIndex;
              return forecast[idx]?.time || "";
            },
            label: () => "",
            afterBody: items => {
              if (!items.length) return [];

              const idx = items[0].dataIndex;
              const f = forecast[idx];

              return [
                `Hs PdE: ${formatNumber(f.wavePde)} m`,
                `Hs Cop: ${formatNumber(f.waveCopernicus)} m`,
                `Hs Obs: ${formatNumber(f.waveObs)} m`,
                `Tp Cop: ${formatNumber(f.tpCopernicus)} s`,
                `Wind obs: ${formatNumber(f.windObs)} m/s`,
                `Wind model: ${formatNumber(f.windSpeed)} m/s`
              ];
            }
          }
        },
        verticalCursorPlugin: {
          selectedIndex: selectedHour
        },
        routeTimingMarkersPlugin: {
          markers: routeTimingMarkers
        },
        pdeWaveArrowsPlugin: {
          datasetIndex: 2,
          directions: dirCop,
          topPaddingPx: 18,
          arrowLengthPx: 12,
          arrowHeadPx: 4,
          lineWidth: 1.1,
          minPixelGap: 18
        }
      },
      scales: {
        x: {
          type: "time",
          min: xAxisMin,
          max: xAxisMax,
          time: {
            unit: "hour",
            stepSize: 3,
            round: "hour",
            tooltipFormat: "yyyy-MM-dd HH:mm",
            displayFormats: {
              hour: "dd-MMM-HH'h'"
            }
          },
          ticks: {
            source: "auto",
            stepSize: 3,
            maxRotation: 55,
            minRotation: 55,
            autoSkip: false
          },
          grid: { color: "#eef2f7" }
        },
        y: {
          beginAtZero: true,
          max: yMaxChart,
          title: {
            display: true,
            text: "Hs (m)"
          },
          grid: { color: "#e5e7eb" }
        }
      }
    },
    plugins: [
      verticalCursorPlugin,
      routeTimingMarkersPlugin,
      pdeWaveArrowsPlugin,
      daySeparatorPlugin
    ]
  });

  window.chart = waveChart;
}

function updateChartCursorOnly() {
  if (!waveChart) return;
  waveChart.options.plugins.verticalCursorPlugin.selectedIndex = selectedHour;
  waveChart.update("none");
}

// ============================
// SLIDER
// ============================

hourSlider.addEventListener("input", e => {
  selectedHour = parseInt(e.target.value, 10);

  updateMarkers();
  updateRouteStyles();
  updateInfoPanel();
  updateHourLabel();
  updateChartCursorOnly();
});

function updateHourLabel() {
  if (!locations.length) {
    hourLabel.innerText = "--";
    return;
  }

  const refLocation = selectedLocation || locations[0];
  const f = refLocation?.forecast?.[selectedHour];

  hourLabel.innerText = f?.time ? formatTimeLabel(f.time) : "--";
}
