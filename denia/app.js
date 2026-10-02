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

      if (window.seaLevelChart) {
        window.seaLevelChart.resize();
      }

      if (window.portWaveChart) {
        window.portWaveChart.resize();
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

let waveChart = null;
let seaLevelChart = null;
let portWaveChart = null;

let locations = [];
let markers = [];

// DOM
const infoPanel = document.getElementById("info-panel");
const hourSlider = document.getElementById("hour-slider");
const hourLabel = document.getElementById("hour-label");

const waveChartCanvas = document.getElementById("wave-chart");
const seaLevelChartCanvas = document.getElementById("sea-level-chart");
const portWaveChartCanvas = document.getElementById("port-wave-chart");

const chartTitle = document.getElementById("chart-title");
const bottomChart = waveChartCanvas
  ? waveChartCanvas.closest(".bottom-chart")
  : null;

// Al abrir Meteoport, la gráfica permanece oculta.
// Se mostrará únicamente cuando el usuario seleccione un punto.
if (bottomChart) {
  bottomChart.classList.add("chart-hidden");
}


// ============================
// MAPA
// ============================

const map = L.map("map");

map.setView([39.05, 1.75], 7);
setTimeout(() => {
  map.invalidateSize();
  map.setView([39.05, 1.75], 7);
}, 300);
window.map = map;

L.tileLayer(
  "https://basemaps.cartocdn.com/rastertiles/light_all/{z}/{x}/{y}.png?key=cb1_45z1_1_419a4a4b3d4e30466605f6b5",
  {
    attribution: "&copy; OpenStreetMap &copy; CARTO",
    maxZoom: 19
  }
).addTo(map);


// ============================
// HELPERS
// ============================

function formatNumber(val, decimals = 2) {
  if (
    val === null ||
    val === undefined ||
    Number.isNaN(Number(val))
  ) {
    return "-";
  }

  return Number(val).toFixed(decimals);
}


function formatTimeLabel(isoTime) {
  if (!isoTime) return "--";

  const d = new Date(isoTime);

  const months = [
    "ene", "feb", "mar", "abr",
    "may", "jun", "jul", "ago",
    "sep", "oct", "nov", "dic"
  ];

  const month = months[d.getUTCMonth()];
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");

  return `${month}-${day}-${hour}h`;
}


function formatDateTimeLong(isoTime) {
  if (!isoTime) return "--";

  const d = new Date(isoTime);

  const months = [
    "ene", "feb", "mar", "abr",
    "may", "jun", "jul", "ago",
    "sep", "oct", "nov", "dic"
  ];

  const year = d.getUTCFullYear();
  const month = months[d.getUTCMonth()];
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  const min = String(d.getUTCMinutes()).padStart(2, "0");

  return `${day} ${month} ${year} ${hour}:${min} UTC`;
}


function getColor(hs) {
  if (
    hs === null ||
    hs === undefined ||
    Number.isNaN(Number(hs))
  ) {
    return "#9ca3af";
  }

  if (hs < THRESHOLDS.greenMax) return "green";
  if (hs < THRESHOLDS.yellowMax) return "yellow";
  if (hs < THRESHOLDS.orangeMax) return "orange";

  return "red";
}


function getHexColorFromHs(hs) {
  if (
    hs === null ||
    hs === undefined ||
    Number.isNaN(Number(hs))
  ) {
    return "#94a3b8";
  }

  if (hs < THRESHOLDS.greenMax) return "#16a34a";
  if (hs < THRESHOLDS.yellowMax) return "#eab308";
  if (hs < THRESHOLDS.orangeMax) return "#f97316";

  return "#dc2626";
}


function getRouteStatus(hs) {
  if (
    hs === null ||
    hs === undefined ||
    Number.isNaN(Number(hs))
  ) {
    return {
      label: "Sin datos",
      color: "#64748b"
    };
  }

  if (hs < 1) {
    return {
      label: "Operativo",
      color: "#16a34a"
    };
  }

  if (hs < 2) {
    return {
      label: "Precaución",
      color: "#eab308"
    };
  }

  if (hs < 3) {
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
  return (
    v !== null &&
    v !== undefined &&
    !Number.isNaN(Number(v))
  );
}


function getPointCoords(point) {
  const lat = isValidNumber(point.requested_lat)
    ? Number(point.requested_lat)
    : (
        isValidNumber(point.lat)
          ? Number(point.lat)
          : null
      );

  const lon = isValidNumber(point.requested_lon)
    ? Number(point.requested_lon)
    : (
        isValidNumber(point.lon)
          ? Number(point.lon)
          : null
      );

  if (!isValidNumber(lat) || !isValidNumber(lon)) {
    return null;
  }

  return [lat, lon];
}


// ============================
// OLEAJE OPERATIVO
// ============================

function getOperationalWave(f) {
  const hasPde = isValidNumber(f?.hs_pde);
  const hasPort = isValidNumber(f?.hs_port_pred);
  const hasCop = isValidNumber(f?.hs_cop);

  if (hasPde) {
    return {
      wave: Number(f.hs_pde),
      tp: isValidNumber(f.tp_pde)
        ? Number(f.tp_pde)
        : null,
      dir: isValidNumber(f.di_pde)
        ? Number(f.di_pde)
        : null,
      source: "PdE"
    };
  }

  if (hasPort) {
    return {
      wave: Number(f.hs_port_pred),
      tp: null,
      dir: null,
      source: "Puerto"
    };
  }

  if (hasCop) {
    return {
      wave: Number(f.hs_cop),
      tp: isValidNumber(f.tp_cop)
        ? Number(f.tp_cop)
        : null,
      dir: isValidNumber(f.di_cop)
        ? Number(f.di_cop)
        : null,
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


// ============================
// PREPARAR FORECAST
// ============================

function buildMergedForecast(point) {

  return (point.forecast || []).map((f, i) => {

    const op = getOperationalWave(f);

    return {

      hour: i,
      time: f.time,

      // Operacional
      wave: op.wave,
      tp: op.tp,
      dir: op.dir,
      waveSource: op.source,

      // Puertos del Estado
      wavePde: isValidNumber(f.hs_pde)
        ? Number(f.hs_pde)
        : null,

      tpPde: isValidNumber(f.tp_pde)
        ? Number(f.tp_pde)
        : null,

      dirPde: isValidNumber(f.di_pde)
        ? Number(f.di_pde)
        : null,

      // Copernicus
      waveCopernicus: isValidNumber(f.hs_cop)
        ? Number(f.hs_cop)
        : null,

      tpCopernicus: isValidNumber(f.tp_cop)
        ? Number(f.tp_cop)
        : null,

      dirCopernicus: isValidNumber(f.di_cop)
        ? Number(f.di_cop)
        : null,

      // Puerto de Dénia - predicción
      wavePort: isValidNumber(f.hs_port_pred)
        ? Number(f.hs_port_pred)
        : null,

      seaLevelPort: isValidNumber(f.sea_level_pred)
        ? Number(f.sea_level_pred)
        : null,

      // Puerto de Dénia - observación
      wavePortObs: isValidNumber(f.hs_port_obs)
        ? Number(f.hs_port_obs)
        : null,

      seaLevelPortObs: isValidNumber(f.sea_level_obs)
        ? Number(f.sea_level_obs)
        : null,

      // Observaciones boya
      waveObs: isValidNumber(f.hs_obs)
        ? Number(f.hs_obs)
        : null,

      windObs: isValidNumber(f.wspeed_obs)
        ? Number(f.wspeed_obs)
        : null,

      // Viento modelo
      windSpeed: isValidNumber(f.wspeed_mod)
        ? Number(f.wspeed_mod)
        : null,

      windDir: isValidNumber(f.wsdir_mod)
        ? Number(f.wsdir_mod)
        : null
    };
  });
}


function getForecastLength() {
  if (!locations.length) return 0;

  return locations[0].forecast.length;
}


function findLocationByName(name) {
  return locations.find(
    loc => loc.name === name
  ) || null;
}


// ============================
// CARGA DE DATOS
// ============================

fetch("./meteo_points_merged.json")

  .then(res => {

    if (!res.ok) {
      throw new Error(
        `HTTP ${res.status} cargando meteo_points_merged.json`
      );
    }

    return res.json();
  })

  .then(meteoData => {

    const rawPoints = Array.isArray(meteoData)
      ? meteoData
      : (meteoData.points || []);

    locations = rawPoints

      .map(point => {

        const coords = getPointCoords(point);

        if (!coords) {
          return null;
        }

        return {

          pointId: point.point_id,
          name: point.name,
          coords,

          thresholds: {
            ...THRESHOLDS
          },

          forecast: buildMergedForecast(point),

          lon: isValidNumber(point.lon)
            ? Number(point.lon)
            : null,

          lat: isValidNumber(point.lat)
            ? Number(point.lat)
            : null,

          requestedLon: isValidNumber(point.requested_lon)
            ? Number(point.requested_lon)
            : null,

          requestedLat: isValidNumber(point.requested_lat)
            ? Number(point.requested_lat)
            : null
        };
      })

      .filter(Boolean);


    if (!locations.length) {

      throw new Error(
        "No hay puntos válidos en meteo_points_merged.json"
      );
    }


    const maxHour = Math.max(
      0,
      getForecastLength() - 1
    );

    hourSlider.max = maxHour;
    hourSlider.value = selectedHour;

    initMarkers();
// Punto mostrado por defecto al abrir la web
selectedLocation = findLocationByName("denia_puerto");

if (selectedLocation) {

  if (bottomChart) {
    bottomChart.classList.remove("chart-hidden");
  }

  renderChart();
}

    updateHourLabel();
    updateInfoPanel();

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

  markers.forEach(({ marker }) => {
    map.removeLayer(marker);
  });

  markers = [];


  locations.forEach(loc => {

    const isObsPoint =
      loc.name?.toLowerCase().startsWith("boya");

    let marker;


    if (isObsPoint) {

      marker = L.marker(
        loc.coords,
        {
          icon: createObsIcon()
        }
      ).addTo(map);

    } else {

      marker = L.circleMarker(
        loc.coords,
        {
          radius: 3,
          color: "#1f2937",
          fillColor: "#1f2937",
          fillOpacity: 0.9,
          weight: 1
        }
      ).addTo(map);
    }


    marker.bindTooltip(
      loc.name,
      {
        direction: "top",
        offset: [0, -6]
      }
    );


    marker.on("click", () => {

      selectedLocation = loc;

      if (bottomChart) {
        bottomChart.classList.remove("chart-hidden");
      }

      updateInfoPanel();
      renderChart();

      // Al cambiar la altura disponible, Leaflet y Chart.js
      // recalculan su tamaño correctamente.
      setTimeout(() => {
        map.invalidateSize();

        if (window.chart) window.chart.resize();
        if (window.seaLevelChart) window.seaLevelChart.resize();
        if (window.portWaveChart) window.portWaveChart.resize();
      }, 50);

    });


    markers.push({
      marker,
      loc,
      isObsPoint
    });

  });
}


function updateMarkers() {

  markers.forEach(
    ({ marker, isObsPoint }) => {

      const color = "#374151";

      if (
        !isObsPoint &&
        marker.setStyle
      ) {

        marker.setStyle({
          color,
          fillColor: color
        });

      }
    }
  );
}


// ============================
// PANEL DE INFORMACIÓN
// ============================

function renderLocationInfoPanel() {

  if (!selectedLocation) return;

  const f =
    selectedLocation.forecast[selectedHour];


  if (!f) {

    infoPanel.innerHTML = `
      <p><strong>Name:</strong>
      ${escapeHtml(selectedLocation.name)}</p>

      <p><strong>No data for this time</strong></p>
    `;

    return;
  }


  const status =
    getRouteStatus(f.wave);


  infoPanel.innerHTML = `

    <h3>
      ${escapeHtml(selectedLocation.name)}
    </h3>

    <p>
      <strong>Time:</strong>
      ${formatTimeLabel(f.time)}
    </p>

    <p>
      <strong>Hs:</strong>
      ${formatNumber(f.wave)} m
      (${escapeHtml(f.waveSource)})
    </p>

    <p>
      <strong>Tp:</strong>
      ${formatNumber(f.tp)} s
    </p>

    <p>
      <strong>Wave direction:</strong>
      ${formatNumber(f.dir)}°
    </p>

    <p>
      <strong>Wind model:</strong>
      ${formatNumber(f.windSpeed)} m/s
    </p>

    <p>
      <strong>Wind dir model:</strong>
      ${formatNumber(f.windDir)}°
    </p>

    <p>
      <strong>Hs obs:</strong>
      ${formatNumber(
        f.wavePortObs !== null
          ? f.wavePortObs
          : f.waveObs
      )} m
    </p>

    <p>
      <strong>Wind obs:</strong>
      ${formatNumber(f.windObs)} m/s
    </p>

    <p>
      <strong>Status:</strong>

      <span
        style="
          color:${status.color};
          font-weight:700;
        "
      >
        ${escapeHtml(status.label)}
      </span>

    </p>
  `;
}


function updateInfoPanel() {

  if (selectedLocation) {

    renderLocationInfoPanel();
    return;
  }

  infoPanel.innerHTML = `
    <p>
      <strong>Selecciona un punto</strong>
    </p>
  `;
}


// ============================
// PLUGIN CURSOR VERTICAL
// ============================

const verticalCursorPlugin = {

  id: "verticalCursorPlugin",

  afterDraw(chart, args, options) {

    const selectedIndex =
      options?.selectedIndex ?? 0;

    const xScale =
      chart.scales.x;

    const yScale =
      chart.scales.y;

    if (!xScale || !yScale) {
      return;
    }


    const refForecast =
      chart.config.options
        ?.plugins
        ?.daySeparatorPlugin
        ?.forecast || [];


    if (
      selectedIndex < 0 ||
      selectedIndex >= refForecast.length
    ) {
      return;
    }


    const selectedTime =
      refForecast[selectedIndex]?.time;

    if (!selectedTime) {
      return;
    }


    const x =
      xScale.getPixelForValue(
        selectedTime
      );

    const topY =
      chart.chartArea.top;

    const bottomY =
      chart.chartArea.bottom;

    const ctx =
      chart.ctx;


    ctx.save();

    ctx.beginPath();

    ctx.moveTo(
      x,
      topY
    );

    ctx.lineTo(
      x,
      bottomY
    );

    ctx.lineWidth = 1.5;

    ctx.strokeStyle =
      "#9ca3af";

    ctx.stroke();

    ctx.restore();
  }
};

// ============================
// PLUGIN FLECHAS OLEAJE
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

      const hsValue =
        dataset.data?.[i]?.y ?? null;

      const dirFrom =
        directions[i];

      if (
        hsValue === null ||
        hsValue === undefined ||
        Number.isNaN(hsValue)
      ) {
        return;
      }

      if (
        dirFrom === null ||
        dirFrom === undefined ||
        Number.isNaN(dirFrom)
      ) {
        return;
      }

      const x = pointEl.x;
      const y = yFixed;

      if (
        lastDrawnX !== null &&
        Math.abs(x - lastDrawnX) < minPixelGap
      ) {
        return;
      }

      lastDrawnX = x;

      const arrowBearing =
        (dirFrom + 180) % 360;

      const rad =
        arrowBearing * Math.PI / 180;

      const dx =
        arrowLengthPx * Math.sin(rad);

      const dy =
        -arrowLengthPx * Math.cos(rad);

      const x1 = x - dx / 2;
      const y1 = y - dy / 2;

      const x2 = x + dx / 2;
      const y2 = y + dy / 2;

      ctx.beginPath();

      ctx.moveTo(x1, y1);
      ctx.lineTo(x2, y2);

      ctx.stroke();

      const angle =
        Math.atan2(
          y2 - y1,
          x2 - x1
        );

      const a1 =
        angle + Math.PI * 0.82;

      const a2 =
        angle - Math.PI * 0.82;

      ctx.beginPath();

      ctx.moveTo(x2, y2);

      ctx.lineTo(
        x2 + arrowHeadPx * Math.cos(a1),
        y2 + arrowHeadPx * Math.sin(a1)
      );

      ctx.moveTo(x2, y2);

      ctx.lineTo(
        x2 + arrowHeadPx * Math.cos(a2),
        y2 + arrowHeadPx * Math.sin(a2)
      );

      ctx.stroke();
    });

    ctx.restore();
  }
};


// ============================
// PLUGIN SEPARACIÓN DE DÍAS
// ============================

const daySeparatorPlugin = {

  id: "daySeparatorPlugin",

  afterDraw(chart) {

    const {
      ctx,
      chartArea,
      scales
    } = chart;

    const xScale =
      scales.x;

    const forecast =
      chart.config.options
        ?.plugins
        ?.daySeparatorPlugin
        ?.forecast || [];

    if (
      !xScale ||
      !forecast.length
    ) {
      return;
    }


    const xMin =
      xScale.min;

    const xMax =
      xScale.max;

    if (
      xMin == null ||
      xMax == null
    ) {
      return;
    }


    const todayRef =
      new Date();


    const startToday =
      Date.UTC(
        todayRef.getUTCFullYear(),
        todayRef.getUTCMonth(),
        todayRef.getUTCDate(),
        0, 0, 0, 0
      );


    const startTomorrow =
      Date.UTC(
        todayRef.getUTCFullYear(),
        todayRef.getUTCMonth(),
        todayRef.getUTCDate() + 1,
        0, 0, 0, 0
      );


    ctx.save();


    // ============================
    // SOMBREADO DEL DÍA DE HOY
    // ============================

    const shadeStart =
      Math.max(
        startToday,
        xMin
      );

    const shadeEnd =
      Math.min(
        startTomorrow,
        xMax
      );


    if (
      shadeEnd > shadeStart
    ) {

      const leftEdge =
        xScale.getPixelForValue(
          shadeStart
        );

      const rightEdge =
        xScale.getPixelForValue(
          shadeEnd
        );

      ctx.fillStyle =
        "rgba(37, 99, 235, 0.05)";

      ctx.fillRect(
        leftEdge,
        chartArea.top,
        rightEdge - leftEdge,
        chartArea.bottom - chartArea.top
      );
    }


    // ============================
    // LÍNEAS CADA 24 HORAS
    // ============================

    ctx.strokeStyle =
      "rgba(70,70,70,0.22)";

    ctx.lineWidth = 1.1;

    ctx.setLineDash(
      [4, 4]
    );


    let boundary =
      Date.UTC(
        new Date(xMin).getUTCFullYear(),
        new Date(xMin).getUTCMonth(),
        new Date(xMin).getUTCDate() + 1,
        0, 0, 0, 0
      );


    while (
      boundary <= xMax
    ) {

      const x =
        xScale.getPixelForValue(
          boundary
        );

      ctx.beginPath();

      ctx.moveTo(
        x,
        chartArea.top
      );

      ctx.lineTo(
        x,
        chartArea.bottom
      );

      ctx.stroke();

      boundary +=
        24 * 3600 * 1000;
    }
    
    // ============================
    // ETIQUETAS RELATIVAS DE DÍAS
    // ============================

    ctx.setLineDash([]);

    ctx.font = "600 12px Arial";
    ctx.fillStyle = "#4b5563";
    ctx.textAlign = "center";
    ctx.textBaseline = "bottom";

    // Etiquetas desde -1d hasta el final real de la gráfica
    const oneDay = 24 * 3600 * 1000;

    let dayOffset = -1;

    while (true) {

      const dayStart =
        startToday + dayOffset * oneDay;

      const dayEnd =
        dayStart + oneDay;

      // Centro del día, limitado al rango visible
      const visibleStart =
        Math.max(dayStart, xMin);

      const visibleEnd =
        Math.min(dayEnd, xMax);

      if (visibleStart < visibleEnd) {

        const centerTime =
          (visibleStart + visibleEnd) / 2;

        const x =
          xScale.getPixelForValue(centerTime);

        let label;

        if (dayOffset === 0) {
          label = "Hoy";
        } else if (dayOffset > 0) {
          label = `+${dayOffset}d`;
        } else {
          label = `${dayOffset}d`;
        }

        ctx.fillText(
          label,
          x,
          chartArea.bottom - 4
        );
      }

      if (dayEnd > xMax) {
        break;
      }

      dayOffset++;
    }

    ctx.restore();
  }
};


// ============================
// RANGO TEMPORAL COMÚN
// ============================

function getChartTimeRange(forecast) {

  if (!forecast.length) {

    return {
      min: null,
      max: null
    };
  }


  const firstTimeMs =
    new Date(
      forecast[0].time
    ).getTime();


  const firstDate =
    new Date(firstTimeMs);


  const min =
    Date.UTC(
      firstDate.getUTCFullYear(),
      firstDate.getUTCMonth(),
      firstDate.getUTCDate(),
      0, 0, 0, 0
    );


  // Primer día + 5 días,
  // terminando a las 21 UTC
  const max =
    Date.UTC(
      firstDate.getUTCFullYear(),
      firstDate.getUTCMonth(),
      firstDate.getUTCDate() + 5,
      21, 0, 0, 0
    );


  return {
    min,
    max
  };
}

// ============================
// UMBRALES AGITACIÓN PUERTO
// ============================

const portThresholdPlugin = {

  id: "portThresholdPlugin",

  afterDraw(chart) {

    const { ctx, chartArea, scales } = chart;

    if (!chartArea || !scales.y) return;

    const yScale = scales.y;

    const thresholds = [
      { value: 0.3, color: "#16a34a" },
      { value: 0.5, color: "#f59e0b" },
      { value: 0.8, color: "#dc2626" }
    ];

    ctx.save();

    thresholds.forEach(t => {

      const y = yScale.getPixelForValue(t.value);

      ctx.beginPath();

      ctx.setLineDash([6, 5]);

      ctx.strokeStyle = t.color;
      ctx.lineWidth = 1.2;

      ctx.moveTo(chartArea.left, y);
      ctx.lineTo(chartArea.right, y);

      ctx.stroke();

    });

    ctx.restore();
  }
};

// ==================================================
// GRÁFICAS ESPECIALES DEL PUERTO DE DÉNIA
// ==================================================
function renderPortCharts() {

  if (
    !selectedLocation ||
    !waveChartCanvas
  ) {
    return;
  }

  const forecast =
    selectedLocation.forecast;

  // Mostrar SOLO la gráfica normal
  waveChartCanvas.style.display = "block";

  if (seaLevelChartCanvas) {
    seaLevelChartCanvas.style.display = "none";
  }

  if (portWaveChartCanvas) {
    portWaveChartCanvas.style.display = "none";
  }


  // Destruir gráficas anteriores
  if (waveChart) {
    waveChart.destroy();
    waveChart = null;
  }

  if (seaLevelChart) {
    seaLevelChart.destroy();
    seaLevelChart = null;
  }

  if (portWaveChart) {
    portWaveChart.destroy();
    portWaveChart = null;
  }


  if (chartTitle) {
    chartTitle.textContent =
      "Agitación - Puerto de Dénia";
  }


  // ============================
  // DATOS
  // ============================

  const hsPred =
    forecast.map(f => ({
      x: f.time,
      y: f.wavePort
    }));


  const hsObs =
    forecast.map(f => ({
      x: f.time,
      y: f.wavePortObs
    }));


  // ============================
  // RANGO TEMPORAL
  // ============================

  const validTimes =
    forecast
      .filter(f =>
        isValidNumber(f.wavePort)
      )
      .map(f =>
        new Date(f.time).getTime()
      )
      .filter(Number.isFinite);


  if (!validTimes.length) {
    return;
  }


  const firstTime =
    Math.min(...validTimes);

  const lastTime =
    Math.max(...validTimes);

  const firstDate =
    new Date(firstTime);


  const timeRange = {

    min: Date.UTC(
      firstDate.getUTCFullYear(),
      firstDate.getUTCMonth(),
      firstDate.getUTCDate(),
      0, 0, 0, 0
    ),

    max: lastTime
  };


  // ============================
  // ESCALA Y
  // ============================

  const allHs = [

    ...hsPred.map(p => p.y),

    ...hsObs.map(p => p.y)

  ].filter(
    v =>
      v != null &&
      !Number.isNaN(v)
  );


  const maxHs =
    allHs.length
      ? Math.max(...allHs)
      : 1;


  const yMaxChart =
  Math.max(1.0, maxHs + 0.15);


  // ============================
  // GRÁFICA
  // ============================

  waveChart =
    new Chart(
      waveChartCanvas,
      {

        type: "line",

        data: {

          datasets: [

            {
              label:
                "Predicción",

              data:
                hsPred,

              borderColor:
                "#16a34a",

              backgroundColor:
                "transparent",

              borderWidth:
                2.2,

              pointRadius:
                0,

              pointHoverRadius:
                4,

              tension:
                0.25,

              spanGaps:
                true
            },


            {
              label:
                "Obs",

              data:
                hsObs,

              borderColor:
                "rgba(0,0,0,0.6)",

              backgroundColor:
                "rgba(0,0,0,0.3)",

              borderWidth:
                1.2,

              pointRadius:
                1.5,

              pointHoverRadius:
                3,

              tension:
                0.2,

              spanGaps:
                true
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
              top: 20,
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

                  if (!items.length) {
                    return "";
                  }

                  const idx =
                    items[0].dataIndex;

                  return (
                    forecast[idx]?.time ||
                    ""
                  );
                },


                label: () => "",


                afterBody: items => {

                  if (!items.length) {
                    return [];
                  }

                  const idx =
                    items[0].dataIndex;

                  const f =
                    forecast[idx];

                  return [

                    `Hs predicción: ${formatNumber(f.wavePort)} m`,

                    `Hs observada: ${formatNumber(f.wavePortObs)} m`

                  ];
                }
              }
            },


            verticalCursorPlugin: {
              selectedIndex:
                selectedHour
            }

                     },


          scales: {

            x: {

              type: "time",

              min:
                timeRange.min,

              max:
                timeRange.max,


              time: {

                unit:
                  "hour",

                stepSize:
                  3,

                round:
                  "hour",

                tooltipFormat:
                  "yyyy-MM-dd HH:mm",

                displayFormats: {
                  hour:
                    "dd-MMM-HH'h'"
                }
              },


              ticks: {

                source:
                  "auto",

                stepSize:
                  3,

                maxRotation:
                  55,

                minRotation:
                  55,

                autoSkip:
                  false
              },


              grid: {
                color:
                  "#eef2f7"
              }
            },


            y: {

              beginAtZero:
                true,

              max:
                yMaxChart,

              title: {
                display: true,
                text: "Hs (m)"
              },

              grid: {
                color:
                  "#e5e7eb"
              }
            }
          }
        },


        plugins: [

  verticalCursorPlugin,

  daySeparatorPlugin,

  portThresholdPlugin

]
      }
    );


  window.chart =
    waveChart;
}

// ==================================================
// GRÁFICA GENERAL
// ==================================================

function renderChart() {

  if (
    !selectedLocation ||
    !waveChartCanvas
  ) {
    return;
  }


  const isPort =
    selectedLocation.name ===
    "denia_puerto";


  // ============================
  // PUERTO
  // ============================

if (isPort) {

  waveChartCanvas.style.display =
    "block";

  seaLevelChartCanvas.style.display =
    "none";

  portWaveChartCanvas.style.display =
    "none";

  renderPortCharts();

  return;
}


  // ============================
  // RESTO DE PUNTOS
  // ============================

  waveChartCanvas.style.display =
    "block";

  seaLevelChartCanvas.style.display =
    "none";

  portWaveChartCanvas.style.display =
    "none";


  if (seaLevelChart) {

    seaLevelChart.destroy();

    seaLevelChart = null;
  }


  if (portWaveChart) {

    portWaveChart.destroy();

    portWaveChart = null;
  }


  if (chartTitle) {

    chartTitle.textContent =
      selectedLocation?.name
        ? selectedLocation.name
        : "Marine forecast";
  }


  const forecast =
    selectedLocation.forecast;


  const timeRange =
    getChartTimeRange(
      forecast
    );


  const hsPort =
    forecast.map(f => ({
      x: f.time,
      y: f.wavePort
    }));


  const hsPde =
    forecast.map(f => ({
      x: f.time,
      y: f.wavePde
    }));


  const hsCop =
    forecast.map(f => ({
      x: f.time,
      y: f.waveCopernicus
    }));


  const hsObs =
    forecast.map(f => ({
      x: f.time,
      y: f.waveObs
    }));


  const dirCop =
    forecast.map(
      f => f.dirCopernicus
    );


  if (waveChart) {

    waveChart.destroy();

    waveChart = null;
  }


  const allHs = [

    ...hsPort.map(
      p => p.y
    ),

    ...hsPde.map(
      p => p.y
    ),

    ...hsCop.map(
      p => p.y
    ),

    ...hsObs.map(
      p => p.y
    )

  ].filter(
    v =>
      v != null &&
      !Number.isNaN(v)
  );


  const maxHs =
    allHs.length
      ? Math.max(...allHs)
      : 2;


  const yMaxChart =
    maxHs + 0.8;


  waveChart =
    new Chart(
      waveChartCanvas,
      {

        type: "line",

        data: {

          datasets: [

            {
              label:
                "Puerto",

              data:
                hsPort,

              borderColor:
                "#16a34a",

              backgroundColor:
                "transparent",

              borderWidth:
                2.2,

              pointRadius:
                0,

              pointHoverRadius:
                4,

              tension:
                0.25,

              spanGaps:
                true
            },


            {
              label:
                "PdE",

              data:
                hsPde,

              borderColor:
                "#dc2626",

              backgroundColor:
                "transparent",

              borderWidth:
                2.2,

              pointRadius:
                0,

              pointHoverRadius:
                4,

              tension:
                0.25,

              spanGaps:
                true
            },


            {
              label:
                "Copernicus",

              data:
                hsCop,

              borderColor:
                "#2563eb",

              backgroundColor:
                "transparent",

              borderWidth:
                2,

              borderDash:
                [6, 4],

              pointRadius:
                0,

              pointHoverRadius:
                4,

              tension:
                0.25,

              spanGaps:
                true
            },


            {
              label:
                "Obs",

              data:
                hsObs,

              borderColor:
                "rgba(0,0,0,0.6)",

              backgroundColor:
                "rgba(0,0,0,0.3)",

              borderWidth:
                1.2,

              pointRadius:
                1.5,

              pointHoverRadius:
                3,

              tension:
                0.2,

              spanGaps:
                true,

              order:
                -10
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
              top: 20,
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

                  if (!items.length) {
                    return "";
                  }

                  const idx =
                    items[0].dataIndex;

                  return (
                    forecast[idx]?.time ||
                    ""
                  );
                },


                label: () => "",


                afterBody: items => {

                  if (!items.length) {
                    return [];
                  }

                  const idx =
                    items[0].dataIndex;

                  const f =
                    forecast[idx];


                  return [

                    `Hs Puerto: ${formatNumber(f.wavePort)} m`,

                    `Hs PdE: ${formatNumber(f.wavePde)} m`,

                    `Tp PdE: ${formatNumber(f.tpPde)} s`,

                    `Di PdE: ${formatNumber(f.dirPde, 0)}°`,

                    `Hs Copernicus: ${formatNumber(f.waveCopernicus)} m`,

                    `Tp Copernicus: ${formatNumber(f.tpCopernicus)} s`,

                    `Di Copernicus: ${formatNumber(f.dirCopernicus, 0)}°`,

                    `Hs Obs: ${formatNumber(f.waveObs)} m`,

                    `Wind obs: ${formatNumber(f.windObs)} m/s`,

                    `Wind model: ${formatNumber(f.windSpeed)} m/s`,

                    `Wind dir model: ${formatNumber(f.windDir, 0)}°`

                  ];
                }
              }
            },


            verticalCursorPlugin: {
              selectedIndex:
                selectedHour
            },


            pdeWaveArrowsPlugin: {

              datasetIndex:
                2,

              directions:
                dirCop,

              topPaddingPx:
                18,

              arrowLengthPx:
                12,

              arrowHeadPx:
                4,

              lineWidth:
                1.1,

              minPixelGap:
                18
            }
          },


          scales: {

            x: {

              type: "time",

              min:
                timeRange.min,

              max:
                timeRange.max,

              time: {

                unit:
                  "hour",

                stepSize:
                  3,

                round:
                  "hour",

                tooltipFormat:
                  "yyyy-MM-dd HH:mm",

                displayFormats: {
                  hour:
                    "dd-MMM-HH'h'"
                }
              },


              ticks: {

                source:
                  "auto",

                stepSize:
                  3,

                maxRotation:
                  55,

                minRotation:
                  55,

                autoSkip:
                  false
              },


              grid: {
                color:
                  "#eef2f7"
              }
            },


            y: {

              beginAtZero:
                true,

              max:
                yMaxChart,

              title: {
                display: true,
                text: "Hs (m)"
              },

              grid: {
                color:
                  "#e5e7eb"
              }
            }
          }
        },


        plugins: [

          verticalCursorPlugin,

          pdeWaveArrowsPlugin,

          daySeparatorPlugin

        ]
      }
    );


  window.chart =
    waveChart;
}


// ============================
// ACTUALIZAR CURSOR DEL SLIDER
// ============================

function updateChartCursorOnly() {

  // ============================
  // GRÁFICA NORMAL
  // ============================

  if (
    waveChart &&
    waveChart.options
      ?.plugins
      ?.verticalCursorPlugin
  ) {

    waveChart.options
      .plugins
      .verticalCursorPlugin
      .selectedIndex =
        selectedHour;

    waveChart.update("none");
  }


  // ============================
  // NIVEL DEL MAR
  // ============================

  if (
    seaLevelChart &&
    seaLevelChart.options
      ?.plugins
      ?.verticalCursorPlugin
  ) {

    seaLevelChart.options
      .plugins
      .verticalCursorPlugin
      .selectedIndex =
        selectedHour;

    seaLevelChart.update("none");
  }


  // ============================
  // AGITACIÓN PUERTO
  // ============================

  if (
    portWaveChart &&
    portWaveChart.options
      ?.plugins
      ?.verticalCursorPlugin
  ) {

    portWaveChart.options
      .plugins
      .verticalCursorPlugin
      .selectedIndex =
        selectedHour;

    portWaveChart.update("none");
  }
}


// ============================
// SLIDER
// ============================

hourSlider.addEventListener(
  "input",
  e => {

    selectedHour =
      parseInt(
        e.target.value,
        10
      );


    updateMarkers();

    updateInfoPanel();

    updateHourLabel();

    updateChartCursorOnly();
  }
);


// ============================
// ETIQUETA DEL SLIDER
// ============================

function updateHourLabel() {

  if (!locations.length) {

    hourLabel.innerText =
      "--";

    return;
  }


  const refLocation =
    selectedLocation ||
    locations[0];


  const f =
    refLocation
      ?.forecast
      ?.[selectedHour];


  hourLabel.innerText =
    f?.time
      ? formatTimeLabel(f.time)
      : "--";
}   

