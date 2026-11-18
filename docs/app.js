const byId = (id) => document.getElementById(id);
const endpointRegistry = "https://raw.githubusercontent.com/CSE210-Team05/Bluesky-SpoilerDetection/live-metrics-endpoint-registry/docs/metrics-endpoint.json";
const svgNamespace = "http://www.w3.org/2000/svg";
const historyLimit = 60;
const history = { ingestion: [], label: [], publish: [] };
const requestTimeoutMilliseconds = 2_500;
const staleWarningSeconds = 4;
const staleErrorSeconds = 15;
let metricsSource = null;
let refreshInFlight = false;
let lastSuccessfulAt = null;
let lastConnectionError = null;

const formatRate = (value) => Number(value || 0).toFixed(2);
const formatInteger = (value) => new Intl.NumberFormat().format(Number(value || 0));

function formatAge(seconds) {
  if (seconds == null) return "Queue empty";
  if (seconds < 1) return "Less than 1s old";
  if (seconds < 60) return `${Math.round(seconds)}s oldest`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m oldest`;
  return `${(seconds / 3600).toFixed(1)}h oldest`;
}

function formatLag(seconds) {
  if (seconds == null) return "Unavailable";
  if (seconds < 1) return "<1s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  return `${(seconds / 60).toFixed(1)}m`;
}

function addSample(bucket, value) {
  bucket.push(Number(value || 0));
  if (bucket.length > historyLimit) bucket.shift();
}

function linePath(values, width, height, maximum) {
  if (values.length === 0) return "";
  const denominator = Math.max(1, values.length - 1);
  return values.map((value, index) => {
    const x = (index / denominator) * width;
    const y = height - (value / maximum) * (height - 4) - 2;
    return `${index === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(" ");
}

function renderSparkline(elementId, values, color) {
  const svg = byId(elementId);
  const maximum = Math.max(1, ...values);
  const line = linePath(values, 240, 54, maximum);
  const area = document.createElementNS(svgNamespace, "path");
  const stroke = document.createElementNS(svgNamespace, "path");
  area.setAttribute("class", "spark-area");
  area.setAttribute("fill", color);
  area.setAttribute("d", line ? `${line} L240,54 L0,54 Z` : "");
  stroke.setAttribute("class", "spark-line");
  stroke.setAttribute("stroke", color);
  stroke.setAttribute("d", line);
  svg.replaceChildren(area, stroke);
}

function renderCharts(rates) {
  addSample(history.ingestion, rates.ingestion_per_second);
  addSample(history.label, rates.label_per_second);
  addSample(history.publish, rates.publish_per_second);

  renderSparkline("ingestion-sparkline", history.ingestion, "#1688f8");
  renderSparkline("label-sparkline", history.label, "#6c5fc7");
  renderSparkline("publish-sparkline", history.publish, "#2c9a63");

  const maximum = Math.max(1, ...history.ingestion, ...history.label, ...history.publish);
  byId("chart-ingestion").setAttribute("d", linePath(history.ingestion, 720, 230, maximum));
  byId("chart-label").setAttribute("d", linePath(history.label, 720, 230, maximum));
  byId("chart-publish").setAttribute("d", linePath(history.publish, 720, 230, maximum));
  byId("chart-max").textContent = maximum < 10 ? maximum.toFixed(1) : Math.ceil(maximum).toString();
}

function queueFill(depth) {
  if (depth <= 0) return 0;
  return Math.min(100, Math.max(10, Math.log10(depth + 1) * 38));
}

function renderQueue(prefix, queue) {
  const depth = Number(queue.available || 0) + Number(queue.leased || 0);
  const age = Number(queue.oldest_job_age_seconds || 0);
  const row = byId(`${prefix}-queue`);
  row.classList.toggle("warning", age > 60 && age <= 300);
  row.classList.toggle("danger", age > 300 || Number(queue.dead || 0) > 0);
  byId(`${prefix}-depth`).textContent = formatInteger(depth);
  byId(`${prefix}-age`).textContent = formatAge(queue.oldest_job_age_seconds);
  byId(`${prefix}-fill`).style.width = `${queueFill(depth)}%`;
  return { depth, age };
}

function renderBottleneck(postState, labelState) {
  const badge = byId("bottleneck");
  const maximumAge = Math.max(postState.age, labelState.age);
  badge.className = "status-badge";
  if (postState.depth === 0 && labelState.depth === 0) {
    badge.textContent = "No backlog";
  } else if (maximumAge > 300) {
    badge.classList.add("danger");
    badge.textContent = postState.age >= labelState.age ? "Detection stalled" : "Publishing stalled";
  } else if (maximumAge > 60) {
    badge.classList.add("warning");
    badge.textContent = postState.age >= labelState.age ? "Detection backlog" : "Publishing backlog";
  } else {
    badge.textContent = "Within target";
  }
}

function renderHealth(stream, generated, queues) {
  const ageSeconds = Math.max(0, (Date.now() - generated.getTime()) / 1000);
  const sourceLag = stream.source_lag_seconds == null
    ? null
    : Number(stream.source_lag_seconds) + ageSeconds;
  const lastEventAge = stream.last_event_age_seconds == null
    ? null
    : Number(stream.last_event_age_seconds) + ageSeconds;
  const threshold = Number(stream.caught_up_lag_seconds || 10);
  const caughtUp = Boolean(stream.connected) && sourceLag != null && sourceLag <= threshold;
  const sourceStalled = Boolean(stream.connected)
    && lastEventAge != null
    && lastEventAge > 30;
  const maximumQueueAge = Math.max(
    Number(queues.posts.oldest_job_age_seconds || 0),
    Number(queues.labels.oldest_job_age_seconds || 0),
  );
  const staleAfterSeconds = 120;
  const ring = byId("health-ring");
  const statusDot = byId("status-dot");
  ring.className = "health-ring";
  statusDot.className = "status-dot";

  if (!stream.connected || ageSeconds > staleAfterSeconds) {
    ring.classList.add("error");
    statusDot.classList.add("stale");
    byId("health-ring-label").textContent = "Offline";
    byId("health-title").textContent = "Telemetry disconnected";
    byId("health-description").textContent = "The dashboard is waiting for a current reader and metrics snapshot.";
    byId("freshness").textContent = `Disconnected · snapshot ${generated.toLocaleTimeString()}`;
  } else if (sourceStalled) {
    ring.classList.add("error");
    statusDot.classList.add("stale");
    byId("health-ring-label").textContent = "Stalled";
    byId("health-title").textContent = "Reader stream is not advancing";
    byId("health-description").textContent = "The gateway is responsive, but Jetstream has delivered no recent events. The reader will reconnect automatically.";
    byId("freshness").textContent = `No source events for ${formatLag(lastEventAge)}`;
  } else if (!caughtUp || maximumQueueAge > 60) {
    ring.classList.add("warning");
    statusDot.classList.add("warning");
    byId("health-ring-label").textContent = "Syncing";
    byId("health-title").textContent = maximumQueueAge > 60 ? "Backlog needs attention" : "Pipeline is catching up";
    byId("health-description").textContent = maximumQueueAge > 60
      ? "Work is flowing, but the oldest queued item is outside the 60-second target."
      : "The reader is connected and processing older source events.";
    byId("freshness").textContent = `Catching up · ${formatLag(sourceLag)} behind`;
  } else {
    ring.classList.add("healthy");
    statusDot.classList.add("live");
    byId("health-ring-label").textContent = "Healthy";
    byId("health-title").textContent = "All systems operational";
    byId("health-description").textContent = "The reader is current and both work queues are within their processing target.";
    byId("freshness").textContent = `Up to date · ${formatLag(sourceLag)} behind`;
  }

  byId("source-lag").textContent = sourceLag == null ? "Unavailable" : `${formatLag(sourceLag)} behind`;
  byId("events-received").textContent = formatInteger(stream.received_total);
  byId("reconnects").textContent = formatInteger(stream.reconnect_total);
  byId("footer-updated").textContent = `Snapshot updated ${generated.toLocaleTimeString()}`;
  byId("freshness").title = `Latest snapshot ${generated.toLocaleString()}`;
}

const totalLabels = {
  ingested: "Ingested",
  detected: "Detected",
  labels_enqueued: "Labels queued",
  published: "Published",
  detection_retries: "Detection retries",
  publication_retries: "Publish retries",
  dead_lettered: "Dead lettered",
};

function renderTotals(values) {
  const totals = byId("totals");
  totals.replaceChildren();
  for (const [name, value] of Object.entries(values)) {
    const item = document.createElement("div");
    const term = document.createElement("dt");
    const detail = document.createElement("dd");
    term.textContent = totalLabels[name] || name.replaceAll("_", " ");
    detail.textContent = formatInteger(value);
    item.append(term, detail);
    totals.append(item);
  }
}

function renderConfiguration(configuration) {
  const tags = byId("tags");
  tags.replaceChildren();
  for (const value of configuration.tags) {
    const tag = document.createElement("span");
    tag.className = "tag";
    tag.textContent = value;
    tags.append(tag);
  }
  if (configuration.tags.length === 0) tags.textContent = "No tags configured";
  byId("tag-count").textContent = formatInteger(configuration.tags.length);
  byId("label-value").textContent = configuration.label_value;
  byId("detector-version").textContent = configuration.detector_version;
}

function render(data) {
  lastSuccessfulAt = Date.now();
  lastConnectionError = null;
  const rates = data.rates || {};
  byId("ingestion-rate").textContent = formatRate(rates.ingestion_per_second);
  byId("label-rate").textContent = formatRate(rates.label_per_second);
  byId("publish-rate").textContent = formatRate(rates.publish_per_second);
  byId("window").textContent = `${data.window_seconds}s rolling window`;
  renderCharts(rates);

  const postState = renderQueue("post", data.queues.posts);
  const labelState = renderQueue("label", data.queues.labels);
  renderBottleneck(postState, labelState);
  byId("total-depth").textContent = formatInteger(postState.depth + labelState.depth);

  renderConfiguration(data.configuration);
  renderTotals(data.totals);
  renderHealth(data.stream || {}, new Date(data.generated_at), data.queues);
  byId("error").hidden = true;
}

async function fetchWithTimeout(url) {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), requestTimeoutMilliseconds);
  try {
    return await fetch(url, { cache: "no-store", signal: controller.signal });
  } finally {
    window.clearTimeout(timeout);
  }
}

function updateWatchdog(force = false) {
  if (lastSuccessfulAt === null) {
    byId("status-dot").className = "status-dot stale";
    byId("health-ring").className = "health-ring error";
    byId("health-ring-label").textContent = "Offline";
    byId("health-title").textContent = "Waiting for live telemetry";
    byId("health-description").textContent = "Start the coordinator to connect this dashboard to the metrics gateway.";
    byId("freshness").textContent = "Connecting to live metrics";
    byId("footer-updated").textContent = "No live snapshot received yet";
    return;
  }

  const staleSeconds = (Date.now() - lastSuccessfulAt) / 1000;
  if (!force && staleSeconds < staleWarningSeconds) return;
  const isError = staleSeconds >= staleErrorSeconds;
  byId("status-dot").className = `status-dot ${isError ? "stale" : "warning"}`;
  byId("health-ring").className = `health-ring ${isError ? "error" : "warning"}`;
  byId("health-ring-label").textContent = isError ? "Stale" : "Delayed";
  byId("health-title").textContent = isError ? "Telemetry is stale" : "Telemetry update delayed";
  byId("health-description").textContent = "The last good values remain visible while the dashboard reconnects automatically.";
  byId("freshness").textContent = `No update for ${Math.max(1, Math.floor(staleSeconds))}s - retrying`;
  byId("footer-updated").textContent = `Last successful update ${new Date(lastSuccessfulAt).toLocaleTimeString()}`;
  if (lastConnectionError) byId("freshness").title = lastConnectionError;
}

function showConnectionError(message) {
  lastConnectionError = String(message);
  byId("error").textContent = lastConnectionError;
  byId("error").hidden = true;
  updateWatchdog(true);
}

async function refresh() {
  if (refreshInFlight) return;
  refreshInFlight = true;
  try {
    if (metricsSource === null) {
      const registryResponse = await fetchWithTimeout(`${endpointRegistry}?t=${Date.now()}`);
      if (!registryResponse.ok) {
        throw new Error(`endpoint registry returned ${registryResponse.status}`);
      }
      const endpoint = await registryResponse.json();
      if (endpoint.state !== "active" || !endpoint.public_url) {
        throw new Error("the coordinator has not opened the live metrics endpoint yet");
      }
      metricsSource = `${endpoint.public_url.replace(/\/$/, "")}/metrics`;
    }

    const metricsResponse = await fetchWithTimeout(`${metricsSource}?t=${Date.now()}`);
    if (!metricsResponse.ok) {
      throw new Error(`metrics request returned ${metricsResponse.status}`);
    }
    render(await metricsResponse.json());
  } catch (error) {
    metricsSource = null;
    showConnectionError(error);
  } finally {
    refreshInFlight = false;
  }
}

refresh();
window.setInterval(refresh, 1_000);
window.setInterval(updateWatchdog, 1_000);
