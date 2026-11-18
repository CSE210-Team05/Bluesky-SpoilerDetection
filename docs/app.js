const byId = (id) => document.getElementById(id);
const endpointRegistry = "https://raw.githubusercontent.com/CSE210-Team05/Bluesky-SpoilerDetection/metrics-runtime/docs/metrics-endpoint.json";
let metricsSource = null;

const formatRate = (value) => Number(value || 0).toFixed(2);
const formatInteger = (value) => new Intl.NumberFormat().format(Number(value || 0));

function formatAge(seconds) {
  if (seconds == null) return "queue empty";
  if (seconds < 60) return `${Math.round(seconds)}s oldest`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m oldest`;
  return `${(seconds / 3600).toFixed(1)}h oldest`;
}

function formatLag(seconds) {
  if (seconds == null) return "source lag unavailable";
  if (seconds < 1) return "<1s behind";
  if (seconds < 60) return `${Math.round(seconds)}s behind`;
  return `${(seconds / 60).toFixed(1)}m behind`;
}

function renderQueue(prefix, queue) {
  byId(`${prefix}-depth`).textContent = formatInteger(queue.available + queue.leased);
  byId(`${prefix}-age`).textContent = formatAge(queue.oldest_job_age_seconds);
}

function renderBottleneck(queues) {
  const postAge = queues.posts.oldest_job_age_seconds || 0;
  const labelAge = queues.labels.oldest_job_age_seconds || 0;
  const badge = byId("bottleneck");
  badge.classList.toggle("warning", Math.max(postAge, labelAge) > 60);
  if (postAge === 0 && labelAge === 0) badge.textContent = "No backlog";
  else if (postAge >= labelAge) badge.textContent = "Detection backlog";
  else badge.textContent = "Publishing backlog";
}

function render(data) {
  byId("ingestion-rate").textContent = formatRate(data.rates.ingestion_per_second);
  byId("label-rate").textContent = formatRate(data.rates.label_per_second);
  byId("publish-rate").textContent = formatRate(data.rates.publish_per_second);
  byId("window").textContent = `${data.window_seconds}s rolling window`;
  renderQueue("post", data.queues.posts);
  renderQueue("label", data.queues.labels);
  renderBottleneck(data.queues);

  const tags = byId("tags");
  tags.replaceChildren();
  for (const value of data.configuration.tags) {
    const tag = document.createElement("span");
    tag.className = "tag";
    tag.textContent = value;
    tags.append(tag);
  }
  if (data.configuration.tags.length === 0) tags.textContent = "No tags configured";
  byId("label-value").textContent = data.configuration.label_value;
  byId("detector-version").textContent = data.configuration.detector_version;

  const totals = byId("totals");
  totals.replaceChildren();
  for (const [name, value] of Object.entries(data.totals)) {
    const item = document.createElement("div");
    const term = document.createElement("dt");
    const detail = document.createElement("dd");
    term.textContent = name.replaceAll("_", " ");
    detail.textContent = formatInteger(value);
    item.append(term, detail);
    totals.append(item);
  }

  const generated = new Date(data.generated_at);
  const ageSeconds = (Date.now() - generated.getTime()) / 1000;
  const stream = data.stream || {};
  const eventAgeSeconds = stream.last_event_age_seconds == null
    ? null
    : stream.last_event_age_seconds + Math.max(0, ageSeconds);
  const sourceLag = stream.source_lag_seconds == null
    ? null
    : stream.source_lag_seconds + Math.max(0, ageSeconds);
  const caughtUpThreshold = Number(stream.caught_up_lag_seconds || 10);
  const caughtUp = stream.connected
    && sourceLag != null
    && sourceLag <= caughtUpThreshold;
  const snapshotTime = generated.toLocaleTimeString();
  const staleAfterSeconds = Math.max(120, Number(data.window_seconds || 0) + 120);
  if (!stream.connected) {
    byId("freshness").textContent = `Disconnected · snapshot ${snapshotTime}`;
  } else if (caughtUp) {
    byId("freshness").textContent = `Up to date · ${formatLag(sourceLag)}`;
  } else {
    byId("freshness").textContent = `Catching up · ${formatLag(sourceLag)}`;
  }
  byId("freshness").title = `Received ${formatInteger(stream.received_total)} events; ${formatInteger(stream.reconnect_total)} reconnects; latest event ${formatLag(eventAgeSeconds)}; snapshot ${snapshotTime}`;
  byId("status-dot").className = `status-dot ${caughtUp && ageSeconds <= staleAfterSeconds ? "live" : "stale"}`;
  byId("error").hidden = true;
}

async function refresh() {
  if (metricsSource === null) {
    try {
      const response = await fetch(`${endpointRegistry}?t=${Date.now()}`, {
        cache: "no-store",
      });
      if (!response.ok) throw new Error(`endpoint registry returned ${response.status}`);
      const endpoint = await response.json();
      if (endpoint.state !== "active" || !endpoint.public_url) {
        throw new Error("the coordinator has not opened the live metrics endpoint yet");
      }
      metricsSource = `${endpoint.public_url.replace(/\/$/, "")}/metrics`;
    } catch (error) {
      byId("status-dot").className = "status-dot stale";
      byId("freshness").textContent = "Waiting for live metrics endpoint…";
      byId("error").textContent = String(error);
      byId("error").hidden = true;
      return;
    }
  }

  try {
    const response = await fetch(`${metricsSource}?t=${Date.now()}`, {
      cache: "no-store",
    });
    if (!response.ok) throw new Error(`metrics request returned ${response.status}`);
    render(await response.json());
  } catch (error) {
    metricsSource = null;
    byId("status-dot").className = "status-dot stale";
    byId("freshness").textContent = "Live endpoint unavailable; reconnecting…";
    byId("error").textContent = String(error);
    byId("error").hidden = true;
  }
}

refresh();
window.setInterval(refresh, 1_000);
