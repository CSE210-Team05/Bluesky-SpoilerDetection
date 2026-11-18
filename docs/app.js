const byId = (id) => document.getElementById(id);

const formatRate = (value) => Number(value || 0).toFixed(2);
const formatInteger = (value) => new Intl.NumberFormat().format(Number(value || 0));

function formatAge(seconds) {
  if (seconds == null) return "queue empty";
  if (seconds < 60) return `${Math.round(seconds)}s oldest`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m oldest`;
  return `${(seconds / 3600).toFixed(1)}h oldest`;
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
  const eventAge = formatAge(
    stream.last_event_age_seconds == null
      ? null
      : stream.last_event_age_seconds + Math.max(0, ageSeconds),
  );
  const snapshotTime = generated.toLocaleTimeString();
  const staleAfterSeconds = Math.max(120, Number(data.window_seconds || 0) + 120);
  byId("freshness").textContent = stream.connected
    ? `Snapshot ${snapshotTime} · stream connected · ${eventAge}`
    : `Snapshot ${snapshotTime} · stream disconnected`;
  byId("freshness").title = `Received ${formatInteger(stream.received_total)} events; ${formatInteger(stream.reconnect_total)} reconnects`;
  byId("status-dot").className = `status-dot ${stream.connected && ageSeconds <= staleAfterSeconds ? "live" : "stale"}`;
  byId("error").hidden = true;
}

async function refresh() {
  try {
    const source = "metrics.json";
    const response = await fetch(`${source}${source.includes("?") ? "&" : "?"}t=${Date.now()}`, {
      cache: "no-store",
    });
    if (!response.ok) throw new Error(`metrics request returned ${response.status}`);
    render(await response.json());
  } catch (error) {
    byId("status-dot").className = "status-dot stale";
    byId("freshness").textContent = "Snapshot unavailable";
    byId("error").textContent = String(error);
    byId("error").hidden = false;
  }
}

refresh();
window.setInterval(refresh, 5_000);
