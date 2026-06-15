const state = {
  dashboard: null,
  preview: false,
  previewStore: null,
  runtime: null,
};

const PAYMENT_OUTAGE_SECONDS = 5;
const METRICS_REFRESH_MS = 1500;
const RESOURCE_LIMITS = {
  "carrier-api": 2,
  "payment-gateway": 3,
  "warehouse-robots": 4,
};
let actionInFlight = false;
let metricsRefreshTimer = null;

const $ = (id) => document.getElementById(id);

const setActionsBusy = (busy) => {
  document.querySelectorAll("button").forEach((button) => {
    button.disabled = busy;
  });
};

const CATALOG = {
  customers: [
    "Acme Field Services",
    "Northwind Retail",
    "Meridian Health",
    "Signal Labs",
    "Lakefront Supply",
    "Vertex Aerospace",
  ],
  skus: [
    "SKU-ROBOT-ARM",
    "SKU-SENSOR-KIT",
    "SKU-COLD-PACK",
    "SKU-DRONE-BAY",
    "SKU-LEDGER-PRO",
    "SKU-SAFETY-RFID",
  ],
  warehouses: ["PHX-1", "DFW-2", "ABE-3", "RNO-4"],
  carriers: ["UPS", "FedEx", "DHL", "Regional Courier"],
};

const createPreviewStore = () => {
  const orders = [
    orderFixture(221, "RECEIPT_SENT", "Receipt sent and order complete", true),
    orderFixture(222, "REVIEW_REQUIRED", "Waiting for fraud review", false, 84),
    orderFixture(223, "INVENTORY_RESERVED", "Gateway timeout, retrying payment", false, 44),
    orderFixture(224, "FAILED", "Bad card moved to DLQ", false, 78),
    orderFixture(225, "SHIPMENT_CREATED", "Shipment booked with DHL", true, 18),
  ];
  return {
    nextSequence: 226,
    runtime: {
      activeResources: ["warehouse-robots", "payment-gateway", "carrier-api"],
      appVersion: "local",
      dbProfile: "fixture",
      nodeId: "browser-preview",
      paymentOutageUntil: null,
      serverProfile: "preview",
    },
    dashboard: {
      burst: { completed: 27, failed: 2, pending: 11, running: 4, total: 44 },
      orders,
      queueHealth: {
        canceled: 0,
        failed: 3,
        oldestPendingJobTime: new Date(Date.now() - 170000).toISOString(),
        p95QueueWaitMs: 740,
        paused: 0,
        pending: 18,
        ready: 9,
        retryRate: 0.13,
        running: 7,
        succeeded: 138,
        waiting: 5,
      },
      recentJobs: [
        jobFixture("019e83f8-5e92-7df1-b4b3-d67f482b7a91", "WAITING", "SINGLE", "HIGH", "applyReviewDecision", "carrier-api", 1),
        jobFixture("019e83f8-52c7-7a55-9643-13a4d801a882", "RUNNING", "CHAIN", "NORMAL", "chargePayment", "payment-gateway", 2),
        jobFixture("019e83f8-471c-760d-8c5a-d96d7ad6e615", "PENDING", "BATCH_CHILD", "NORMAL", "startImportedOrder", "", 0),
        jobFixture("019e83f8-3304-74da-a26b-0f9f3923a882", "FAILED", "SINGLE", "NORMAL", "chargePayment", "payment-gateway", 3),
        jobFixture("019e83f8-2092-78c6-9d7c-bb01a5ac6290", "SUCCEEDED", "SINGLE", "HIGH", "sendReceipt", "", 1),
      ],
      reviews: [
        {
          customer: orders[1].customer,
          fraudScore: 84,
          id: "review-ORD-000222",
          orderId: "ORD-000222",
          vip: false,
        },
      ],
      statusCounts: {},
      stream: {
        burstiness: 0.25,
        failureMix: 0.35,
        lastTickAt: new Date().toISOString(),
        ordersPerMinute: 30,
        produced: 225,
        running: true,
        seed: 8675309,
        startedAt: new Date(Date.now() - 8 * 60000).toISOString(),
        tokenRemainder: 0.4,
        updatedAt: new Date().toISOString(),
      },
    },
  };
};

const orderFixture = (sequence, status, message, vip = false, fraudScore = 36) => {
  const index = sequence % CATALOG.customers.length;
  return {
    addressBad: status === "FAILED",
    carrier: CATALOG.carriers[sequence % CATALOG.carriers.length],
    createdAt: new Date(Date.now() - (230 - sequence) * 12000).toISOString(),
    customer: CATALOG.customers[index],
    fraudScore,
    inventoryPressure: status === "INVENTORY_RESERVED",
    jobId: `019e83f8-${String(sequence).padStart(4, "0")}-7000-9000-000000000000`,
    message,
    orderId: `ORD-${String(sequence).padStart(6, "0")}`,
    paymentProfile: status === "FAILED" ? "BAD_CARD" : "NORMAL",
    quantity: 1 + (sequence % 7),
    sequence,
    sku: CATALOG.skus[sequence % CATALOG.skus.length],
    status,
    vip,
    warehouse: CATALOG.warehouses[sequence % CATALOG.warehouses.length],
  };
};

const jobFixture = (id, status, type, priority, method, resource, attempts) => ({
  attempts,
  businessKey: `showcase-${id.slice(0, 8)}`,
  createdAt: new Date(Date.now() - 90000).toISOString(),
  dependsOn: type === "CHAIN" ? "019e83f8-1000-7000-9000-000000000000" : null,
  error: status === "FAILED" ? "Permanent payment failure" : "",
  id,
  method,
  maxRetries: 4,
  pickedBy: status === "RUNNING" ? "preview-node-1" : "",
  priority,
  resource,
  scheduledTime: new Date().toISOString(),
  status,
  tags: resource ? ["showcase", "order-fulfillment", resource] : ["showcase", "order-fulfillment"],
  target: "run.ratchet.showcase.jobs.ShowcaseJobs",
  type,
  updatedAt: new Date().toISOString(),
});

const ensurePreview = () => {
  if (!state.previewStore) {
    state.previewStore = createPreviewStore();
  }
  state.preview = true;
  return state.previewStore;
};

const api = async (path, body) => {
  const options =
    body === undefined
      ? {}
      : {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body ?? {}),
        };
  if (state.preview) {
    return previewResponse(path, body);
  }
  try {
    const response = await fetch(path, options);
    if (!response.ok) {
      if (response.status === 404 && previewAllowed()) {
        return previewResponse(path, body);
      }
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return response.json();
  } catch (error) {
    if (previewAllowed()) {
      return previewResponse(path, body);
    }
    throw error;
  }
};

const previewAllowed = () =>
  location.protocol === "file:" || location.port === "4173" || new URLSearchParams(location.search).has("preview");

const post = (path, body) => api(path, body);

const previewResponse = (path, body = {}) => {
  const store = ensurePreview();
  if (path.endsWith("api/runtime")) {
    return { ...store.runtime };
  }
  if (path.endsWith("api/dashboard")) {
    advancePreview(store);
    return previewDashboard(store);
  }
  if (path.endsWith("api/stream/start")) {
    updateStream(store, { ...controlsPayload(), ...body, running: true });
    return store.dashboard.stream;
  }
  if (path.endsWith("api/stream/update")) {
    updateStream(store, body);
    return store.dashboard.stream;
  }
  if (path.endsWith("api/stream/stop")) {
    store.dashboard.stream.running = false;
    store.dashboard.stream.updatedAt = new Date().toISOString();
    return store.dashboard.stream;
  }
  if (path.endsWith("api/reset")) {
    state.previewStore = createPreviewStore();
    return { cancelledJobs: 12, cancelledRecurring: 1 };
  }
  if (path.endsWith("api/scenarios/import-burst")) {
    importPreviewBurst(store, body?.count ?? 40);
    return { batchJobId: "preview-batch" };
  }
  if (path.endsWith("api/scenarios/payment-outage")) {
    store.runtime.paymentOutageUntil = new Date(Date.now() + (body?.seconds ?? PAYMENT_OUTAGE_SECONDS) * 1000).toISOString();
    store.dashboard.queueHealth.retryRate = 0.28;
    store.dashboard.queueHealth.pending += 7;
    addPreviewScenarioOrders(store, body?.count ?? 8, {
      carrier: "DHL",
      customer: "Signal Labs",
      jobStatus: "SCHEDULED",
      message: "Payment outage, backing off",
      method: "chargePayment",
      paymentProfile: "NORMAL",
      resource: "payment-gateway",
      status: "INVENTORY_RESERVED",
    });
    return { paymentOutageUntil: store.runtime.paymentOutageUntil, submitted: body?.count ?? 8 };
  }
  if (path.endsWith("api/scenarios/fraud-review")) {
    const order = addPreviewScenarioOrder(store, {
      fraudScore: 92,
      jobStatus: "WAITING",
      message: "Waiting for fraud review",
      method: "applyReviewDecision",
      priority: "CRITICAL",
      status: "REVIEW_REQUIRED",
      vip: true,
    });
    store.dashboard.reviews = [
      {
        customer: order.customer,
        fraudScore: order.fraudScore,
        id: `review-${order.orderId}`,
        orderId: order.orderId,
        vip: order.vip,
      },
      ...store.dashboard.reviews,
    ].slice(0, 6);
    return { scenario: "fraud-review", submitted: 1 };
  }
  if (path.endsWith("api/scenarios/bad-card")) {
    addPreviewScenarioOrder(store, {
      jobStatus: "FAILED",
      message: "Bad card moved to DLQ",
      method: "chargePayment",
      paymentProfile: "BAD_CARD",
      resource: "payment-gateway",
      status: "FAILED",
    });
    store.dashboard.queueHealth.failed += 1;
    return { scenario: "bad-card", submitted: 1 };
  }
  if (path.endsWith("api/scenarios/warehouse-crunch")) {
    addPreviewScenarioOrders(store, body?.count ?? 16, {
      inventoryPressure: true,
      jobStatus: "SCHEDULED",
      message: "Inventory pressure, retrying pick",
      method: "reserveInventory",
      resource: "warehouse-robots",
      status: "FRAUD_SCORED",
    });
    store.dashboard.queueHealth.pending += body?.count ?? 16;
    return { scenario: "warehouse-crunch", submitted: body?.count ?? 16 };
  }
  if (path.endsWith("api/scenarios/carrier-outage")) {
    addPreviewScenarioOrders(store, body?.count ?? 8, {
      carrier: "Regional Courier",
      jobStatus: "SCHEDULED",
      message: "Carrier capacity retry",
      method: "createShipment",
      resource: "carrier-api",
      status: "PAYMENT_CHARGED",
    });
    return { scenario: "carrier-outage", submitted: body?.count ?? 8 };
  }
  if (path.includes("api/reviews/") && path.endsWith("/decision")) {
    decidePreviewReview(store, path, body);
    return { delivered: 1 };
  }
  if (path.includes("api/jobs/") && path.endsWith("/retry")) {
    retryPreviewJob(store, path);
    return { retried: true };
  }
  return {};
};

const updateStream = (store, update) => {
  const stream = store.dashboard.stream;
  if (update.seed !== undefined) {
    stream.seed = Number(update.seed);
  }
  if (update.ordersPerMinute !== undefined) {
    stream.ordersPerMinute = Number(update.ordersPerMinute);
  }
  if (update.burstiness !== undefined) {
    stream.burstiness = Number(update.burstiness);
  }
  if (update.failureMix !== undefined) {
    stream.failureMix = Number(update.failureMix);
  }
  if (update.running !== undefined) {
    stream.running = Boolean(update.running);
  }
  stream.updatedAt = new Date().toISOString();
};

const addPreviewScenarioOrders = (store, count, options) => {
  const size = Math.max(1, Math.min(50, Number(count || 1)));
  for (let i = 0; i < size; i++) {
    addPreviewScenarioOrder(store, options);
  }
};

const addPreviewScenarioOrder = (store, options) => {
  const sequence = store.nextSequence++;
  const order = orderFixture(
    sequence,
    options.status,
    options.message,
    options.vip ?? sequence % 5 === 0,
    options.fraudScore ?? 34 + (sequence % 18)
  );
  order.carrier = options.carrier ?? order.carrier;
  order.customer = options.customer ?? order.customer;
  order.inventoryPressure = options.inventoryPressure ?? order.inventoryPressure;
  order.paymentProfile = options.paymentProfile ?? order.paymentProfile;
  store.dashboard.orders = [order, ...store.dashboard.orders].slice(0, 25);
  store.dashboard.recentJobs = [
    jobFixture(
      order.jobId,
      options.jobStatus ?? "PENDING",
      options.type ?? "CHAIN",
      options.priority ?? (order.vip ? "HIGH" : "NORMAL"),
      options.method ?? "validateOrder",
      options.resource ?? "warehouse-robots",
      options.attempts ?? 1
    ),
    ...store.dashboard.recentJobs,
  ].slice(0, 30);
  return order;
};

const advancePreview = (store) => {
  const stream = store.dashboard.stream;
  if (!stream.running) {
    return;
  }
  const shouldAdd = Math.max(1, Math.min(4, Math.round(stream.ordersPerMinute / 60)));
  for (let i = 0; i < shouldAdd; i++) {
    const sequence = store.nextSequence++;
    const status = sequence % 11 === 0 ? "REVIEW_REQUIRED" : sequence % 7 === 0 ? "INVENTORY_RESERVED" : "VALIDATED";
    const fraudScore = status === "REVIEW_REQUIRED" ? 76 + (sequence % 18) : 18 + (sequence % 42);
    const order = orderFixture(sequence, status, statusMessage(status), sequence % 9 === 0, fraudScore);
    store.dashboard.orders = [order, ...store.dashboard.orders].slice(0, 25);
    if (status === "REVIEW_REQUIRED") {
      store.dashboard.reviews = [
        {
          customer: order.customer,
          fraudScore: order.fraudScore,
          id: `review-${order.orderId}`,
          orderId: order.orderId,
          vip: order.vip,
        },
        ...store.dashboard.reviews,
      ].slice(0, 6);
    }
    store.dashboard.recentJobs = [
      jobFixture(order.jobId, status === "REVIEW_REQUIRED" ? "WAITING" : "PENDING", "CHAIN", order.vip ? "HIGH" : "NORMAL", "validateOrder", "warehouse-robots", 0),
      ...store.dashboard.recentJobs,
    ].slice(0, 30);
    stream.produced += 1;
  }
  stream.lastTickAt = new Date().toISOString();
};

const importPreviewBurst = (store, count) => {
  const size = Math.max(1, Math.min(80, Number(count || 40)));
  store.dashboard.burst = { completed: 0, failed: 0, pending: size - 5, running: 5, total: size };
  store.dashboard.queueHealth.pending += size;
  store.dashboard.recentJobs = [
    {
      ...jobFixture("preview-batch-job", "RUNNING", "BATCH", "NORMAL", "startImportedOrder", "", 1),
      businessKey: "showcase-import-burst",
      tags: ["showcase", "batch", "import"],
    },
    ...store.dashboard.recentJobs,
  ].slice(0, 30);
};

const decidePreviewReview = (store, path, body) => {
  const reviewId = decodeURIComponent(path.split("api/reviews/")[1].split("/decision")[0]);
  const decision = body?.decision === "reject" ? "REVIEW_REJECTED" : "REVIEW_APPROVED";
  const review = store.dashboard.reviews.find((item) => item.id === reviewId);
  store.dashboard.reviews = store.dashboard.reviews.filter((item) => item.id !== reviewId);
  if (review) {
    const order = store.dashboard.orders.find((item) => item.orderId === review.orderId);
    if (order) {
      order.status = decision;
      order.message = decision === "REVIEW_APPROVED" ? "Fraud review approved" : "Dashboard decision rejected";
    }
  }
};

const retryPreviewJob = (store, path) => {
  const jobId = decodeURIComponent(path.split("api/jobs/")[1].split("/retry")[0]);
  const job = store.dashboard.recentJobs.find((item) => item.id === jobId);
  if (job) {
    job.status = "PENDING";
    job.error = "";
    job.attempts += 1;
  }
};

const previewDashboard = (store) => {
  store.dashboard.statusCounts = statusCountsFromOrders(store.dashboard.orders);
  return JSON.parse(JSON.stringify(store.dashboard));
};

const statusCountsFromOrders = (orders) =>
  orders.reduce((counts, order) => {
    counts[order.status] = (counts[order.status] || 0) + 1;
    return counts;
  }, {});

const statusMessage = (status) => {
  switch (status) {
    case "REVIEW_REQUIRED":
      return "Waiting for fraud review";
    case "INVENTORY_RESERVED":
      return "Inventory reserved in constrained warehouse";
    default:
      return "Customer and address validated";
  }
};

const statusClass = (status) => {
  if (["RECEIPT_SENT", "SUCCEEDED", "REVIEW_APPROVED", "PAYMENT_CHARGED"].includes(status)) {
    return "good";
  }
  if (["FAILED", "REVIEW_REJECTED", "CANCELED"].includes(status)) {
    return "bad";
  }
  if (["WAITING", "REVIEW_REQUIRED", "PENDING", "READY", "SCHEDULED", "RUNNING", "INVENTORY_RESERVED"].includes(status)) {
    return "warn";
  }
  return "";
};

const pill = (text) => `<span class="pill ${statusClass(text)}">${text}</span>`;

const updateRuntime = async () => {
  state.runtime = await api("api/runtime");
  $("runtime").innerHTML = [
    `${state.runtime.serverProfile}/${state.runtime.dbProfile}`,
    `node ${state.runtime.nodeId}`,
    `version ${state.runtime.appVersion}`,
    state.preview ? "preview data" : null,
  ]
    .filter(Boolean)
    .map((item) => `<span>${item}</span>`)
    .join("");
};

const updateDashboard = async () => {
  state.dashboard = await api("api/dashboard");
  render();
};

const render = () => {
  const data = state.dashboard;
  if (!data) {
    return;
  }
  const counts = data.statusCounts || {};
  $("kpis").innerHTML = [
    kpi("Stream", data.stream?.running ? "Running" : "Stopped", data.stream?.running ? "good" : ""),
    kpi("Produced", data.stream?.produced ?? 0, "good"),
    kpi("Orders", sumCounts(counts), ""),
    kpi("Reviews", data.reviews?.length ?? 0, data.reviews?.length ? "warn" : ""),
    kpi("Failed", counts.FAILED ?? 0, counts.FAILED ? "bad" : ""),
    kpi("Fulfilled", counts.RECEIPT_SENT ?? 0, "good"),
  ].join("");

  const q = data.queueHealth || {};
  const delayed = Math.max(0, Number(q.pending || 0) - Number(q.ready || 0));
  $("queue").innerHTML = [
    ["Ready", q.ready],
    ["Delayed", delayed],
    ["Running", q.running],
    ["Waiting", q.waiting],
    ["Failed", q.failed],
    ["Retry Rate", `${Math.round((q.retryRate || 0) * 100)}%`],
  ]
    .map(([label, value]) => `<div class="metric"><span>${label}</span><strong>${value ?? 0}</strong></div>`)
    .join("");

  $("activity").innerHTML = renderActivity(data);
  $("reviews").innerHTML = renderReviews(data.reviews || []);
  $("orders").innerHTML = (data.orders || []).map(renderOrder).join("");
  $("jobs").innerHTML = (data.recentJobs || []).map(renderJob).join("");
};

const kpi = (label, value, tone) => `<div class="kpi ${tone}"><span>${label}</span><strong>${value}</strong></div>`;

const sumCounts = (counts) => Object.values(counts).reduce((sum, value) => sum + Number(value || 0), 0);

const renderActivity = (data) => {
  const jobs = data.recentJobs || [];
  const orders = data.orders || [];
  const q = data.queueHealth || {};
  const batch = data.burst || {};
  const reviews = data.reviews || [];
  const paymentRunning = runningForResource(jobs, "payment-gateway");
  const warehouseRunning = runningForResource(jobs, "warehouse-robots");
  const activeBusinessKeys = new Set([
    ...jobs.map((job) => job.businessKey).filter(Boolean),
    ...orders.map((order) => order.orderId).filter(Boolean),
  ]).size;
  const retrying = jobs.filter((job) => Number(job.attempts || 0) > 1 || jobDisplayStatus(job) === "SCHEDULED").length;
  const chainJobs = Math.max(countByType(jobs, "CHAIN"), orders.length);
  const batchTotal = Number(batch.total ?? batch.totalItems ?? 0);
  const batchCompleted = Number(batch.completed ?? batch.completedItems ?? 0);
  const batchFailed = Number(batch.failed ?? batch.failedItems ?? 0);
  const batchRunning = Number(batch.running ?? (batchTotal && !batch.complete ? 1 : 0));
  const batchPending = Number(batch.pending ?? Math.max(0, batchTotal - batchCompleted - batchFailed - batchRunning));
  const batchProgress = batchTotal ? `${batchCompleted}/${batchTotal}` : "Idle";
  const batchDetail = batchTotal
    ? `${Math.round(Number(batch.percentDone ?? (batchCompleted / batchTotal) * 100))}% done, ${batchFailed} failed`
    : `${batchRunning} running, ${batchPending} pending`;

  return [
    activityItem("Recurring", data.stream?.running ? "Active" : "Stopped", `${data.stream?.ordersPerMinute ?? 0}/min producer`),
    activityItem("Chains", chainJobs, "validate -> fraud -> inventory -> payment -> shipment"),
    activityItem("Signals", reviews.length, "fraud reviews waiting for dashboard decisions"),
    activityItem("Retries", retrying, `${Math.round((q.retryRate || 0) * 100)}% recent retry rate`),
    activityItem(
      "Resources",
      `${paymentRunning}/${RESOURCE_LIMITS["payment-gateway"]} pay`,
      `${warehouseRunning}/${RESOURCE_LIMITS["warehouse-robots"]} warehouse slots`
    ),
    activityItem("Batches", batchProgress, batchDetail),
    activityItem("Business Keys", activeBusinessKeys, "duplicate order workflows are coalesced"),
    activityItem("Metrics", `${q.ready ?? 0} ready`, `${q.running ?? 0} running, ${q.waiting ?? 0} waiting`),
  ].join("");
};

const activityItem = (label, value, detail) => `
  <div class="activityItem">
    <span>${label}</span>
    <strong>${value}</strong>
    <small>${detail}</small>
  </div>
`;

const countByType = (jobs, type) => jobs.filter((job) => job.type === type).length;

const runningForResource = (jobs, resource) =>
  jobs.filter((job) => job.resource === resource && job.status === "RUNNING").length;

const renderReviews = (reviews) => {
  if (!reviews.length) {
    return `<div class="empty">No open reviews</div>`;
  }
  return reviews
    .map(
      (review) => `
    <div class="review">
      <div class="reviewTop">
        <strong>${review.orderId}</strong>
        ${pill(`score ${review.fraudScore}`)}
      </div>
      <div class="muted">${review.customer}${review.vip ? " - VIP" : ""}</div>
      <div class="reviewActions">
        <button data-review="${review.id}" data-decision="approve">Approve</button>
        <button data-review="${review.id}" data-decision="reject" class="danger">Reject</button>
      </div>
    </div>
  `
    )
    .join("");
};

const renderOrder = (order) => `
  <tr>
    <td><strong>${order.orderId}</strong></td>
    <td>${order.customer}</td>
    <td>${order.sku}<br><span class="muted">${order.warehouse} - ${order.carrier}</span></td>
    <td>${order.vip ? pill("VIP") : "Standard"}</td>
    <td>${pill(order.status)}</td>
    <td>${order.message || ""}</td>
  </tr>
`;

const renderJob = (job) => {
  const status = jobDisplayStatus(job);
  return `
    <tr>
      <td><span title="${job.id}">${job.id.slice(0, 8)}</span></td>
      <td><span title="${job.status}">${pill(status)}</span></td>
      <td>${job.type}</td>
      <td>${job.priority}</td>
      <td>${job.method || ""}<br><span class="muted">${job.resource || ""}</span></td>
      <td>${job.attempts}</td>
      <td>
        <div class="jobActions">
          <button data-detail-job="${job.id}">Details</button>
          ${job.status === "FAILED" ? `<button data-job="${job.id}">Retry</button>` : ""}
        </div>
      </td>
    </tr>
  `;
};

const jobDisplayStatus = (job) => {
  if (job.status !== "PENDING" || !job.scheduledTime) {
    return job.status;
  }
  return Date.parse(job.scheduledTime) <= Date.now() ? "READY" : "SCHEDULED";
};

const showJobDetail = (jobId) => {
  const job = (state.dashboard?.recentJobs || []).find((item) => item.id === jobId);
  if (!job) {
    return;
  }
  const rows = [
    ["Job ID", job.id],
    ["Status", jobDisplayStatus(job)],
    ["Type", job.type],
    ["Priority", job.priority],
    ["Target", job.target],
    ["Method", job.method],
    ["Business Key", job.businessKey || "none"],
    ["Resource", job.resource || "none"],
    ["Attempts", `${job.attempts ?? 0}/${job.maxRetries ?? 0}`],
    ["Tags", (job.tags || []).join(", ") || "none"],
    ["Depends On", job.dependsOn || "none"],
    ["Picked By", job.pickedBy || "none"],
    ["Created", formatInstant(job.createdAt)],
    ["Scheduled", formatInstant(job.scheduledTime)],
    ["Updated", formatInstant(job.updatedAt)],
    ["Error", job.error || "none"],
  ];
  $("jobDetailBody").innerHTML = rows
    .map(
      ([label, value]) => `
        <div>
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </div>
      `
    )
    .join("");

  const dialog = $("jobDetail");
  if (typeof dialog.showModal === "function") {
    dialog.showModal();
  } else {
    dialog.setAttribute("open", "open");
  }
};

const showMetricsDetail = async () => {
  await refreshMetricsDetail();
  const dialog = $("metricsDetail");
  if (typeof dialog.showModal === "function") {
    dialog.showModal();
  } else {
    dialog.setAttribute("open", "open");
  }
  startMetricsRefresh();
};

const fetchMetricsText = async () => {
  const response = await fetch("metrics", { headers: { Accept: "text/plain" } });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.text();
};

const refreshMetricsDetail = async () => {
  const metrics = state.preview ? previewMetricsText() : await fetchMetricsText();
  const raw = $("metricsRaw");
  const rawScrollTop = raw.scrollTop;
  const rawScrollLeft = raw.scrollLeft;
  $("metricsSummary").innerHTML = renderMetricsSummary(metrics);
  raw.textContent = metrics;
  raw.scrollTop = rawScrollTop;
  raw.scrollLeft = rawScrollLeft;
};

const startMetricsRefresh = () => {
  if (metricsRefreshTimer) {
    return;
  }
  metricsRefreshTimer = setInterval(() => {
    const dialog = $("metricsDetail");
    if (!dialog.open) {
      stopMetricsRefresh();
      return;
    }
    refreshMetricsDetail().catch(console.error);
  }, METRICS_REFRESH_MS);
};

const stopMetricsRefresh = () => {
  if (!metricsRefreshTimer) {
    return;
  }
  clearInterval(metricsRefreshTimer);
  metricsRefreshTimer = null;
};

const renderMetricsSummary = (metrics) =>
  [
    ["Orders", metricValue(metrics, "ratchet_showcase_orders")],
    ["Open Reviews", metricValue(metrics, "ratchet_showcase_reviews_open")],
    ["Completed Jobs", metricSum(metrics, "ratchet_jobs_completed_total")],
    ["Failed Jobs", metricSum(metrics, "ratchet_jobs_failed_total")],
  ]
    .map(
      ([label, value]) => `
        <div>
          <span>${label}</span>
          <strong>${formatMetricNumber(value)}</strong>
        </div>
      `
    )
    .join("");

const metricValue = (metrics, name) => {
  return metricSamples(metrics, name)[0] ?? 0;
};

const metricSum = (metrics, name) => {
  return metricSamples(metrics, name).reduce((sum, value) => sum + value, 0);
};

const metricSamples = (metrics, name) =>
  metrics
    .split("\n")
    .filter((line) => line.startsWith(`${name} `) || line.startsWith(`${name}{`))
    .map((line) => Number(line.trim().split(/\s+/).at(-1)))
    .filter(Number.isFinite);

const formatMetricNumber = (value) => {
  if (!Number.isFinite(value)) {
    return "0";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
};

const previewMetricsText = () => {
  const data = state.dashboard || state.previewStore?.dashboard || {};
  const counts = data.statusCounts || {};
  const completed = Number(data.queueHealth?.succeeded || 0);
  const failed = Number(data.queueHealth?.failed || counts.FAILED || 0);
  return [
    "# HELP ratchet_showcase_orders In-memory showcase order count",
    "# TYPE ratchet_showcase_orders gauge",
    `ratchet_showcase_orders ${sumCounts(counts)}.0`,
    "# HELP ratchet_showcase_reviews_open Open showcase review tickets",
    "# TYPE ratchet_showcase_reviews_open gauge",
    `ratchet_showcase_reviews_open ${Number(data.reviews?.length || 0)}.0`,
    "# HELP ratchet_jobs_completed_total Completed Ratchet jobs",
    "# TYPE ratchet_jobs_completed_total counter",
    `ratchet_jobs_completed_total{type=\"showcase\"} ${completed}.0`,
    "# HELP ratchet_jobs_failed_total Failed Ratchet jobs",
    "# TYPE ratchet_jobs_failed_total counter",
    `ratchet_jobs_failed_total{type=\"showcase\"} ${failed}.0`,
  ].join("\n");
};

const formatInstant = (value) => {
  if (!value) {
    return "none";
  }
  const time = Date.parse(value);
  if (Number.isNaN(time)) {
    return value;
  }
  return new Date(time).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

const controlsPayload = () => ({
  burstiness: Number($("burstiness").value) / 100,
  failureMix: Number($("failureMix").value) / 100,
  ordersPerMinute: Number($("rate").value),
  seed: Number($("seed").value || 8675309),
});

const refreshOutputs = () => {
  $("rateValue").textContent = $("rate").value;
  $("burstValue").textContent = `${$("burstiness").value}%`;
  $("failureValue").textContent = `${$("failureMix").value}%`;
};

document.addEventListener("click", async (event) => {
  const button = event.target.closest("button");
  if (!button || actionInFlight) {
    return;
  }
  if (button.value === "close" && button.closest("dialog")) {
    return;
  }
  const detailJob = button.dataset.detailJob;
  if (detailJob) {
    showJobDetail(detailJob);
    return;
  }
  const review = button.dataset.review;
  const job = button.dataset.job;
  actionInFlight = true;
  setActionsBusy(true);
  try {
    if (button.id === "start") {
      await post("api/stream/start", controlsPayload());
    } else if (button.id === "stop") {
      await post("api/stream/stop", {});
    } else if (button.id === "reset") {
      await post("api/reset", {});
      await updateRuntime();
    } else if (button.id === "update") {
      await post("api/stream/update", controlsPayload());
    } else if (button.id === "newSeed") {
      $("seed").value = String(Math.floor(Date.now() % 2147483647));
      await post("api/stream/update", controlsPayload());
    } else if (button.id === "metricsPreview") {
      await showMetricsDetail();
    } else if (button.id === "fraudReview") {
      await post("api/scenarios/fraud-review", {});
    } else if (button.id === "badCard") {
      await post("api/scenarios/bad-card", {});
    } else if (button.id === "warehouseCrunch") {
      await post("api/scenarios/warehouse-crunch", { count: 16 });
    } else if (button.id === "carrierOutage") {
      await post("api/scenarios/carrier-outage", { count: 8 });
    } else if (button.id === "importBurst") {
      await post("api/scenarios/import-burst", { count: 40, seed: Number($("seed").value) });
    } else if (button.id === "paymentOutage") {
      await post("api/scenarios/payment-outage", { count: 8, seconds: PAYMENT_OUTAGE_SECONDS });
      await updateRuntime();
    } else if (review) {
      await post(`api/reviews/${encodeURIComponent(review)}/decision`, {
        decision: button.dataset.decision,
        reason: "Dashboard decision",
      });
    } else if (job) {
      await post(`api/jobs/${encodeURIComponent(job)}/retry`, {});
    }
    await updateDashboard();
  } catch (error) {
    console.error(error);
  } finally {
    actionInFlight = false;
    setActionsBusy(false);
  }
});

["rate", "burstiness", "failureMix"].forEach((id) => $(id).addEventListener("input", refreshOutputs));
$("metricsDetail").addEventListener("close", stopMetricsRefresh);
$("metricsDetail").addEventListener("cancel", stopMetricsRefresh);

refreshOutputs();
updateRuntime().catch(console.error);
updateDashboard().catch(console.error);
setInterval(updateDashboard, 1500);
