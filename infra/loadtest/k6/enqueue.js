import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';

const targetUrl = trimTrailingSlash(__ENV.TARGET_URL || 'http://localhost:8080');
const rate = intEnv('LOAD_RATE', 100);
const duration = __ENV.LOAD_DURATION || '1m';
const preAllocatedVUs = intEnv('LOAD_PRE_ALLOCATED_VUS', 50);
const maxVUs = intEnv('LOAD_MAX_VUS', 500);
const httpP95Ms = intEnv('LOAD_HTTP_P95_MS', 1000);
const httpErrorRate = floatEnv('LOAD_HTTP_ERROR_RATE', 0.01);
const teardownTimeout = __ENV.LOAD_TEARDOWN_TIMEOUT || '3m';
const minAcceptedNodes = intEnv('MIN_ACCEPT_NODES', 0);
const nodeProbeRequests = intEnv('NODE_PROBE_REQUESTS', 200);
const clusterWaitSeconds = intEnv('CLUSTER_WAIT_SECONDS', 60);
const loadMode = (__ENV.LOAD_MODE || 'spread').toLowerCase();
const spreadMode = loadMode !== 'throughput';
const noConnectionReuse = boolEnv('LOAD_NO_CONNECTION_REUSE', spreadMode);
const enqueueRetries = intEnv('LOAD_ENQUEUE_RETRIES', 3);
const enqueueRetryDelayMs = intEnv('LOAD_ENQUEUE_RETRY_DELAY_MS', 100);
const enqueueRetryBackoff = floatEnv('LOAD_ENQUEUE_RETRY_BACKOFF', 2);

const enqueueRequests = new Counter('ratchet_enqueue_requests');
const enqueueFailures = new Counter('ratchet_enqueue_failures');
const enqueueRetryAttempts = new Counter('ratchet_enqueue_retry_attempts');

export const options = {
  noConnectionReuse,
  setupTimeout: `${Math.max(60, clusterWaitSeconds * 2 + 30)}s`,
  teardownTimeout,
  scenarios: {
    enqueue: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs,
      maxVUs,
    },
  },
  thresholds: {
    http_req_failed: [`rate<${httpErrorRate}`],
    http_req_duration: [`p(95)<${httpP95Ms}`],
    ratchet_enqueue_failures: ['count==0'],
  },
};

export function setup() {
  const runId = __ENV.RUN_ID || `k6-${Date.now()}-${Math.floor(Math.random() * 1000000)}`;
  const cluster = waitForCluster();
  const expectedNodes = minAcceptedNodes > 0 ? minAcceptedNodes : Math.max(1, cluster.activeNodes);
  const probedNodes = waitForGatewayNodes(expectedNodes);

  console.log(`runId=${runId}`);
  console.log(`loadMode=${loadMode}`);
  console.log(`noConnectionReuse=${noConnectionReuse}`);
  console.log(`teardownTimeout=${teardownTimeout}`);
  console.log(`enqueueRetries=${enqueueRetries}`);
  console.log(`enqueueRetryDelayMs=${enqueueRetryDelayMs}`);
  console.log(`enqueueRetryBackoff=${enqueueRetryBackoff}`);
  console.log(`clusterActiveNodes=${cluster.activeNodes}`);
  console.log(`gatewayProbeNodes=${JSON.stringify(probedNodes)}`);

  return { runId, expectedNodes };
}

export default function (data) {
  const sequence = exec.scenario.iterationInTest;
  const payload = JSON.stringify({
    runId: data.runId,
    sequence,
    workload: __ENV.JOB_WORKLOAD || 'noop',
    sleepMs: intEnv('JOB_SLEEP_MS', 0),
    sleepJitterMs: intEnv('JOB_SLEEP_JITTER_MS', 0),
    sleepSpikeRate: floatEnv('JOB_SLEEP_SPIKE_RATE', 0),
    sleepSpikeMs: intEnv('JOB_SLEEP_SPIKE_MS', 0),
    failureRate: floatEnv('JOB_FAILURE_RATE', 0),
    payloadBytes: intEnv('JOB_PAYLOAD_BYTES', 0),
    maxRetries: intEnv('JOB_MAX_RETRIES', 0),
    priority: __ENV.JOB_PRIORITY || 'NORMAL',
    timeoutSeconds: intEnv('JOB_TIMEOUT_SECONDS', 60),
  });

  const response = postJobWithRetry(payload);
  const acceptedNode = responseHeader(response, 'X-Ratchet-Node-Id') || 'missing';
  enqueueRequests.add(1, { accepted_node: acceptedNode });

  const ok = check(response, {
    'enqueue accepted': (r) => r.status === 202,
    'accepted node returned': () => acceptedNode !== 'missing',
  });
  if (!ok) {
    enqueueFailures.add(1);
  }
}

export function teardown(data) {
  const response = getWithRetry(`${targetUrl}/api/runs/${data.runId}`);
  if (response.status !== 200) {
    fail(`run status returned HTTP ${response.status}`);
  }

  const status = response.json();
  const enqueueNodeCounts = status.enqueueNodeCounts || {};
  const enqueueNodeCount = Object.keys(enqueueNodeCounts).length;
  console.log(`runStatus=${JSON.stringify(status)}`);

  if (enqueueNodeCount < data.expectedNodes) {
    fail(
      `jobs were enqueued by ${enqueueNodeCount} node(s), expected at least ${data.expectedNodes}; ` +
        `counts=${JSON.stringify(enqueueNodeCounts)}`
    );
  }
}

function postJobWithRetry(payload) {
  let response = null;
  for (let attempt = 0; attempt <= enqueueRetries; attempt++) {
    response = http.post(`${targetUrl}/api/jobs`, payload, {
      headers: enqueueHeaders(),
    });
    if (!shouldRetry(response)) {
      return response;
    }
    if (attempt < enqueueRetries) {
      enqueueRetryAttempts.add(1, { status: String(response.status || 0) });
      sleep(retryDelaySeconds(attempt));
    }
  }
  return response;
}

function getWithRetry(url) {
  let response = null;
  for (let attempt = 0; attempt <= enqueueRetries; attempt++) {
    response = http.get(url, { headers: controlHeaders() });
    if (!shouldRetry(response)) {
      return response;
    }
    if (attempt < enqueueRetries) {
      sleep(retryDelaySeconds(attempt));
    }
  }
  return response;
}

function shouldRetry(response) {
  return response.status === 0 || response.status === 502 || response.status === 503 || response.status === 504;
}

function retryDelaySeconds(attempt) {
  const multiplier = Math.pow(Math.max(1, enqueueRetryBackoff), attempt);
  return (enqueueRetryDelayMs * multiplier) / 1000;
}

function waitForCluster() {
  const deadline = Date.now() + clusterWaitSeconds * 1000;
  let lastError = '';
  while (Date.now() < deadline) {
    const response = http.get(`${targetUrl}/api/cluster`, { headers: controlHeaders() });
    if (response.status === 200) {
      const cluster = response.json();
      if (cluster.activeNodes >= Math.max(1, minAcceptedNodes)) {
        return cluster;
      }
      lastError = `activeNodes=${cluster.activeNodes}`;
    } else {
      lastError = `HTTP ${response.status}`;
    }
    sleep(1);
  }
  fail(`cluster did not become ready within ${clusterWaitSeconds}s: ${lastError}`);
}

function waitForGatewayNodes(expectedNodes) {
  const deadline = Date.now() + clusterWaitSeconds * 1000;
  let lastProbe = {};
  while (Date.now() < deadline) {
    lastProbe = probeGatewayNodes(nodeProbeRequests);
    if (Object.keys(lastProbe).length >= expectedNodes) {
      return lastProbe;
    }
    sleep(1);
  }
  fail(
    `gateway reached ${Object.keys(lastProbe).length} node(s), expected at least ${expectedNodes}; ` +
      `probe=${JSON.stringify(lastProbe)}`
  );
}

function probeGatewayNodes(requests) {
  const counts = {};
  for (let i = 0; i < requests; i++) {
    const response = http.get(`${targetUrl}/api/node`, { headers: { Connection: 'close' } });
    if (response.status !== 200) {
      continue;
    }
    const node = responseHeader(response, 'X-Ratchet-Node-Id') || response.json('nodeId') || 'missing';
    counts[node] = (counts[node] || 0) + 1;
  }
  return counts;
}

function enqueueHeaders() {
  const headers = {
    'Content-Type': 'application/json',
  };
  if (spreadMode || noConnectionReuse) {
    headers.Connection = 'close';
  }
  return headers;
}

function controlHeaders() {
  if (spreadMode || noConnectionReuse) {
    return { Connection: 'close' };
  }
  return {};
}

function responseHeader(response, name) {
  return response.headers[name] || response.headers[name.toLowerCase()];
}

function intEnv(name, defaultValue) {
  const value = __ENV[name];
  if (value === undefined || value === '') {
    return defaultValue;
  }
  return parseInt(value, 10);
}

function floatEnv(name, defaultValue) {
  const value = __ENV[name];
  if (value === undefined || value === '') {
    return defaultValue;
  }
  return parseFloat(value);
}

function boolEnv(name, defaultValue) {
  const value = __ENV[name];
  if (value === undefined || value === '') {
    return defaultValue;
  }
  return !['0', 'false', 'no', 'off'].includes(value.toLowerCase());
}

function trimTrailingSlash(value) {
  return value.replace(/\/+$/, '');
}
