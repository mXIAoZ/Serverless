const http = require('http');
const fs = require('fs');
const path = require('path');

const runtimeApi = process.env.RUNTIME_API || 'http://localhost:9000';
const handlerName = process.env.FUNCTION_HANDLER || 'handler.handler';
const functionDir = process.env.FUNCTION_DIR || '/function';

function loadHandler() {
  const lastDot = handlerName.lastIndexOf('.');
  if (lastDot <= 0 || lastDot === handlerName.length - 1) {
    throw new Error(`invalid FUNCTION_HANDLER ${handlerName}`);
  }

  const moduleName = handlerName.slice(0, lastDot);
  const exportName = handlerName.slice(lastDot + 1);
  if (!/^[A-Za-z0-9_-]+$/.test(moduleName)) {
    throw new Error(`invalid handler module ${moduleName}`);
  }
  if (!/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(exportName)) {
    throw new Error(`invalid handler export ${exportName}`);
  }

  const root = fs.realpathSync(functionDir);
  const modulePath = fs.realpathSync(path.join(root, `${moduleName}.js`));
  if (modulePath !== path.join(root, path.basename(modulePath))) {
    throw new Error(`handler module escapes FUNCTION_DIR: ${moduleName}`);
  }
  const mod = require(modulePath);
  const handler = mod[exportName];
  if (typeof handler !== 'function') {
    throw new Error(`handler export ${exportName} is not a function`);
  }
  return handler;
}

function request(method, target, body) {
  const url = new URL(target, runtimeApi);
  const data = body === undefined ? undefined : Buffer.from(JSON.stringify(body));

  return new Promise((resolve, reject) => {
    const req = http.request(
      url,
      {
        method,
        headers: data
          ? { 'Content-Type': 'application/json', 'Content-Length': data.length }
          : undefined,
      },
      (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => resolve({ statusCode: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      }
    );
    req.on('error', reject);
    if (data) {
      req.write(data);
    }
    req.end();
  });
}

async function nextInvocation() {
  const res = await request('GET', '/runtime/invocation/next');
  if (res.statusCode === 204) {
    return null;
  }
  if (res.statusCode !== 200) {
    throw new Error(`next invocation failed with ${res.statusCode}`);
  }
  const deadlineMs = Number(res.headers['lambda-runtime-deadline-ms']);
  if (!Number.isFinite(deadlineMs)) {
    throw new Error('missing or invalid Lambda-Runtime-Deadline-Ms header');
  }
  return {
    requestId: res.headers['lambda-runtime-aws-request-id'],
    deadlineMs,
    event: res.body.length ? JSON.parse(res.body.toString('utf8')) : {},
  };
}

function ensureSuccess(res, action) {
  if (res.statusCode < 200 || res.statusCode >= 300) {
    throw new Error(`${action} failed with ${res.statusCode}: ${res.body.toString('utf8')}`);
  }
}

function normalizeResult(value) {
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    return value;
  }
  return { result: value };
}

function errorPayload(error) {
  return {
    errorType: error && error.name ? error.name : 'Error',
    errorMessage: error && error.message ? error.message : String(error),
  };
}

async function main() {
  const handler = loadHandler();
  for (;;) {
    const invocation = await nextInvocation();
    if (!invocation) {
      continue;
    }

    const context = {
      awsRequestId: invocation.requestId,
      deadlineMs: invocation.deadlineMs,
    };

    try {
      const result = await handler(invocation.event, context);
      const res = await request('POST', `/runtime/invocation/${invocation.requestId}/response`, normalizeResult(result));
      ensureSuccess(res, 'post response');
    } catch (error) {
      const res = await request('POST', `/runtime/invocation/${invocation.requestId}/error`, errorPayload(error));
      ensureSuccess(res, 'post error');
    }
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error('[nodejs-bootstrap]', error);
    process.exit(1);
  });
}

module.exports = {
  errorPayload,
  loadHandler,
  normalizeResult,
  nextInvocation,
};
