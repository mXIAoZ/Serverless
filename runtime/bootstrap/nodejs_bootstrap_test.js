const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const bootstrapPath = require.resolve('./nodejs_bootstrap');

function loadBootstrap(functionDir, handler) {
  process.env.FUNCTION_DIR = functionDir;
  process.env.FUNCTION_HANDLER = handler;
  delete require.cache[bootstrapPath];
  return require('./nodejs_bootstrap');
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'node-bootstrap-test-'));
fs.writeFileSync(path.join(tmp, 'handler.js'), 'exports.handler = async function(event, context) { return context.deadlineMs + event.delta; };\n');

let bootstrap = loadBootstrap(tmp, 'handler.handler');
const handler = bootstrap.loadHandler();
assert.strictEqual(typeof handler, 'function');
assert.deepStrictEqual(bootstrap.normalizeResult('ok'), { result: 'ok' });
assert.deepStrictEqual(bootstrap.normalizeResult(42), { result: 42 });
assert.deepStrictEqual(bootstrap.normalizeResult({ statusCode: 200 }), { statusCode: 200 });
assert.deepStrictEqual(bootstrap.errorPayload(new TypeError('bad')), {
  errorType: 'TypeError',
  errorMessage: 'bad',
});

bootstrap = loadBootstrap(tmp, '../escape.handler');
assert.throws(() => bootstrap.loadHandler(), /invalid handler module/);
bootstrap = loadBootstrap(tmp, 'handler.not-valid');
assert.throws(() => bootstrap.loadHandler(), /invalid handler export/);

fs.rmSync(tmp, { recursive: true, force: true });
