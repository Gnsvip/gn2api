// Isolated PostgreSQL/WASM verification; no production connections or app deps.
// GN2API_PGLITE_PATH points to a separately installed @electric-sql/pglite.
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const { PGlite } = require(process.env.GN2API_PGLITE_PATH || '@electric-sql/pglite');

async function main() {
  const db = new PGlite();
  try {
    await db.exec(`
      CREATE TABLE groups(id bigint,platform text);
      CREATE TABLE accounts(id bigint,platform text);
      CREATE TABLE ops_error_logs(id bigint,request_id text,created_at timestamptz,platform text DEFAULT 'openai',
        group_id bigint,account_id bigint,requested_model text,model text DEFAULT 'gpt-test',user_id bigint,
        error_type text DEFAULT 'api_error',error_owner text DEFAULT 'client',status_code integer,
        upstream_status_code integer,error_source text DEFAULT 'client_request',error_message text,
        upstream_error_message text,upstream_error_detail text,error_body text,upstream_errors jsonb DEFAULT '[]',
        is_count_tokens boolean DEFAULT false);
      CREATE TABLE channel_monitor_v2_error_metrics_1m(bucket_start timestamptz,platform text,group_id bigint,
        model text,error_category text,taxonomy_version integer,error_requests bigint,
        PRIMARY KEY(bucket_start,platform,group_id,model,error_category,taxonomy_version));
      CREATE TABLE channel_monitor_v2_error_metrics_rollup(bucket_seconds integer,bucket_start timestamptz,
        platform text,group_id bigint,model text,error_category text,taxonomy_version integer,error_requests bigint,
        PRIMARY KEY(bucket_seconds,bucket_start,platform,group_id,model,error_category,taxonomy_version));
      INSERT INTO ops_error_logs(id,request_id,created_at,group_id,status_code,error_message) VALUES
        (1,'balance','2026-11-05T12:00:10Z',42,403,'Insufficient account balance'),
        (2,'forbidden','2026-11-05T12:00:11Z',42,403,'Permission denied'),
        (3,'rate','2026-11-05T12:00:12Z',42,429,'Too many requests'),
        (4,'quota','2026-11-05T12:00:13Z',42,429,'insufficient_quota'),
        (5,'balance','2026-11-05T12:00:01Z',42,500,'Earlier failed attempt'),
        (6,'partial','2026-11-05T12:00:14Z',43,403,'Insufficient account balance'),
        (7,'correct','2026-11-05T12:00:15Z',44,403,'Insufficient account balance'),
        (8,'auth','2026-11-05T12:00:16Z',46,401,'Insufficient account balance');
    `);
    const tiers = [60,300,3600,43200,86400];
    for (const seconds of tiers) {
      const table = seconds === 60 ? 'channel_monitor_v2_error_metrics_1m' : 'channel_monitor_v2_error_metrics_rollup';
      const extra = seconds === 60 ? '' : `${seconds},`;
      const bucket = seconds === 86400 ? '2026-11-05T00:00:00Z' : '2026-11-05T12:00:00Z';
      for (const [group,category,count] of [
        [42,'upstream_forbidden',2],[42,'rate_or_capacity',2],
        [43,'upstream_forbidden',3],[44,'quota_or_balance',1],
        [45,'upstream_forbidden',5],[46,'authentication',1],
      ]) {
        await db.exec(`INSERT INTO ${table} VALUES (${extra}'${bucket}','openai',${group},'gpt-test','${category}',1,${count});`);
      }
    }
    const generate = (apply, start='2026-11-05T00:00:00Z', end='2026-11-06T00:00:00Z') => {
      const result = spawnSync(process.env.GN2API_PYTHON || 'python', [
        path.join(__dirname,'repair_balance_error_categories.py'),
        '--start',start,'--end',end, ...(apply ? ['--apply'] : []),
      ], {encoding:'utf8'});
      assert.equal(result.status, 0, result.stderr);
      return result.stdout;
    };
    const snapshot = async () => (await db.query(`SELECT * FROM channel_monitor_v2_error_metrics_1m ORDER BY group_id,error_category`)).rows;
    const before = await snapshot();
    const preview = await db.exec(generate(false));
    const plan = preview.find(r => r.rows.some(row => row.action));
    assert.equal(plan.rows.filter(r => r.action === 'repair').length, 5);
    assert.equal(plan.rows.filter(r => r.action === 'skip_source_mismatch').length, 5);
    assert.equal(plan.rows.filter(r => r.action === 'already_correct').length, 5);
    assert.deepEqual(await snapshot(), before, 'preview must not modify facts');
    const partialPreview = await db.exec(generate(false, '2026-11-05T12:00:00Z', '2026-11-05T12:01:00Z'));
    const partialPlan = partialPreview.find(r => r.rows.some(row => row.action));
    assert.ok(partialPlan.rows.every(r => r.seconds === 60), 'partial rollup buckets must be excluded');
    assert.throws(() => generate(false, '2026-11-05T00:00:00Z', '2026-11-13T00:00:00Z'), /repair windows/);
    assert.throws(() => generate(false, '2026-11-05T00:00:00', '2026-11-06T00:00:00Z'), /invalid timestamp/);
    await db.exec(generate(true));
    const after = await snapshot();
    assert.deepEqual(after.filter(r => r.group_id === 42).map(r => [r.error_category,r.error_requests]),
      [['quota_or_balance',2],['rate_or_capacity',1],['upstream_forbidden',1]]);
    assert.deepEqual(after.filter(r => r.group_id !== 42), before.filter(r => r.group_id !== 42),
      'incomplete history, missing sources, prior repairs, and authentication must remain intact');
    for (const seconds of tiers) {
      const table = seconds === 60 ? 'channel_monitor_v2_error_metrics_1m' : 'channel_monitor_v2_error_metrics_rollup';
      const where = seconds === 60 ? '' : `AND bucket_seconds=${seconds}`;
      const rows = (await db.query(`SELECT error_category,error_requests FROM ${table} WHERE group_id=42 ${where} ORDER BY 1`)).rows;
      assert.deepEqual(rows.map(r => [r.error_category,r.error_requests]),
        [['quota_or_balance',2],['rate_or_capacity',1],['upstream_forbidden',1]]);
    }
    await db.exec(generate(true));
    assert.deepEqual(await snapshot(), after, 'repair must be idempotent');
    console.log('PASS: real PostgreSQL classifier, request deduplication, five tiers, count preservation, incomplete source guards, read-only preview, and repeated repair');
  } finally { await db.close(); }
}
main().catch(err => { console.error(err); process.exitCode=1; });
