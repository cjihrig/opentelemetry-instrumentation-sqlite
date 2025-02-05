'use strict';
const assert = require('node:assert');
const {
  afterEach,
  before,
  beforeEach,
  suite,
  test
} = require('node:test');
const {
  SpanKind,
  SpanStatusCode
} = require('@opentelemetry/api');
const {
  ATTR_DB_QUERY_TEXT,
  ATTR_DB_SYSTEM,
  DB_SYSTEM_VALUE_SQLITE
} = require('@opentelemetry/semantic-conventions/incubating');
const {
  InMemorySpanExporter,
  SimpleSpanProcessor,
  Span
} = require('@opentelemetry/sdk-trace-base');
const { NodeTracerProvider } = require('@opentelemetry/sdk-trace-node');
const { SqliteInstrumentation } = require('../lib');
const pkg = require('../package.json');

// The instrumentation must be created before importing the instrumented module.
const instr = new SqliteInstrumentation();
const { DatabaseSync, StatementSync } = require('node:sqlite');
instr.disable();

function assertSpan(actual, expected) {
  assert.ok(actual instanceof Span);
  assert.strictEqual(actual.kind, SpanKind.CLIENT);
  assert.strictEqual(actual.instrumentationLibrary.name, pkg.name);
  assert.strictEqual(actual.instrumentationLibrary.version, pkg.version);
  assert.strictEqual(actual.attributes[ATTR_DB_SYSTEM], DB_SYSTEM_VALUE_SQLITE);
}

suite('SqliteInstrumentation', () => {
  let memoryExporter;

  before(() => {
    memoryExporter = new InMemorySpanExporter();
    instr.setTracerProvider(new NodeTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(memoryExporter)]
    }));
  });

  beforeEach(() => {
    memoryExporter.reset();
    instr.enable();
  });

  afterEach(() => {
    instr.disable();
  });

  suite('DatabaseSync.prototype.exec()', () => {
    test('is instrumented', () => {
      const db = new DatabaseSync(':memory:');
      const sql = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      const result = db.exec(sql);
      assert.strictEqual(result, undefined);
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 1);
      assertSpan(spans[0]);
      assert.strictEqual(spans[0].name, 'exec');
      assert.strictEqual(spans[0].attributes[ATTR_DB_QUERY_TEXT], undefined);
      assert.deepStrictEqual(spans[0].status, { code: SpanStatusCode.OK });
    });

    test('instrumentation can be disabled', () => {
      instr.disable();
      const db = new DatabaseSync(':memory:');
      const sql = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      const result = db.exec(sql);
      assert.strictEqual(result, undefined);
      assert.deepStrictEqual(memoryExporter.getFinishedSpans(), []);
    });

    test('errors are reported', () => {
      const db = new DatabaseSync(':memory:');

      assert.throws(() => {
        db.exec('invalid-sql');
      }, /syntax error/);
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 1);
      assertSpan(spans[0]);
      assert.strictEqual(spans[0].name, 'exec');
      assert.strictEqual(spans[0].attributes[ATTR_DB_QUERY_TEXT], undefined);
      assert.deepStrictEqual(spans[0].status, {
        code: SpanStatusCode.ERROR,
        message: 'near "invalid": syntax error'
      });
    });
  });

  suite('DatabaseSync.prototype.prepare()', () => {
    test('is instrumented', () => {
      const db = new DatabaseSync(':memory:');
      const sql = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 1);
      assertSpan(spans[0]);
      assert.strictEqual(spans[0].name, 'prepare');
      assert.strictEqual(spans[0].attributes[ATTR_DB_QUERY_TEXT], sql);
      assert.deepStrictEqual(spans[0].status, { code: SpanStatusCode.OK });
    });

    test('instrumentation can be disabled', () => {
      instr.disable();
      const db = new DatabaseSync(':memory:');
      const sql = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.deepStrictEqual(memoryExporter.getFinishedSpans(), []);
    });

    test('errors are reported', () => {
      const db = new DatabaseSync(':memory:');
      const sql = 'invalid-sql';

      assert.throws(() => {
        db.prepare(sql);
      }, /syntax error/);
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 1);
      assertSpan(spans[0]);
      assert.strictEqual(spans[0].name, 'prepare');
      assert.strictEqual(spans[0].attributes[ATTR_DB_QUERY_TEXT], sql);
      assert.deepStrictEqual(spans[0].status, {
        code: SpanStatusCode.ERROR,
        message: 'near "invalid": syntax error'
      });
    });
  });

  suite('StatementSync.prototype.all()', () => {
    test('is instrumented', () => {
      const db = new DatabaseSync(':memory:');
      const create = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      assert.strictEqual(db.exec(create), undefined);
      const sql = 'INSERT INTO test (id, data) VALUES (?, ?) RETURNING *';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.deepStrictEqual(stmt.all(1, 'foo'), [
        { __proto__: null, id: 1, data: 'foo' }
      ]);
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      assertSpan(spans[2]);
      assert.strictEqual(spans[2].name, 'all');
      assert.strictEqual(spans[2].attributes[ATTR_DB_QUERY_TEXT], sql);
      assert.deepStrictEqual(spans[2].status, { code: SpanStatusCode.OK });
    });

    test('instrumentation can be disabled', () => {
      instr.disable();
      const db = new DatabaseSync(':memory:');
      const create = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      assert.strictEqual(db.exec(create), undefined);
      const sql = 'INSERT INTO test (id, data) VALUES (?, ?) RETURNING *';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.deepStrictEqual(stmt.all(1, 'foo'), [
        { __proto__: null, id: 1, data: 'foo' }
      ]);
      assert.deepStrictEqual(memoryExporter.getFinishedSpans(), []);
    });

    test('errors are reported', () => {
      const db = new DatabaseSync(':memory:');
      const create = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      assert.strictEqual(db.exec(create), undefined);
      const sql = 'INSERT INTO test (id, data) VALUES (?, ?) RETURNING *';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.throws(() => {
        stmt.all({});
      });
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      assertSpan(spans[2]);
      assert.strictEqual(spans[2].name, 'all');
      assert.strictEqual(spans[2].attributes[ATTR_DB_QUERY_TEXT], sql);
      assert.deepStrictEqual(spans[2].status, {
        code: SpanStatusCode.ERROR,
        message: 'NOT NULL constraint failed: test.id'
      });
    });
  });

  suite('StatementSync.prototype.get()', () => {
    test('is instrumented', () => {
      const db = new DatabaseSync(':memory:');
      const create = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      assert.strictEqual(db.exec(create), undefined);
      const sql = 'INSERT INTO test (id, data) VALUES (?, ?) RETURNING *';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.deepStrictEqual(
        stmt.get(1, 'foo'),
        { __proto__: null, id: 1, data: 'foo' }
      );
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      assertSpan(spans[2]);
      assert.strictEqual(spans[2].name, 'get');
      assert.strictEqual(spans[2].attributes[ATTR_DB_QUERY_TEXT], sql);
      assert.deepStrictEqual(spans[2].status, { code: SpanStatusCode.OK });
    });

    test('instrumentation can be disabled', () => {
      instr.disable();
      const db = new DatabaseSync(':memory:');
      const create = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      assert.strictEqual(db.exec(create), undefined);
      const sql = 'INSERT INTO test (id, data) VALUES (?, ?) RETURNING *';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.deepStrictEqual(
        stmt.get(1, 'foo'),
        { __proto__: null, id: 1, data: 'foo' }
      );
      assert.deepStrictEqual(memoryExporter.getFinishedSpans(), []);
    });

    test('errors are reported', () => {
      const db = new DatabaseSync(':memory:');
      const create = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      assert.strictEqual(db.exec(create), undefined);
      const sql = 'INSERT INTO test (id, data) VALUES (?, ?) RETURNING *';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.throws(() => {
        stmt.get({});
      });
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      assertSpan(spans[2]);
      assert.strictEqual(spans[2].name, 'get');
      assert.strictEqual(spans[2].attributes[ATTR_DB_QUERY_TEXT], sql);
      assert.deepStrictEqual(spans[2].status, {
        code: SpanStatusCode.ERROR,
        message: 'NOT NULL constraint failed: test.id'
      });
    });
  });

  suite('StatementSync.prototype.run()', () => {
    test('is instrumented', () => {
      const db = new DatabaseSync(':memory:');
      const create = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      assert.strictEqual(db.exec(create), undefined);
      const sql = 'INSERT INTO test (id, data) VALUES (?, ?) RETURNING *';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.deepStrictEqual(
        stmt.run(1, 'foo'),
        { changes: 0, lastInsertRowid: 1 }
      );
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      assertSpan(spans[2]);
      assert.strictEqual(spans[2].name, 'run');
      assert.strictEqual(spans[2].attributes[ATTR_DB_QUERY_TEXT], sql);
      assert.deepStrictEqual(spans[2].status, { code: SpanStatusCode.OK });
    });

    test('instrumentation can be disabled', () => {
      instr.disable();
      const db = new DatabaseSync(':memory:');
      const create = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      assert.strictEqual(db.exec(create), undefined);
      const sql = 'INSERT INTO test (id, data) VALUES (?, ?) RETURNING *';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.deepStrictEqual(
        stmt.run(1, 'foo'),
        { changes: 0, lastInsertRowid: 1 }
      );
      assert.deepStrictEqual(memoryExporter.getFinishedSpans(), []);
    });

    test('errors are reported', () => {
      const db = new DatabaseSync(':memory:');
      const create = 'CREATE TABLE test (id NUMBER NOT NULL, data TEXT)';
      assert.strictEqual(db.exec(create), undefined);
      const sql = 'INSERT INTO test (id, data) VALUES (?, ?) RETURNING *';
      const stmt = db.prepare(sql);
      assert.ok(stmt instanceof StatementSync);
      assert.throws(() => {
        stmt.run({});
      });
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 3);
      assertSpan(spans[2]);
      assert.strictEqual(spans[2].name, 'run');
      assert.strictEqual(spans[2].attributes[ATTR_DB_QUERY_TEXT], sql);
      assert.deepStrictEqual(spans[2].status, {
        code: SpanStatusCode.ERROR,
        message: 'NOT NULL constraint failed: test.id'
      });
    });
  });
});
