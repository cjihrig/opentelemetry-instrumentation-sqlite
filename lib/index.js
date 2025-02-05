'use strict';
const {
  SpanKind,
  SpanStatusCode
} = require('@opentelemetry/api');
const {
  InstrumentationBase,
  InstrumentationNodeModuleDefinition
} = require('@opentelemetry/instrumentation');
const {
  ATTR_DB_QUERY_TEXT,
  ATTR_DB_SYSTEM,
  DB_SYSTEM_VALUE_SQLITE
} = require('@opentelemetry/semantic-conventions/incubating');
const pkg = require('../package.json');
const STMT_METHODS = ['all', 'get', 'run'];

class SqliteInstrumentation extends InstrumentationBase {
  constructor(config) {
    super(pkg.name, pkg.version, config);
  }

  init() {
    const instrumentation = new InstrumentationNodeModuleDefinition(
      'node:sqlite',
      ['>=22.5.0'],
      (moduleExports) => {
        const { DatabaseSync, StatementSync } = moduleExports;
        const instr = this;

        this._wrap(DatabaseSync.prototype, 'exec', (original) => {
          return function exec(...params) {
            const span = instr.tracer.startSpan(original.name, {
              kind: SpanKind.CLIENT,
              attributes: {
                [ATTR_DB_SYSTEM]: DB_SYSTEM_VALUE_SQLITE
              }
            });
            return genericWrap(original, this, params, span);
          };
        });

        this._wrap(DatabaseSync.prototype, 'prepare', (original) => {
          return function prepare(...params) {
            const span = instr.tracer.startSpan(original.name, {
              kind: SpanKind.CLIENT,
              attributes: {
                [ATTR_DB_SYSTEM]: DB_SYSTEM_VALUE_SQLITE,
                [ATTR_DB_QUERY_TEXT]: params[0]
              }
            });
            return genericWrap(original, this, params, span);
          };
        });

        this._massWrap(StatementSync.prototype, STMT_METHODS, (original) => {
          return function statementExecution(...params) {
            const span = instr.tracer.startSpan(original.name, {
              kind: SpanKind.CLIENT,
              attributes: {
                [ATTR_DB_SYSTEM]: DB_SYSTEM_VALUE_SQLITE,
                [ATTR_DB_QUERY_TEXT]: this.sourceSQL
              }
            });
            return genericWrap(original, this, params, span);
          };
        });

        return moduleExports;
      },
      (moduleExports) => {
        const { DatabaseSync, StatementSync } = moduleExports;

        this._unwrap(DatabaseSync.prototype, 'exec');
        this._unwrap(DatabaseSync.prototype, 'prepare');
        this._massUnwrap(StatementSync.prototype, STMT_METHODS);
      }
    );

    return [instrumentation];
  }
}

function genericWrap(original, self, params, span) {
  try {
    const result = original.apply(self, params);

    span.setStatus({ code: SpanStatusCode.OK });
    return result;
  } catch (err) {
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: err?.message
    });
    throw err;
  } finally {
    span.end();
  }
}

module.exports = { SqliteInstrumentation };
module.exports.default = { SqliteInstrumentation };
