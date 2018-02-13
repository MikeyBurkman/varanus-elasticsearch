'use strict';

const esSender = require('elasticsearch-sender');

module.exports = function(opts) {
  opts = opts || {};

  // Required
  const host = opts.host;
  const appName = opts.appName;
  const env = opts.env;
  const indexName = opts.indexName;
  const indexType = opts.indexType; // 'monthly', 'daily', or 'single'
  const recordType = opts.recordType;

  // Optional
  const esLogLevel = opts.esLogLevel;
  const separator = opts.separator || '#';

  const sendToEs = esSender.buildSender({
    host: host,
    indexName: indexName,
    indexType: indexType,
    recordType: recordType,
    esLogLevel: esLogLevel,
    indexShape: {
      appName: { type: 'keyword', index: true },
      serviceName: { type: 'keyword', index: true },
      env: { type: 'keyword', index: true },
      created: { type: 'date', index: true},
      time: { type: 'integer' }
    }
  });

  return function flush(items) {
    const records = items.map((item) => ({
      appName: appName,
      serviceName: [item.service, item.fnName].join(separator),
      env: env,
      time: item.time,
      created: item.created.toISOString()
    }));

    return sendToEs(records);
  };
}
