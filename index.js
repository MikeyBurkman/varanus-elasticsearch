'use strict';

const esSender = require('elasticsearch-sender');

module.exports = function(opts) {
  opts = opts || {};

  // Required
  const host = opts.host;
  const appName = opts.appName;
  const env = opts.env;
  const indexName = opts.indexName;
  const indexType = opts.indexType;
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
      appName: { type: 'string', index: 'not_analyzed' },
      serviceName: { type: 'string', index: 'not_analyzed' },
      env: { type: 'string', index: 'not_analyzed' },
      time: { type: 'integer' },
      created: { type: 'date'}
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
