'use strict';

const elasticsearch = require('elasticsearch');
const CronJob = require('cron').CronJob;

const INDEX_TYPES = {
  single: null,
  daily: '0 0 0 * * *', // Every day at midnight
  monthly: '0 0 0 1 * *' // Midnight on the first day of every month
};

module.exports = function(opts) {
  opts = opts || {};

  const host = opts.host;
  const appName = opts.appName;
  const env = opts.env;
  const index = opts.index || 'defaultindex';
  const type = opts.type || 'defaultType';
  const esLogLevel = opts.esLogLevel || 'info';
  const separator = opts.separator || '#';
  const indexType = opts.indexType || 'single';

  if (!host) {
    throw new Error('Must provide an "opts.host" string');
  }

  if (Object.keys(INDEX_TYPES).indexOf(indexType) === -1) {
    throw new Error('Invalid "opts.indexType" string; must be one of ' + 
      JSON.stringify(Object.keys(INDEX_TYPES)));
  }

  const client = new elasticsearch.Client({
    host: host,
    log: esLogLevel
  });

  const indexCronTime = INDEX_TYPES[indexType];

  let indexInfo = newIndex();

  if (indexCronTime) {
    // Create a new index every so often
    new CronJob({
      cronTime: indexCronTime,
      onTick: function() {
        indexInfo = newIndex(); 
      },
      start: true
    });
  }

  return function flush(items) {
    const toSend = [];
    items.forEach(function(item) {
      toSend.push(indexInfo.headerItem);
      toSend.push({
        appName: appName,
        serviceName: [item.service, item.fnName].join(separator),
        env: env,
        time: item.time,
        created: item.created.toISOString()
      });
    });

    return indexInfo.createIndexPromise // Make sure the index is created before anything else
      .then(function() {
        return client.bulk({
          body: toSend
        });
      });

  };

  function newIndex() {
    const newIndexName = getCurrentIndexName();
    return {
      headerItem: { 
        index: {
          _index: newIndexName, 
          _type: type 
        } 
      },
      createIndexPromise: createIndexInES(client, newIndexName, type)
    };
  }

  // Gets the name of the index that any record created right now should use
  function getCurrentIndexName() {
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth() + 1;
    const date = now.getDate();

    switch (indexType) {
      case 'single': 
        return index;
      case 'monthly':
          return `${index}-${year}-${month}`;
      case 'daily':
        return `${index}-${year}-${month}-${date}`;
      default:
        throw new Error('Should never be here if indexType is validated: ' + indexType);
    }
  }

};

// Creates the necessary Elasticsearch index if it doesn't alreay exist
function createIndexInES(client, indexName, type) {

  const mappings = {
    [type]: {
      properties: {
        appName: { type: 'string', index: 'not_analyzed' },
        serviceName: { type: 'string', index: 'not_analyzed' },
        env: { type: 'string', index: 'not_analyzed' },
        time: { type: 'integer' },
        created: { type: 'date'}
      }
    }
  };

  return client.indices.create({
    index: indexName,
    body: {
      mappings: mappings
    }
  })
  .catch(function(err) {
    // We only care if the error is something other than the index already existing
    if (err.message.indexOf('index_already_exists_exception') === -1) {
      throw err;
    }
  });
}
