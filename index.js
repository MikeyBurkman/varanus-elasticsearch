'use strict';

var elasticsearch = require('elasticsearch');

module.exports = function(opts) {
  opts = opts || {};

  var host = opts.host;
  var appName = opts.appName;
  var env = opts.env;
  var index = opts.index || 'defaultindex';
  var type = opts.type || 'defaultType';
  var esLogLevel = opts.esLogLevel || 'info';

  if (!host) {
    throw new Error('Must provide an `opts.host` string');
  }

  var client = new elasticsearch.Client({
    host: host,
    log: esLogLevel
  });

  // The first item for every record we send is always identical, so just create it once
  var headerItem = { index:  { _index: index, _type: type } };

  // At startup, always make sure the index exists
  var createIndexPromise = createIndex(client, index, type);

  return function flush(items) {

    var toSend = [];
    items.forEach(function(item) {
      toSend.push(headerItem);
      toSend.push({
        appName: appName,
        serviceName: item.service + '-' + item.fnName,
        env: env,
        time: item.time,
        created: item.created.toISOString()
      });
    });

    return createIndexPromise // Make sure the index is created before anything else
      .then(function() {
        return client.bulk({
          body: toSend
        });
      });

  };

};

// Creates the necessary Elasticsearch index if it doesn't alreay exist
function createIndex(client, index, type) {

  var mappings = {};
  mappings[type] = {
    properties: {
      appName: { type: 'string', index: 'not_analyzed' },
      serviceName: { type: 'string', index: 'not_analyzed' },
      env: { type: 'string', index: 'not_analyzed' },
      time: { type: 'integer' },
      created: { type: 'date'}
    }
  };

  return client.indices.create({
    index: index,
    body: {
      mappings: mappings
    }
  })
  .catch(function(err) {
    // We only care if the error is something other than the index already existing
    if (err.message.indexOf('IndexAlreadyExistsException') === -1) {
      throw err;
    }
  });
}
