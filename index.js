'use strict';

var elasticsearch = require('elasticsearch');

module.exports = function(opts) {
  opts = opts || {};

  var host = opts.host;
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

  // At startup, always make sure the index exists
  var createIndexPromise = createIndex();

  return function flush(items) {

    var toSend = [];
    items.forEach(function(item) {
      toSend.push({ index:  { _index: index, _type: type } });
      toSend.push({
        created: item.created.toISOString(),
        name: item.name,
        time: item.time,
        env: env
      });
    });

    return createIndexPromise() // Make sure the index is created before anything else
      .then(function() {
        return client.bulk({
          body: toSend
        });

  };

  ////

  // Creates the necessary Elasticsearch index if it doesn't alreay exist
  function createIndex() {

    var mappings = {};
    mappings[type] = {
      properties: {
        name: { type: 'string', index: 'not_analyzed' },
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
      // If the index already exists, then no big deal
      if (err.message.indexOf('IndexAlreadyExistsException') === -1) {
        throw err;
      }
    });
  }

};
