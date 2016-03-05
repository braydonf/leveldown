'use strict';

const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const rimraf = require('rimraf');
const leveldown = require('../');
const dbDir = path.resolve(__dirname, './read-aggregate.db');

var db = leveldown(dbDir);

db.open(function (err) {
  if (err) {
    throw err;
  }

  const TOTAL_RECORDS = 1000000;
  const SPACER = new Buffer('00', 'hex');
  const PREFIX = new Buffer('01', 'hex');
  const MAX_DOUBLE = new Buffer('ffffffffffffffff', 'hex');
  const MIN_DOUBLE = new Buffer('0000000000000000', 'hex');

  cleanTestData();

  function cleanTestData(done) {
    rimraf(dbDir, function(err) {
      if (err) {
	throw err;
      }
      writeTestData();
    });
  }

  function writeTestData(done) {
    var c = 0;
    var operations = [];
    while (c < TOTAL_RECORDS) {
      var tempurature = crypto.randomBytes(4);
      var date = crypto.randomBytes(8);
      operations.push({
	type: 'put',
	key: Buffer.concat([PREFIX, date, SPACER, tempurature])
      });
      c++;
    }
    db.batch(operations, function(err) {
      if (err) {
	throw err;
      }
      runIterator();
    });
  }

  function runIterator() {
    var start = new Date();
    var count = 0;

    var iterator = db.iterator({
      highWaterMark: 16 * 1024, // default
      gt: Buffer.concat([PREFIX, MIN_DOUBLE]),
      lt: Buffer.concat([PREFIX, MAX_DOUBLE]),
      keyEncoding: 'binary',
      keys: true,
      values: false
    });

    function iterate() {
      iterator.next(function(err, key) {
	if (err) {
	  return done(err);
	}
	if (key) {
	  count++;
	  iterate();
	} else {
	  done();
	}
      });
    }

    function done() {
      var end = new Date();
      console.log('iterator:');
      console.log('milliseconds', end - start);
      console.log('count', count);

      runIteratorWithCache();
    }

    iterate();

  }

  function runIteratorWithCache() {
    var start = new Date();
    var count = 0;

    var iterator = db.iterator({
      highWaterMark: 16 * 1024, // default
      includeCache: true,
      gt: Buffer.concat([PREFIX, MIN_DOUBLE]),
      lt: Buffer.concat([PREFIX, MAX_DOUBLE]),
      keyEncoding: 'binary',
      keys: true,
      values: false
    });

    function aggregate() {
      iterator.next(function(err, cache) {
	if (err) {
	  return done(err);
	}
	if (cache) {
	  for(var i = 0; i < cache.length; i++) {
	    if (i % 2) {
	      count++;
	    }
	  }
	  aggregate();
	} else {
	  done();
	}
      });
    }

    function done() {
      var end = new Date();
      console.log('iterator (with cache returned):');
      console.log('milliseconds', end - start);
      console.log('count', count);
    }

    aggregate();
  }

});
