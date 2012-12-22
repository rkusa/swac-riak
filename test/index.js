var Arkansas = require('arkansas')
  , should   = require('should')
  , db       = require('riak-js').getClient({
    host: process.env.RIAK_HOST || 'localhost',
    port: process.env.RIAK_PORT || 8098
  })
  , model

var domain = require('domain')
  , d = domain.create()
d.req = {}

var domainify = function(fn) {
  return function(done) {
    d.run(fn.bind(null, done))
  }
}

function cleanup(done) {
  var buf = []
    , keys = function(keys) { buf = buf.concat(keys) }
    , end = function() {
      var count = buf.length
      if (count === 0) return done()
      var removed = function() {
        if (--count === 0) done()
      }
      buf.forEach(function(key) {
        db.remove('TestModel', key, removed)
      })
    }
  db.keys('TestModel')
    .on('keys', keys)
    .on('end', end)
    .start()
}

describe('Arkansas Riak Adapter', function() {
  before(cleanup)
  after(cleanup)
  describe('CRUD', function() {
    before(function(done) {
      model = Arkansas.Model.define('TestModel', function() {
        this.use(require('../'), {
          host: process.env.RIAK_HOST || 'localhost',
          port: process.env.RIAK_PORT || 8098
        }, function() {
          this.add2i('by-type', 'type')
        })
        this.property('key')
        this.property('type')
      }, done)
    })
    var cur
    it('POST should work', domainify(function(done) {
      model.post({ key: '1', type: 'a' }, function(err, row) {
        should.not.exist(err)
        cur = row
        db.get('TestModel', row.id, function(err, body) {
          if (err) throw err
          body.key.should.equal(row.key)
          body.type.should.equal(row.type)
          done()
        })
      })
    }))
    it('PUT should work', domainify(function(done) {
      cur.key = 2
      cur.type = 'b'
      model.put(cur.id, cur, function(err, row) {
        should.not.exist(err)
        db.get('TestModel', row.id, function(err, body) {
          if (err) throw err
          body.key.should.equal(cur.key)
          body.type.should.equal(cur.type)
          done()
        })
      })
    }))
    it('GET should work', domainify(function(done) {
      model.get(cur.id, function(err, body) {
        should.not.exist(err)
        body.id.should.equal(cur.id)
        body.key.should.equal(cur.key)
        body.type.should.equal(cur.type)
        done()
      })
    }))
    it('LIST should work', domainify(function(done) {
      model.post({ key: '1', type: 'a' }, function(err, row) {
        should.not.exist(err)
        model.list(function(err, items) {
          if (err) throw err
          items.should.have.lengthOf(2)
          done()
        })
      })
    }))
  })
  describe('Secondary Index', function() {
    it('should be established', function(done) {
      model.post({ key: 1, type: 'a' }, function(err, row) {
        should.not.exist(err)
        db.get('TestModel', row.id, function(err, data, meta) {
          meta._headers.should.have.property('x-riak-index-type_bin', 'a')
          done()
        })
      })
    })
  })
})
