var riak = require('riak-js')

var API = function(db, model, define, callback) {
  this.db     = db
  this.model  = model
  this.views  = {}
  this.index  = []
  this.queue  = []
  this.callback = function() {
    if (this.queue.length === 0) {
      if (callback) callback()
    } else {
      (this.queue.shift())()
    }
  }
  if (define)   define.call(this)
  var that = this
  process.nextTick(function() {
    if (that.queue.length > 0) (that.queue.shift())()
  })
}

API.prototype.add2i = function(name, prop, isInt) {
  this.index.push(prop)
  this.views[name] = this.db.mapreduce
  .add({
    bucket: this.model._type,
    index: prop + '_' + (isInt === true ? 'int' : 'bin')
  })
  .map({
    language: "javascript",
    source: function(value, keyData, arg) { 
      var data = Riak.mapValuesJson(value)[0]
      return [{
        key: value.key,
        data: data
      }]
    }
  })
}

API.prototype.allowSearch = function() {
  var that = this
  this.queue.push(function() {
    that.db.saveBucket(that.model._type, {
      search: true
    }, that.callback.bind(that))
  })
}

API.prototype.view = function(name, view) {
  var that = this

  this.params[name] = {}
  if (typeof view === 'function') {
    this.queue.push(function() {
      that.views[name] = view
      that.callback()
    })
  } else {
    if (!view.reduce) this.params[name].include_docs = true
    else this.params[name].reduce = true
    this.queue.push(function() {
      that.db.get(that.design, function(err, body) {
        if (err) {
          if (err.status_code !== 404) throw err
          else {
            body = {
              language: 'javascript',
              views: {}
            }
          }
        }
        body.views[name] = !view.map ? { map: view } : view
        that.db.insert(body, that.design, that.callback.bind(that))
      })
    })
  }
}

API.prototype.createModel = function(id, data) {
  data.id        = id
  var instance   = new this.model(data)
  instance.isNew = false
  return instance
}

API.prototype.get = function(id, callback) {
  if (!callback) callback = function() {}
  if (!id) return callback(null, null)

  var that = this
    , domain = process.domain
  this.db.get(this.model._type, id, function(err, body, meta) {
    process.domain = domain
    if (err) {
      if (err.notFound === true) return callback(null, null)
      else                       return callback(err, null)
    }
    callback(null, that.createModel(meta.key, body))
  })
}

API.prototype.list = function(/*view, key, callback*/) {
  var args = Array.prototype.slice.call(arguments)
    , that = this
    , callback = args.pop()

  var view = this.views[view = args.shift() || 'all']
    , key = args.shift() || this.model._type
  if (key) view.inputs.key = key
  else     delete view.inputs.key

  var domain = process.domain
  view.run(function(err, body, meta) {
    process.domain = domain
    if (err) return callback(err, null)
    callback(null, body.map(function(row) {
      return that.createModel(row.key, row.data)
    }))
  })
}

API.prototype.put = function(instance, callback) {
  this.post(instance, callback)
}

API.prototype.post = function(instance, callback) {
  if (!callback) callback = function() {}

  if (!(instance instanceof this.model))
    instance = new this.model(instance)
  var props = instance.toJSON(true)
  delete props.id
  props.$type = this.model._type

  var that = this
  
  // add 2i HTTP Headers
  var index = {}
  this.index.forEach(function(prop) {
    index[prop] = props[prop]
  })

  var domain = process.domain
  this.db.save(this.model._type, instance.id, props, {
    index: index,
    returnbody: false
  }, function(err, data, meta) {
    process.domain = domain
    if (err) return callback(err, null)
    instance.id = meta.key
    instance.isNew = false
    callback(null, instance)
  })
}

API.prototype.delete = function(instance, callback) {
  if (!callback) callback = function() {}
  var domain = process.domain
  this.db.remove(this.model._type, instance.id, function(err) {
    process.domain = domain
    callback(err)
  })
}

exports.initialize = function(model, opts, define, callback) {
  if (typeof opts === 'function') {
    define = opts
    opts = {}
  }
  var db = riak.getClient(opts)
    , api = new API(db, model, define, callback)

  api.add2i('all', '$type')

  return api
}
