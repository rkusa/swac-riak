# [SWAC](https://github.com/rkusa/swac)' ![](https://dl.dropbox.com/u/6699613/swac-logo.png) Riak Adapter

## Status [![Build Status](https://secure.travis-ci.org/rkusa/swac-riak.png)](http://travis-ci.org/rkusa/swac-riak)

Don't use yet.

## Usage

```js
this.use('riak', { host: 'localhost', port: 8098 }, function() {
  // definition
})
```

### Options

* **host** - (default: localhost) riak's host
* **port** - (default: 8098) riak's port

## Definition API

The definitions context provides the following methods:

### .add2i(name, prop)

**Arguments:**

* **name** - the view's name
* **prop** - the property that should be indexed

**Example:**

```js
this.use('riak', function() {
  this.view('by-user', 'user')
})
```