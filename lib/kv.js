const fs = require('fs')
const redis = require('redis')
const streams = require('web-streams-polyfill')
const { promisify } = require('util')

class KeyValueStore {
  constructor (uri) {

    this.client = redis.createClient(uri)
    this.setAsync = promisify(this.client.set).bind(this.client)
    this.getAsync = promisify(this.client.get).bind(this.client)
    this.keysAsync = promisify(this.client.keys).bind(this.client)
    this.scanAsync = promisify(this.client.scan).bind(this.client)
    this.ttlAsync = promisify(this.client.ttl).bind(this.client)

    this.store = new Map()
  }

  put (key, value, opts={}) {
    const cmd = [key, value]
    const epoch = Math.floor(Date.now() / 1000)
    console.log(`got expirationTtl: ${opts.expirationTtl}`)
    if ((typeof(opts.expirationTtl) != 'undefined') || (opts.expirationTtl != null)) {
      if ((typeof(opts.expirationTtl) != 'number') || (opts.expirationTtl == NaN)) {
        throw new TypeError('Unsupported expirationTtl, only numbers are supported')
      }
      cmd.push('EX', opts.expirationTtl)
    } else if ((typeof(opts.expiration) != 'undefined') || (opts.expiration != null)) {
      if ((typeof(opts.expiration) != 'number') || (opts.expiration == NaN)) {
        throw new TypeError('Unsupported expiration, only numbers are supported')
      }
      const exp = opts.expiration - epoch
      if (exp <= 0) {
        console.log('expiration is in the past')
        return Promise.resolve(false)
      }
      cmd.push('EX', exp)
    }
    return this.setAsync(cmd)
      .then(function(msg) { return msg === 'OK' })
      .catch(function(msg) {
        throw new Error(msg)
      })
  }

  get (key, type = 'text') {
    const validTypes = ['text', 'arrayBuffer', 'json', 'stream']
    if (!validTypes.includes(type)) {
      throw new TypeError('Unknown response type. Possible types are "text", "arrayBuffer", "json", and "stream".')
    }

    return this.getAsync(key)
      .then(
        function(value) {
          if ((value === undefined) || (value === null))  {
            return Promise.resolve(null)
          }
          switch (type) {
            case 'text':
              return Promise.resolve(value.toString())
            case 'arrayBuffer':
              return Promise.resolve(Uint8Array.from(value).buffer)
            case 'json':
              return Promise.resolve(JSON.parse(value.toString()))
            case 'stream':
              const { readable, writable } = new streams.TransformStream()
              const writer = writable.getWriter()
              writer.write(Uint8Array.from(value)).then(() => writer.close())
              return Promise.resolve(readable)
          }
        },
        function(err) {
          if (err) throw err
        }
      )
      .catch(function(msg) {
        throw new Error(msg)
      })
  }

  // expiration is broken / not implemented yet
  list (opts={prefix: '', limit: 1000, cursor: '0'}) {

    const $this = this
    const keys = []
    const cmdOpts = {
      prefix: opts.prefix || '',
      limit: opts.limit || 1000,
      cursor: opts.cursor || 0
    }
    const cmd = [cmdOpts.cursor, 'MATCH']
    if ((typeof(cmdOpts.prefix) !== 'undefined') || (typeof(cmdOpts.prefix) !== 'null')) {
      cmd.push(`${cmdOpts.prefix}*`)
    } else {
      cmd.push('*')
    }
    cmd.push('COUNT',cmdOpts.limit)

    return this.scanAsync(cmd)
      .then(
        function(res){
          const cursor = res[0]
          const keys = res[1].sort().map(key => ({
            name: key,
            expiration: Promise.resolve($this.ttlAsync(key))
          }))
          const listComplete = cursor == 0 ? true : false

          const scanRes = {
            keys: keys,
            cursor: cursor,
            list_complete: listComplete
          }

          console.log(scanRes)
          return Promise.resolve(scanRes)
        },
        function(err){
          if (err) throw err
        }
      )
      .catch(function(err) {
        throw new Error(err)
      })

    // // Iterate over keys available
    // for (const key of this.store.keys()) {
    //   // If options.prefix is set, skip any keys that don't match the prefix
    //   if (options.prefix && key.indexOf(options.prefix) !== 0) continue

    //   // Push the key if prefix matches or prefix is not set
    //   keys.push(key)
    // }

    // // Order keys and format them as objects to match CF
    // const orderedKeys = keys.sort().map(key => ({ name: key }))

    // return Promise.resolve({
    //   keys: orderedKeys,
    //   cursor: 'not-implemented',
    //   list_complete: true,
    // })
  }

  getTtl(key) {
    return this.ttlAsync(key)
      .then(
        function(res){
          console.log(res)
          return res
        },
        function(err){
          if (err) throw err
        }
      )
      .catch(function(err) {
        throw new Error(err)
      })
  }

  delete (key) {
    if (!this.store.has(key)) {
      throw new Error('HTTP DELETE request failed: 404 Not Found')
    }
    this.store.delete(key)
    return Promise.resolve(undefined)
  }

  ping () {
    return this.client.ping()
  }
}

module.exports = { KeyValueStore }
