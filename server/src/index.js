const sqlite3 = require('sqlite3').verbose();
const co = require('co');
const net = require('net');
const fs = require('fs');


/** This class implements the base class for a waitable condition
*/
class WaitableConditionBase
{
  constructor()
  {
    /// Whether this condition is currently signalled
    this._signalled = false;
    /** Promise and resolve function for waiting for signalled status change
        @cell promise Promise
        @cell resolve Resolve function for the promise
    */
    this._wait = null;
    /// Name for debugging purposes
    this.name = "";
  }

  _waitSignalledInternal(negate)
  {
    // Is the signalled state already what the user wants?
    if (this._signalled !== negate)
      return Promise.resolve(this);

    // Create a promise to wait for if there isn't one yet for the next signalled status change
    if (!this._wait)
    {
      this._wait = { promise: null, resolve: null };
      this._wait.promise = new Promise(resolve => this._wait.resolve = resolve);
    }

    return this._wait.promise;
  }

  /// Updates the current signalled status (internal function, for use by derived objects
  _setSignalled(signalled)
  {
    signalled = !!signalled;
    if (this._signalled === signalled)
      return;

    this._signalled = signalled;
    if (this._wait)
    {
      this._wait.resolve(this);
      this._wait = null;
    }
  }

  // Returns a promise that be resolved when the status is or becomes signalled
  waitSignalled()
  {
    return this._waitSignalledInternal(false);
  }

  // Returns a promise that be resolved when the status is or becomes not signalled
  waitNotSignalled()
  {
    return this._waitSignalledInternal(true);
  }
}

/// This class implements a FIFO with a wait function that is resolved when an element is present
class FIFO extends WaitableConditionBase
{
  constructor()
  {
    super();
    this._elts = [];
  }

  push(elt)
  {
    this._elts.push(elt);
    this._setSignalled(true);
  }

  shift()
  {
    let result = this._elts.shift();
    this._setSignalled(this._elts.length !== 0);
    return result;
  }
}

let conn_cnt = 0;
class Connection
{
  constructor(sock, cache)
  {
    this.id = ++conn_cnt;
    this.sock = sock;
    this.cache = cache;
    this.fifo = new FIFO;
    sock.on("data", buffer => this.fifo.push({ type: "data", data: buffer }));
    sock.on("error", e => this.fifo.push({ type: "error", data: e }));
    sock.on("end", e => this.fifo.push({ type: "end" }));
    co(() => this.runloop());
  }

  * runloop()
  {
    let buffer = null;
    while (true)
    {
      yield this.fifo.waitSignalled();
      const e = this.fifo.shift();
      if (e.type != "data")
      {
        //console.log(this.id, ": ", e.type);
        this.sock.end();
        return;
      }

      if (!buffer)
        buffer = e.data;
      else
        buffer = Buffer.concat([ buffer, e.data ]);

//      console.log(this.id + ": ", e.type, "+", e.data.length, " len now ", buffer.length);

      // Process all requests in the buffer
      while (true)
      {
        let res = this.parseRequest(buffer);
        if (!res)
          break;

        buffer = res.buffer;
        let stop = yield co(() => this.handleRequest(res.request));
        if (stop)
        {
          this.sock.end();
          return;
        }
      }
    }
  }

  parseRequest(buffer)
  {
    let line = buffer.toString("latin1", 0, 1024);
    let cmd_match = line.match(/^([^ \r\n]*)?( .*)?\r\n/);
    if (!cmd_match)
      return;

    line = cmd_match[0];
//    console.log(this.id + ": line: ", line);

    let req = null;
    let cmd = (cmd_match[1] || "");
    let bytes = 0;
    switch (cmd)
    {
      case "version":   { req = { type: "version" }; } break;
      case "quit":      { req = { type: "quit" }; } break;
      case "set":
      case "add":
      case "replace":
      {
        let sub_match = line.match(/^([^ ]*) ([^ ]+) ([0-9]+) ([0-9]+) ([0-9]+)/);
        if (sub_match)
        {
          bytes = parseInt(sub_match[5]);
          req = { type: cmd
                , key: sub_match[2]
                , flags: parseInt(sub_match[3])
                , exptime: parseInt(sub_match[4])
                , bytes: bytes
                , noreply: !!sub_match[6]
                , buffer: buffer.slice(cmd_match[0].length, cmd_match[0].length + bytes)
                };

          if (buffer.length < cmd_match[0].length + bytes + 2)
          {
//            console.log(this.id + ": buffer too small", buffer.length, cmd_match[0].length + bytes);
            return;
          }
//          console.log("got buffer", bytes, req.buffer.length);
          bytes += 2;
        }
      } break;
      case "get":
      case "gets":
      {
        let keys = (cmd_match[2] || "").split(" ").filter(x => x);
        if (keys.length)
          req = { type: cmd, keys: keys };
      } break;
    }

    if (!req)
    {
      console.log(cmd_match);
      req = { type: "invalid_command", cmd: cmd_match[1] + (cmd_match[2] || '') };
    }

    // console.log("parsed cmd", req);

    return { buffer: buffer.slice(cmd_match[0].length + bytes), request: req };
  }

  hasKey(key)
  {

  }

  *handleRequest(req)
  {
    switch (req.type)
    {
      case "version":   { this.sock.write("VERSION 1\r\n"); } break;
      case "quit":      { return true; }
      case "set":
      case "add":
      case "replace":   { return co(() => this.handleStore(req)); }
      case "get":
      case "gets":      { return co(() => this.handleGet(req)); }
      case "invalid_command": { this.sock.write("CLIENT_ERROR invalid command: " + req.cmd + "\r\n"); } break;
      default: throw new Error("Unhandled command " + req.type);
    }
  }

  *handleStore(req)
  {
    if (req.type !== "set")
    {
      let has_key = yield Promise.resolve(this.cache.hasKey(req.key));
      if ((req.type === "add") === has_key)
      {
        if (!req.noreply)
        {
          this.sock.write("NOT_STORED\r\n");
          console.log("not stored");
        }
        return false;
      }
    }

    console.log("set", req.key);
    yield Promise.resolve(this.cache.set(req.key, { flags: req.flags, data: req.buffer }));
    if (!req.noreply)
    {
      this.sock.write("STORED\r\n");
      console.log("stored");
    }
    return false;
  }

  *handleGet(req)
  {
    console.log("get", req.keys);
    for (let i = 0; i < req.keys.length; ++i)
    {
      let key = req.keys[i];
      let res = yield Promise.resolve(this.cache.get(key));
      if (res)
      {
        console.log(" found key", key);
        this.sock.write("VALUE " + key + " " + res.flags + " " + res.data.length + "\r\n");
        this.sock.write(res.data);
        this.sock.write("\r\n");
      }
      else
        console.log(" miss key", key);
    }
    this.sock.write("END\r\n");
    return false;
  }
}

class Cache
{
  constructor()
  {
    this.data = {};

    this._hits = 0;
    this._misses = 0;
    this._retrieved = 0;
  }

  init()
  {
  }

  getStats()
  {
    return { hits: this._hits, misses: this._misses, retrieved: this._retrieved };
  }


  get(key)
  {
    return this.data[key];
  }

  set(key, value)
  {
    this.data[key] = value;
  }

  hasKey(key)
  {
    return this.data.hasOwnProperty(key);
  }
}

function runSQLiteCmd(call)
{
  return new Promise((resolve, reject) => call((err, result) =>
  {
    if (err)
      reject(new Error(err));
    else
      resolve(result);
  }));
}

class SQLCache extends Cache
{
  constructor()
  {
    super();
    this.db = new sqlite3.Database("/opt/memcache-data/files.db");
    console.log(this.db);
    this.cleans = 1;
    this.runningclean = false;

    this.lastelts = 0;
    this.lastsize = 0;
  }

  init()
  {
    return co(function*()
    {
      let start = Date.now();

      // Run in WAL mode
      yield runSQLiteCmd(cb => this.db.run("PRAGMA journal_mode = WAL", cb));
      // drop old caches
      yield runSQLiteCmd(cb => this.db.run("DROP INDEX IF EXISTS datacacheindex", cb));
      yield runSQLiteCmd(cb => this.db.run("DROP TABLE IF EXISTS cache", cb));
      // cache with data
      yield runSQLiteCmd(cb => this.db.run("CREATE TABLE IF NOT EXISTS datacache(key VARCHAR, flags INTEGER, data BLOB, len INTEGER, lastuse INTEGER)", cb));
      yield runSQLiteCmd(cb => this.db.run("CREATE INDEX IF NOT EXISTS datacacheindex ON datacache(key)", cb));
      // cache for LRU
      yield runSQLiteCmd(cb => this.db.run("CREATE TABLE IF NOT EXISTS lrucache(key VARCHAR, len INTEGER, lastuse INTEGER)", cb));
      yield runSQLiteCmd(cb => this.db.run("CREATE INDEX IF NOT EXISTS lrucacheindex ON lrucache(key)", cb));

      setInterval(() => this._cleanup(), 1000);
      console.log(`init in ${Date.now()-start}ms`);
      return "ok";
    }.bind(this));
  }

  getStats()
  {
    return { hits: this._hits, misses: this._misses, retrieved: this._retrieved };
  }

  get(key)
  {
    return co(function*()
    {
      let start = Date.now();
      console.log(" sql get ", key);
      let res = yield new Promise(resolve => this.db.all("SELECT * FROM datacache WHERE key = ?", [ key ], (err, rows) => resolve({ err, rows })));
      if (!res.rows.length)
      {
        ++this._misses;
        console.log(` sql not found ${key} in ${Date.now()-start}ms`);
        return null;
      }
      // schedule an async update of the lastuse date
      this.db.all("UPDATE lrucache SET lastuse = ? WHERE key = ?", [ Date.now(), key ], _ => _);
      if (res.rows[0].data.length != res.rows[0].len)
      {
        console.error("length failure", res.rows[0].data.length, res.rows[0].len);
        ++this._misses;
        return null;
      }
      ++this._hits;
      this._retrieved += res.rows[0].len;
      console.log(` sql found ${key} in ${Date.now()-start}ms`);
      return { flags: res.rows[0].flags, data: res.rows[0].data };
    }.bind(this));
  }

  set(key, value)
  {
    let start = Date.now();
    console.log(" sql store ", key);
    return co(function*()
    {
      // delete old key
      let now = Date.now();
      yield new Promise(resolve => this.db.run("DELETE FROM datacache WHERE key = ?", [ key ], resolve));
      yield new Promise(resolve => this.db.run("DELETE FROM lrucache WHERE key = ?", [ key ], resolve));
      yield new Promise(resolve => this.db.run("INSERT INTO datacache(key, flags, data, len, lastuse) VALUES (?,?,?,?,?)", [ key, value.flags, value.data, value.data.length, now ], resolve));
      yield new Promise(resolve => this.db.run("INSERT INTO lrucache(key, len, lastuse) VALUES (?,?,?)", [ key, value.data.length, now ], resolve));
      console.log(` sql stored ${key} in ${Date.now()-start}ms`);
      this.cleans = 2;
      return true;
    }.bind(this));
  }

  hasKey(key)
  {
    return co(function*()
    {
      let res = yield new Promise(resolve => this.db.all("SELECT key FROM datacache WHERE key = ?", [ key ], (err, rows) => resolve({ err, rows })));
      return !!res.rows.length;
    }.bind(this));
  }

  _cleanup()
  {
    if (!this.cleans || this.runningclean)
    {
      if (!this.runningclean)
        this._writeStats();
      return;
    }

    --this.cleans;
    this.runningclean = true;

    return co(function*()
    {
      let start = Date.now();
      console.log(`running cleanup query`);
      let res = yield new Promise(resolve => this.db.all("SELECT key, len, lastuse FROM lrucache ORDER BY lastuse DESC", [], (err, rows) => resolve({ err, rows })));
      if (!res.rows.length)
      {
        console.log(`empty table at cleanup in ${Date.now()-start}ms`);
        this.runningclean = false;
        this.lastelts = 0;
        this.lastsize = 0;
        return;
      }

      let maxlen = 10 * 1000 * 1000 * 1000; // about 10 GB
      let totallen = 0;
      let removelen = 0;
      let removecount = 0;
      let lastuse = null;
      for (let row of res.rows)
      {
        totallen += row.len;
        if (totallen > maxlen && !lastuse)
          lastuse = row.lastuse;
        if (lastuse)
        {
          removelen += row.len;
          ++removecount;
        }
      }

      if (lastuse)
      {
        console.log(`Going to remove ${removecount} items with ${removelen} bytes, query was ${Date.now()-start}ms`);
        yield new Promise(resolve => this.db.run("DELETE FROM datacache WHERE lastuse <= ?", [ lastuse ], _ => _));
        yield new Promise(resolve => this.db.run("DELETE FROM lrucache WHERE lastuse <= ?", [ lastuse ], _ => _));
        console.log(`done in ${Date.now()-start}ms`);
      }
      else
        console.log(`nothing removed in ${Date.now()-start}ms`);

      this.runningclean = false;
      this.lastelts = res.rows.length - removecount;
      this.lastsize = totallen - removelen;

      this._writeStats();
    }.bind(this));
  }

  _writeStats()
  {
    let stats = this.getStats();
    fs.writeFileSync("/opt/memcache-data/stats.txt",
      `SQLite3 memcached stats:

Items: ${this.lastelts}
Size: ${(this.lastsize / 1024 / 1024).toFixed(1)} MB
Hits: ${stats.hits}
Misses: ${stats.misses}
Retrieved: ${(this._retrieved / 1024 / 1024).toFixed(1)} MB
`);
  }

  close()
  {
    return new Promise(resolve => this.db.close(resolve));
  }
}

/** This cache wrapper accepts writes immediately, and delay-writes them to the wrapped
    cache
*/
class AsyncStoreWrapper extends Cache
{
  constructor(wrapped)
  {
    super();
    this.wrapped = wrapped;
    this.cache = {};
  }

  init()
  {
    return this.wrapped.init();
  }

  getStats()
  {
    let stats = this.wrapped.getStats();
    stats.hits += this._hits;
    stats.retrieved += this._retrieved;
    return stats;
  }

  get(key)
  {
    if (this.cache[key])
    {
      ++this._hits;
      this._retrieved += this.cache[key].data.len;
      return this.cache[key];
    }
    return this.wrapped.get(key);
  }

  set(key, value)
  {
    this.cache[key] = value;
    co(function*()
    {
      yield this.wrapped.set(key, value);
      delete this.cache[key];
    }.bind(this));
    return true;
  }

  hasKey(key)
  {
    if (this.cache[key])
      return true;
    return this.wrapped.hasKey(key);
  }

  close()
  {
    return this.wrapped.close();
  }
}


let server;
let cache = new AsyncStoreWrapper(new SQLCache);

console.log("initializing... (test2)");
try
{
  fs.mkdirSync("/opt/memcache-data");
}
catch(ignore)
{
  //just ensuring the directory is there
}

Promise.resolve(cache.init()).then(x =>
{
  console.log("initresult", x);
  server = net.createServer(sock => new Connection(sock, cache));
  server.listen(11211);
  console.log("listening!");
}).catch(err => { console.log(err); process.exit(); });

process.on('SIGINT', () => co(function*() {
    console.log("Caught INT signal, closing server");
    if (server)
      yield new Promise(resolve => server.close(resolve));
    console.log("Closing DB");
    yield cache.close();
    console.log("Done, exiting process");
    process.exit();
}));

process.on('SIGTERM', () => co(function*() {
    console.log("Caught TERM signal, closing server");
    if (server)
      yield new Promise(resolve => server.close(resolve));
    console.log("Closing DB");
    yield cache.close();
    console.log("Done, exiting process");
    process.exit();
}));
