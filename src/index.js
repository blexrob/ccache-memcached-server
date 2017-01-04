const sqlite3 = require('sqlite3').verbose();
const co = require('co');
const net = require('net');

//const file = "/tmp/node-memcache.db";
//const db = new sqlite3.Database(file);


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
      let has_key = yield Promise.resolve(this.cache.has_key(req.key));
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

/*
class Cache
{
  constructor()
  {
    this.data = {};
  }

  init()
  {
  }

  get(key)
  {
    return this.data[key];
  }

  set(key, value)
  {
    this.data[key] = value;
  }

  has_key(key)
  {
    return this.data.hasOwnProperty(key);
  }
}*/

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

class SQLCache
{
  constructor()
  {
    this.db = new sqlite3.Database("/opt/memcache-data/files.db");
    console.log(this.db);
  }

  init()
  {
    return co(function*()
    {
      yield runSQLiteCmd(cb => this.db.all("CREATE TABLE IF NOT EXISTS cache(key VARCHAR, flags INTEGER, data BLOB, len INTEGER, lastuse INTEGER)", cb));
      yield runSQLiteCmd(cb => this.db.all("CREATE INDEX IF NOT EXISTS cacheindex ON cache(key)", cb));
    }.bind(this));
  }

  get(key)
  {
    return co(function*()
    {
      let res = yield new Promise(resolve => this.db.all("SELECT * FROM cache WHERE key = ?", [ key ], (err, rows) => resolve({ err, rows })));
      if (!res.rows.length)
        return null;
      // schedule an async update of the lastuse date
      this.db.all("UPDATE cache SET lastuse = ? WHERE key = ?", [ Date.now(), key ], _ => _);
      if (res.rows[0].data.length != res.rows[0].len)
      {
        console.error("length failure", res.rows[0].data.length, res.rows[0].len);
      }
      return { flags: res.rows[0].flags, data: res.rows[0].data };
    }.bind(this));
  }

  set(key, value)
  {
    console.log("store ", key);
    return co(function*()
    {
      // delete old key
      yield new Promise(resolve => this.db.run("DELETE FROM cache WHERE key = ?", [ key ], resolve));
      yield new Promise(resolve => this.db.run("INSERT INTO cache(key, flags, data, len, lastuse) VALUES (?,?,?,?,?)", [ key, value.flags, value.data, value.data.length, Date.now() ], resolve));
      return true;
    }.bind(this));
  }

  has_key(key)
  {
    return co(function*()
    {
      let res = yield new Promise(resolve => this.db.all("SELECT key FROM cache WHERE key = ?", [ key ], (err, rows) => resolve({ err, rows })));
      return !!res.rows.length;
    }.bind(this));
  }

  cleanup()
  {
    return co(function*()
    {
      let res = yield new Promise(resolve => this.db.all("SELECT key, len FROM cache ORDER BY lastuse DESC", [], (err, rows) => resolve({ err, rows })));
      if (!res.rows.length)
      {
        // console.log(`empty table at cleanup`);
        return;
      }

      let maxlen = 3 * 1000 * 1000 * 1000; // about 3 GB
      let totallen = 0;
      let removed = 0;
      for (let row of res.rows)
      {
        totallen += row.len;
        if (totallen > maxlen)
        {
          yield new Promise(resolve => this.db.all("SELECT key FROM cache WHERE key = ?", [ row.key ], _ => _));
          removed += res.len;
        }
      }
      if (removed)
        console.log(`removed ${removed} of ${totallen}`);
    }.bind(this));
  }
}

let cache = new SQLCache;
setInterval(() => cache.cleanup(), 1000);

console.log("initializing!");
Promise.resolve(cache.init()).then(x =>
{
  console.log("initresult", x);
  let server = net.createServer(sock => new Connection(sock, cache));
  server.listen(11211);
  console.log("listening!");
}).catch(err => { console.log(err); process.exit(); });

process.on('SIGINT', function() {
    console.log("Caught interrupt signal");
    process.exit();
});

/*
console.log("schedule run");
db.run("SELECT name FROM sqlite_master WHERE type='table' AND name='ENTRIES'", x =>
{
  console.log("results", x);
});
console.log("scheduled run");


//db.run("CREATE TABLE cache (key TEXT, )");

/*

class Cache
{
  get(key):
  {

  }

  set(key, value)
  {

  }
}


const server = mc.server();

server.listen(11200, function()
{
  console.log('ready for connections from memcache clients');
});
*/


