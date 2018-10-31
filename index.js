const path = require("path");
const url = require("url");
const util = require("util");

const AWS = require("aws-sdk");
const fs = require("fs-extra");
const lockingCache = require("locking-cache");
const pImmediate = require("p-immediate");
const mercator = new (require("@mapbox/sphericalmercator"))();
const yauzl = require("yauzl");

const Archive = require("./lib/archive");
const httpReader = require("./lib/http_reader");
const s3Reader = require("./lib/s3_reader");

const S3 = new AWS.S3();

const lockedLoad = lockingCache({
  max: 1000,
  dispose: async (key, [archive]) => archive.close()
});

process.on("exit", code => {
  lockedLoad.cache.forEach(v => v[0].close());
});

const openZip = util.promisify(yauzl.open.bind(yauzl));
const fromRandomAccessReader = util.promisify(
  yauzl.fromRandomAccessReader.bind(yauzl)
);

async function open(filename, randomAccessReader = null, size = null) {
  if (randomAccessReader != null && size != null) {
    return fromRandomAccessReader(randomAccessReader, size, {
      autoClose: false
    });
  }

  return openZip(filename, {
    autoClose: false
  });
}

async function _loadArchive(source, blockSize) {
  const uri = url.parse(source);

  switch (uri.protocol) {
    case "file:": {
      const zip = await open(
        path.resolve(path.join(uri.hostname, uri.pathname))
      );
      return new Archive(source, zip);
    }

    case "http:":
    case "https:": {
      const { reader, size } = await httpReader(source, blockSize);
      const zip = await open(source, reader, size);
      return new Archive(source, zip);
    }

    case "s3:": {
      const { reader, size } = await s3Reader(source, blockSize);
      const zip = await open(source, reader, size);
      return new Archive(source, zip);
    }

    case null: {
      const zip = await open(source);
      return new Archive(`file://${path.resolve(source)}`, zip);
    }

    default:
      throw new Error(`${uri.protocol} is an unsupported protocol.`);
  }
}

async function _loadMeta(source) {
  const uri = url.parse(source);

  switch (uri.protocol) {
    case "file:":
      return await fs.readJSON(
        path.resolve(path.join(uri.hostname, uri.pathname)),
        "utf-8"
      );

    case "http:":
    case "https:":
      return (await fetch(source)).json();

    case "s3:":
      const obj = await S3.getObject({
        Bucket: uri.hostname,
        Key: uri.pathname.slice(1)
      }).promise();

      return JSON.parse(obj.Body.toString());

    case null:
      return JSON.parse(await fs.readFile(path.resolve(source), "utf-8"));

    default:
      throw new Error(`${uri.protocol} is an unsupported protocol.`);
  }
}

const loadArchive = util.promisify(
  lockedLoad((source, ...params) => {
    const lock = params.pop();
    const blockSize = params.pop();

    return lock(source, async unlock => {
      try {
        return unlock(null, await _loadArchive(source, blockSize));
      } catch (err) {
        return unlock(err);
      }
    });
  })
);

const loadMeta = util.promisify(
  lockedLoad((source, lock) => {
    return lock(source, async unlock => {
      try {
        return unlock(null, await _loadMeta(source));
      } catch (err) {
        console.log("failed", err);
        return unlock(err);
      }
    });
  })
);

class Source {
  constructor(source) {
    this.source = source;

    this._meta = null;
    this._ready = false;
    this._initialize();
  }

  async _initialize() {
    this._meta = await loadMeta(this.source);
    this._ready = true;
  }

  async _open() {
    while (!this._ready) {
      await pImmediate();
    }
  }

  async meta() {
    await this._open();

    return this._meta;
  }

  async getTile(zoom, x, y, format = null) {
    await this._open();

    const { bounds, maxzoom, materializedZooms, minzoom } = this._meta;

    // validate zooms
    if (zoom < minzoom || zoom > maxzoom) {
      return null;
    }

    // validate coords against bounds
    var xyz = mercator.xyz(bounds, zoom);

    if (x < xyz.minX || x > xyz.maxX || y < xyz.minY || y > xyz.maxY) {
      return null;
    }

    // calculate source archive coordinates
    const mz = materializedZooms
      // prevent mutation
      .slice()
      .reverse()
      .find(z => z <= zoom);
    const dz = zoom - mz;
    const mx = x >> dz;
    const my = y >> dz;

    const archive = await loadArchive(
      this._meta.source
        .replace("{z}", mz)
        .replace("{x}", mx)
        .replace("{y}", my)
    );

    return archive.getTile(zoom, x, y, format);
  }
}

module.exports = source => {
  return new Source(source);
};

module.exports.archive = loadArchive;
