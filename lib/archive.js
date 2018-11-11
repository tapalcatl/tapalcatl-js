"use strict";

const path = require("path");
const url = require("url");
const util = require("util");

const httpReader = require("./http_reader");
const locker = require("./locker");
const pImmediate = require("p-immediate");
const s3Reader = require("./s3_reader");
const yauzl = require("yauzl");

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

const loadArchive = util.promisify(
  locker((source, ...params) => {
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

class Archive {
  static async load(source) {
    return loadArchive(source);
  }

  constructor(source, zip) {
    this.entries = {};
    this.metadata = {
      headers: []
    };
    this.source = source;

    // private
    this._ready = false;
    this._zip = zip;

    this._initialize();
  }

  _initialize() {
    const zip = this._zip;

    zip.on("entry", entry => {
      this.entries[entry.fileName] = entry;
    });

    zip.on("end", () => (this._ready = true));

    if (zip.comment != null) {
      try {
        this.metadata = JSON.parse(zip.comment);
      } catch (err) {
        console.warn(`Invalid metadata: "${zip.comment}"\n`, err.stack);
      }
    }
  }

  async _open() {
    while (!this._ready) {
      await pImmediate();
    }
  }

  close() {
    this._zip.close();
  }

  // TODO variant
  async getTile(zoom, x, y, { format = null, scale = null } = {}) {
    const { formats, maxscale, minscale } = this.metadata;

    if (format == null && Object.keys(formats).length === 1) {
      // only 1 format is defined; use that
      format = Object.keys(this.metadata.formats).pop();
    }

    if (minscale === maxscale) {
      scale = scale || minscale || 1;
    }

    let filename = `${zoom}/${x}/${y}`;

    if (scale > 1) {
      filename += `@${scale}x`;
    }

    if (format != "") {
      // allow format to be ""
      filename += `.${format}`;
    }

    await this._open();

    const entry = this.entries[filename];

    if (entry == null) {
      return null;
    }

    let headers = [];

    if (entry.fileComment !== "") {
      try {
        headers = JSON.parse(entry.fileComment);
      } catch (err) {
        console.warn(`Invalid tile headers: "${entry.fileComment}"`, err.stack);
      }
    }

    const rs = await util.promisify(this._zip.openReadStream.bind(this._zip))(
      entry
    );

    let formatHeaders = this.metadata.formats[format];

    if (typeof formatHeaders === "string") {
      formatHeaders = [
        {
          "Content-Type": formatHeaders
        }
      ];
    }

    const derivedHeaders = [
      {
        ETag: entry.crc32.toString()
      },
      {
        "Last-Modified": entry.getLastModDate().toUTCString()
      }
    ];

    return {
      headers: [...formatHeaders, ...headers, ...derivedHeaders],
      body: rs
    };
  }
}

module.exports = Archive;
