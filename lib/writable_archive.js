"use strict";

const assert = require("assert");
const path = require("path");
const { PassThrough, Readable } = require("stream");
const url = require("url");

const AWS = require("aws-sdk");
const fs = require("fs-extra");
const pEvent = require("p-event");
const pImmediate = require("p-immediate");
const { ZipFile } = require("yazl");

const S3 = new AWS.S3();

async function createWriteStream(target) {
  const uri = url.parse(target);

  switch (uri.protocol) {
    case "file:":
      const filename = path.resolve(path.join(uri.hostname || "", uri.pathname.slice(1)));

      await fs.ensureDir(path.dirname(filename));

      return fs.createWriteStream(filename);

    case "s3:":
      const pt = new PassThrough();
      pt.url = target;

      S3.upload({
        Bucket: uri.hostname,
        Key: uri.pathname.slice(1),
        Body: pt,
        ContentType: "application/zip"
      }, err => pt.destroy(err));

      return pt;

    default:
      throw new Error(`Unsupported protocol: ${uri.protocol}`);
  }
}

class WritableArchive {
  constructor(zoom, x, y, target, metadata) {
    this.target = target;

    assert.ok(metadata.formats != null);

    this.metadata = {
      minzoom: zoom,
      maxzoom: zoom,
      root: `${zoom}/${x}/${y}`,
      ...metadata
    };

    this._zoom = zoom;
    this._x = x;
    this._y = y;

    assert.ok(this.metadata.minzoom <= this.metadata.maxzoom);

    this._initialize();
  }

  async _initialize() {
    this._closed = true;
    this.entryCount = 0;
    this._ready = false;

    this._zip = new ZipFile();

    // this._writeStream = await createWriteStream(this.target);
    // this._writeStream.on("close", () => (this._closed = true));

    // this._zip.outputStream.pipe(this._writeStream);

    this._ready = true;
  }

  async _close() {
    while (!this._closed) {
      await pImmediate();
    }
  }

  async ready() {
    while (!this._ready) {
      await pImmediate();
    }
  }

  async addTile(z, x, y, data, { compress = true, format = null, headers = null, scale } = {}) {
    const {
      formats,
      metatile,
      minscale,
      maxscale,
      minzoom,
      maxzoom,
      root
    } = this.metadata;

    if (!(minzoom <= z && z <= maxzoom)) {
      throw new Error(
        `Tile zoom out of range: ${z} must be between ${minzoom} and ${maxzoom}`
      );
    }

    const dz = z - this._zoom;
    const bx = x >> dz;
    const by = y >> dz;

    if (
      !(
        this._x <= bx &&
        bx <= this._x + metatile - 1 &&
        this._y <= by &&
        by <= this._y + metatile - 1
      )
    ) {
      throw new Error(
        `Tile coordinate out of range: ${z}/${x}/${y} must be contained by ${root} (where metatile == ${metatile}) and is contained by ${
          this._zoom
        }/${bx}/${by} instead`
      );
    }

    // TODO centralize filename logic w/ archive
    if (format == null && Object.keys(formats).length === 1) {
      // only 1 format is defined; use that
      format = Object.keys(this.metadata.formats).pop();
    }

    if (minscale === maxscale) {
      scale = scale || minscale || 1;
    }

    let filename = `${z}/${x}/${y}`;

    if (scale > 1) {
      filename += `@${scale}x`;
    }

    if (format != "") {
      // allow format to be ""
      filename += `.${format}`;
    }

    this.entryCount++;

    const options = {
      // TODO don't compress when Content-Encoding: gzip; stored versions get bigger
      compress
    };

    if (headers != null) {
      // TODO consider taking Last-Modified and using it as options.mtime
      options.fileComment = JSON.stringify(headers);
    }

    if (this._writeStream == null) {
      this._closed = false;
      this._writeStream = await createWriteStream(this.target);
      this._writeStream.on("close", () => (this._closed = true));

      this._zip.outputStream.pipe(this._writeStream);
    }

    if (data == null) {
      const p = new PassThrough();

      this._zip.addReadStream(p, filename, options);

      return p;
    }

    if (data instanceof Buffer) {
      return this._zip.addBuffer(data, filename, options);
    }

    if (data instanceof Readable) {
      return this._zip.addReadStream(data, filename, options);
    }

    if (typeof data === "object") {
      return this._zip.addBuffer(
        Buffer.from(JSON.stringify(data)),
        filename,
        options
      );
    }

    return this._zip.addBuffer(Buffer.from(data), filename, options);
  }

  async close() {
    await this.ready();

    this._zip.end({
      comment: JSON.stringify(this.metadata)
    });

    await this._close();
  }
}

module.exports = WritableArchive;
