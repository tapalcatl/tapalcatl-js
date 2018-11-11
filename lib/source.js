"use strict";

const path = require("path");
const url = require("url");

const AWS = require("aws-sdk");
const fetch = require("node-fetch");
const fs = require("fs-extra");
const pImmediate = require("p-immediate");
const mercator = new (require("@mapbox/sphericalmercator"))();

const Archive = require("./archive");
const { cache } = require("./locker");

const S3 = new AWS.S3();

class Source {
  static load(source) {
    const key = `source:${source}`;
    let s = cache.get(key);

    if (s == null) {
      s = new Source(source);
      cache.set(key, s)
    }

    return s;
  }

  constructor(source) {
    this.source = source;

    this._meta = null;
    this._ready = false;
    this._initialize();
  }

  close() {}

  async _initialize() {
    this._meta = await this._loadMeta();
    this._ready = true;
  }

  async _open() {
    while (!this._ready) {
      await pImmediate();
    }
  }

  async _loadMeta() {
    const uri = url.parse(this.source);

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

  async meta() {
    await this._open();

    return this._meta;
  }

  async getTile(zoom, x, y, { format = null, scale = null } = {}) {
    await this._open();

    const {
      bounds,
      maxscale,
      maxzoom,
      materializedZooms,
      minscale,
      minzoom
    } = this._meta;
    let { metatile } = this._meta;

    metatile = metatile || 1;

    // if only 1 scale is present, default to that
    if (minscale === maxscale) {
      scale = scale || minscale || 1;
    }

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
    let mx = x >> dz;
    let my = y >> dz;
    mx -= mx % metatile;
    my -= my % metatile;

    const archive = await Archive.load(
      url.resolve(
        this.source,
        this._meta.source
          .replace("{z}", mz)
          .replace("{x}", mx)
          .replace("{y}", my)
      )
    );

    return archive.getTile(zoom, x, y, { format, scale });
  }
}

module.exports = Source;