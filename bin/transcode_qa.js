#!/usr/bin/env node
// given: bounds, zoom range, materialized zooms, metatile (, format, scale)

const assert = require("assert");
const path = require("path");
const url = require("url");
const util = require("util");

const AWS = require("aws-sdk");
const fs = require("fs-extra");
const mercator = new (require("@mapbox/sphericalmercator"))();
const morton = require("@thi.ng/morton");
const PQueue = require("p-queue");
var tilelive = require("@mapbox/tilelive");
require("tilelive-modules/loader")(tilelive);

const S3 = new AWS.S3();
const WritableArchive = require("../lib/writable_archive");

function* archives(
  materializedZooms,
  minZoom,
  maxZoom,
  { bounds = [-180, -90, 180, 90], metatile = 1 } = {}
) {
  assert.ok(
    Math.ceil(Math.log2(metatile)) === Math.floor(Math.log2(metatile)),
    "Metatile must be a power of 2."
  );

  for (let i = 0; i < materializedZooms.length; i++) {
    const mz = materializedZooms[i];

    let zoom = mz;

    // if (mz <= minZoom) {
    //   zoom = minZoom;
    // }

    let nextZoom;

    if (materializedZooms[i + 1] != null) {
      nextZoom = materializedZooms[i + 1] - 1;
    } else {
      nextZoom = maxZoom;
    }

    const { minX, minY, maxX, maxY } = mercator.xyz(bounds, zoom);

    const minZK = morton.mux2(
      minX - (minX % metatile),
      minY - (minY % metatile)
    );
    // TODO this is wrong (@ z6)
    const maxZK = morton.mux2(
      maxX + metatile - (maxX % metatile) - 1,
      maxY + metatile - (maxY % metatile) - 1
    );
    for (let zk = minZK; zk <= maxZK; zk += metatile ** 2) {
      const [ax, ay] = morton.demux2(zk);
      const dz = Math.max(minZoom, zoom) - zoom;

      yield {
        // bounds: mercator.bbox(ax, ay, zoom),
        // TODO constrain bounds to min(bounds, bbox())
        bounds,
        metatile,
        x: ax,
        y: ay,
        zoom,
        minzoom: Math.max(minZoom, zoom),
        maxzoom: nextZoom,
        tiles: tiles.bind(null, zoom + dz, ax << dz, ay << dz, nextZoom, {
          // TODO constrain bounds to min(bounds, bbox())
          bounds: bounds,
          metatile: metatile << dz
        })
      };
    }
  }
}

function* tiles(
  zoom,
  x,
  y,
  maxZoom,
  { bounds = [-180, -90, 180, 90], metatile = 1 } = {}
) {
  assert.ok(
    Math.ceil(Math.log2(metatile)) === Math.floor(Math.log2(metatile)),
    "Metatile must be a power of 2."
  );
  assert.ok(x % metatile === 0, "Root tile must be a meta tile");
  assert.ok(y % metatile === 0, "Root tile must be a meta tile");

  for (let z = zoom; z <= maxZoom; z++) {
    // calculate the Δ between the current and root zooms
    const dz = z - zoom;

    // calculate the top-left corner of the tiles at the current zoom
    const minX = x << dz;
    const minY = y << dz;

    // calculate the z key for the top-left corner
    const minZK = morton.mux2(minX, minY);

    // calculate the z key for the bottom-right corner (constrained to the available tiles)
    const maxZK = Math.min(4 ** z, minZK + 4 ** dz * metatile ** 2);

    // get the tile bounds for the target geographic bounds
    const xyz = mercator.xyz(bounds, z);

    // yield all tiles from the current zoom
    for (let zk = minZK; zk < maxZK; zk++) {
      const [ax, ay] = morton.demux2(zk);

      // skip out of bounds coordinates
      if (
        xyz.minX <= ax &&
        ax <= xyz.maxX &&
        xyz.minY <= ay &&
        ay <= xyz.maxY
      ) {
        yield [z, ax, ay];
      }
    }
  }
}

async function populateArchive(getTile, target, archive, formats, metatile) {
  if (target.endsWith("/")) {
    target = target.slice(0, -1);
  }

  console.log(`${archive.zoom}/${archive.x}/${archive.y}`);
  const arc = new WritableArchive(
    archive.zoom,
    archive.x,
    archive.y,
    `${target}/${archive.zoom}/${archive.x}/${archive.y}.zip`,
    {
      minzoom: archive.minzoom,
      maxzoom: archive.maxzoom,
      formats,
      metatile
    }
  );

  for (const tile of archive.tiles()) {
    // console.log(tile)
    const [z, x, y] = tile;

    try {
      const [body, headers] = await getTile(z, x, y);
      // TODO diff headers against source info to see if anything is distinct
      // capture common headers and write into archive (and metadata, which means writing it later)
      arc.addTile(z, x, y, body, {
        compress: false
      });
    } catch (err) {
      if (err.message !== "Tile does not exist") {
        throw err;
      }

      if (z > archive.minzoom && arc.entryCount === 0) {
        // no tiles were added at the root zoom; assume that this means no tiles are present for the entire sub-pyramid
        break;
      }
    }
  }

  await arc.close();
}

async function main() {
  // const materializedZooms = [8, 12];
  const materializedZooms = [6];
  // const materializedZooms = [8];
  // const minZoom = 8;
  // const maxZoom = 15;
  // const metatile = 4;
  const metatile = 1;
  // const bounds = [-123, 36, -120, 39];
  // const source = "mbtiles:///Users/seth/src/mojodna/tapalcatl-js/isle_of_man.mbtiles";
  const argv = process.argv.slice(2);

  if (argv.length !== 2) {
    throw new Error("Unsupported arguments");
  }

  const source = argv.shift();
  const target = argv.shift();

  const load = util.promisify(tilelive.load);

  const src = await load(source);

  // const target = "file://" + path.resolve("qa");

  src.getTile[util.promisify.custom] = (zoom, x, y) => {
    return new Promise((resolve, reject) => {
      return src.getTile(zoom, x, y, (err, body, headers) => {
        if (err) {
          return reject(err);
        }

        return resolve([body, headers]);
      });
    });
  };
  const getTile = util.promisify(src.getTile);
  const getInfo = util.promisify(src.getInfo.bind(src));

  // strip out undesired TileJSON-ish properties
  const { scheme, basename, filesize, format, type, ...info } = await getInfo();
  // TODO allow these to be overridden
  let { bounds, minzoom, maxzoom } = info;
  // bounds = [-4.8302, 54.0446, -4.3101, 54.4188];
  // bounds = [-4.921875, 53.9560855309879, -4.21875, 54.521081495443596];
  // minzoom = 12;
  // maxzoom = 12;

  let formats = null;

  switch (format) {
    case "pbf":
      formats = {
        mvt: [
          {
          "Content-Type": "application/vnd.mapbox-vector-tile"
          },
          {
          "Content-Encoding": "gzip"
          }
        ]
      };
      break;

    default:
      throw new Error(`Unsupported format: ${format}`);
  }

  const meta = {
    ...info,
    tapalcatl: "2.0.0",
    materializedZooms,
    metatile,
    minzoom,
    maxzoom,
    formats,
    source: "{z}/{x}/{y}.zip"
  };

  const uri = url.parse(target);
  switch (uri.protocol) {
    case "file:":
      const dirname = path.join(uri.hostname || "", uri.pathname.slice(1));
      await fs.ensureDir(dirname);
      await fs.writeJSON(path.join(dirname, "meta.json"), meta);
      break;

    case "s3:":
      const bucket = uri.hostname;
      const key = path.join(uri.pathname.slice(1), "meta.json");

      await S3.putObject({
        Bucket: bucket,
        Key: key,
        Body: JSON.stringify(meta),
        ContentType: "application/json"
      }).promise();
      break;

    default:
      throw new Error(`Unsupported protocol: ${uri.protocol}`);
  }

  const queue = new PQueue({
    concurrency: 16
  });

  for (const archive of archives(materializedZooms, minzoom, maxzoom, {
    bounds,
    metatile
  })) {
    await queue.onEmpty();

    queue.add(populateArchive.bind(null, getTile, target, archive, formats, metatile));
  }

  await queue.onIdle();
}

main()
  .then(process.exit)
  .catch(err => {
    console.error(err.stack);
    process.exit(1);
  });
