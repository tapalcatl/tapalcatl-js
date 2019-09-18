#!/usr/bin/env node
// given: bounds, zoom range, materialized zooms, metatile (, format, scale)

const assert = require("assert");
const path = require("path");
const url = require("url");
const util = require("util");

const fs = require("fs-extra");
const MBTiles = require("@mapbox/mbtiles");
const mercator = new (require("@mapbox/sphericalmercator"))();
const morton = require("@thi.ng/morton");

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

    if (mz <= minZoom) {
      zoom = minZoom;
    }

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
    const maxZK = morton.mux2(
      maxX + metatile - (maxX % metatile),
      maxY + metatile - (maxY % metatile)
    );
    for (let zk = minZK; zk <= maxZK; zk += metatile ** 2) {
      const [ax, ay] = morton.demux2(zk);

      yield {
        bounds: mercator.bbox(ax, ay, zoom),
        metatile,
        x: ax,
        y: ay,
        zoom,
        maxZoom: nextZoom,
        tiles: tiles.bind(null, zoom, ax, ay, nextZoom, {
          metatile
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

async function main() {
  const materializedZooms = [8, 12];
  // const minZoom = 8;
  // const maxZoom = 15;
  const metatile = 4;
  // const metatile = 1;
  // const bounds = [-123, 36, -120, 39];

  return new Promise((resolve, reject) => {
    new MBTiles(
      "/Users/seth/Documents/typography.mbtiles?mode=ro",
      async function(err, mbtiles) {
        if (err) {
          return reject(err);
        }

        const target = "file://" + path.resolve("out");

        mbtiles.getTile[util.promisify.custom] = (zoom, x, y) => {
          return new Promise((resolve, reject) => {
            return mbtiles.getTile(zoom, x, y, (err, body, headers) => {
              if (err) {
                return reject(err);
              }

              return resolve([body, headers]);
            });
          });
        };
        const getTile = util.promisify(mbtiles.getTile);
        const getInfo = util.promisify(mbtiles.getInfo.bind(mbtiles));

        // strip out undesired TileJSON-ish properties
        const { scheme, basename, filesize, ...info } = await getInfo();
        // TODO allow these to be overridden
        let { bounds, minzoom, maxzoom } = info;

        const meta = {
          ...info,
          tapalcatl: "2.0.0",
          materializedZooms,
          metatile,
          formats: {
            png: "image/png"
          },
          source: "{z}/{x}/{y}.zip"
        };

        const uri = url.parse(target);
        switch (uri.protocol) {
          case "file:":
            const dirname = path.join(uri.hostname || "", uri.pathname);
            await fs.ensureDir(dirname);
            await fs.writeJSON(path.join(dirname, "meta.json"), meta);
            break;

          default:
            throw new Error(`Unsupported protocol: ${uri.protocol}`);
        }

        for (const archive of archives(materializedZooms, minzoom, maxzoom, {
          bounds,
          metatile
        })) {
          console.log(`${archive.zoom}/${archive.x}/${archive.y}`);
          const arc = new WritableArchive(
            archive.zoom,
            archive.x,
            archive.y,
            path.join(target, `${archive.zoom}/${archive.x}/${archive.y}.zip`),
            {
              minzoom: minzoom,
              maxzoom: archive.maxZoom,
              formats: {
                png: "image/png"
              },
              metatile
            }
          );

          for (const tile of archive.tiles()) {
            const [z, x, y] = tile;

            try {
              const [body, headers] = await getTile(z, x, y);
              // console.log(tile);
              // console.log(tile, body, headers);
              // TODO diff headers against source info
              arc.addTile(z, x, y, body);
            } catch (err) {
              if (err.message !== "Tile does not exist") {
                return reject(err);
              }

              if (z > archive.zoom && arc.entryCount === 0) {
                // no tiles were added at the root zoom; assume that this means no tiles are present for the entire sub-pyramid
                break;
              }
            }
          }

          arc.close();
        }

        return resolve();
      }
    );
  });
}

main()
  .then(process.exit)
  .catch(err => {
    console.error(err.stack);
    process.exit(1);
  });
