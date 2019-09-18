const path = require("path");

const fs = require("fs-extra");
const toArray = require("stream-to-array");

const WritableArchive = require("./lib/writable_archive");
const tapalcatl = require(".");

const ARCHIVE_FIXTURE = "test/fixtures/4_9_9.zip";
const HTTP_ARCHIVE_FIXTURE =
  "http://mojodna.s3.amazonaws.com/tapalcatl-fixtures/4_9_9.zip";
const HTTPS_ARCHIVE_FIXTURE =
  "https://mojodna.s3.amazonaws.com/tapalcatl-fixtures/4_9_9.zip";
const S3_ARCHIVE_FIXTURE = "s3://mojodna/tapalcatl-fixtures/4_9_9.zip";
const TILE_FILENAME = "test/fixtures/7_75_74.tif";
const TILE_FIXTURE = fs.readFileSync(TILE_FILENAME);
const META_FIXTURE = require("./test/fixtures/lc_meta.json");

const DEFAULT_HEADERS = [
  {
    "Content-Type": "image/tiff"
  },
  {
    ETag: "486160910"
  },
  {
    "Last-Modified": "Thu, 01 Nov 2018 02:10:26 GMT"
  }
];

describe("HTTP archives", () => {
  let archive;

  beforeAll(async () => {
    archive = await tapalcatl.archive(HTTP_ARCHIVE_FIXTURE);
  });

  test("it recognizes remote (HTTP) files", () => {
    expect(archive.source).toEqual(HTTP_ARCHIVE_FIXTURE);
  });

  test("it recognizes remote (HTTPS) files", async () => {
    const archive = await tapalcatl.archive(HTTPS_ARCHIVE_FIXTURE);

    expect(archive.source).toEqual(HTTPS_ARCHIVE_FIXTURE);
  });

  test("it reads metadata", () => {
    expect(archive.metadata).toEqual(META_FIXTURE);
  });

  test("it reads tiles", async () => {
    const { body } = await archive.getTile(7, 75, 74);

    const buf = Buffer.concat(await toArray(body));

    expect(buf).toEqual(TILE_FIXTURE);
  });

  test("it reads headers for tiles", async () => {
    const { headers } = await archive.getTile(7, 75, 75);

    expect(headers).toEqual([
      DEFAULT_HEADERS[0],
      {
        ETag: "3656453970"
      },
      DEFAULT_HEADERS[2]
      // TODO update the fixture to include additional headers
      // {
      //   "X-Something": "hello"
      // }
    ]);
  });

  test("it defaults to headers from metadata", async () => {
    const { headers } = await archive.getTile(7, 75, 74);

    expect(headers).toEqual(DEFAULT_HEADERS);
  });

  test("it returns empty data for nonexistent tiles", async () => {
    const tile = await archive.getTile(0, 0, 0);

    expect(tile).toBeNull;
  });
});

describe("local archives", () => {
  let archive;

  beforeAll(async () => {
    archive = await tapalcatl.archive(
      `file://${path.resolve(ARCHIVE_FIXTURE)}`
    );
  });

  test("it reads local files", () => {
    expect(archive.source).toMatch(/^file:\/\/\/\w/);
  });

  test("it reads local files with relative paths", async () => {
    const uri = `file://./${ARCHIVE_FIXTURE}`;

    const archive = await tapalcatl.archive(uri);

    expect(archive.source).toEqual(uri);
  });

  test("it handles paths without protocols as local files", () => {
    expect(archive.source).toMatch(/^file:\/\/\/\w/);
  });

  test("it reads metadata", async () => {
    const metadata = archive.metadata;

    expect(metadata).toEqual(META_FIXTURE);
  });

  test("it fails on malformed metadata", async () => {
    // TODO
  });

  test("it reads tiles", async () => {
    const { body } = await archive.getTile(7, 75, 74);

    const buf = Buffer.concat(await toArray(body));

    expect(buf).toEqual(TILE_FIXTURE);
  });

  test("it reads headers for tiles", async () => {
    const { headers } = await archive.getTile(7, 75, 75);

    expect(headers).toEqual([
      DEFAULT_HEADERS[0],
      { ETag: "3656453970" },
      DEFAULT_HEADERS[2]
    ]);
    // TODO update the fixture to include additional headers
    // {
    //   "X-Something": "hello"
    // }
  });

  test("it defaults to headers from metadata", async () => {
    const { headers } = await archive.getTile(7, 75, 74);

    expect(headers).toEqual(DEFAULT_HEADERS);
  });

  test("it returns empty data for nonexistent tiles", async () => {
    const tile = await archive.getTile(0, 0, 0);

    expect(tile).toBeNull();
  });
});

describe("S3 archives", () => {
  let archive;

  beforeAll(async () => {
    archive = await tapalcatl.archive(S3_ARCHIVE_FIXTURE);
  });

  test("it recognizes remote (S3) archives", () => {
    expect(archive.source).toEqual(S3_ARCHIVE_FIXTURE);
  });

  test("it reads metadata", () => {
    expect(archive.metadata).toEqual(META_FIXTURE);
  });

  test("it reads tiles", async () => {
    const { body } = await archive.getTile(7, 75, 74);

    const buf = Buffer.concat(await toArray(body));

    expect(buf).toEqual(TILE_FIXTURE);
  });

  test("it reads headers for tiles", async () => {
    const { headers } = await archive.getTile(7, 75, 74);

    expect(headers).toEqual(DEFAULT_HEADERS);
  });

  test("it returns empty data for nonexistent tiles", async () => {
    const tile = await archive.getTile(0, 0, 0);

    expect(tile).toBeNull;
  });
});

describe("Source", () => {
  const source = tapalcatl("s3://mojodna-temp/lc/meta.json");

  describe("meta", () => {
    let meta;

    beforeAll(async () => {
      meta = await source.meta();
    });

    test("meta includes a name", async () => {
      expect(meta.name).toEqual("Land Cover");
    });
  });

  test("getTile", async () => {
    const { body } = await source.getTile(7, 75, 74);

    const buf = Buffer.concat(await toArray(body));

    expect(buf).toEqual(TILE_FIXTURE);
  });

  test("getTile out of zoom range", async () => {
    const tile = await source.getTile(10, 75, 74);

    expect(tile).toBe(null);
  });

  test("getTile out of bounds", async () => {
    const tile = await source.getTile(4, -10, 10);

    expect(tile).toBe(null);
  });
});

describe("Writable Archive", () => {
  const archiveName = "./test/test.zip";
  let archive;

  beforeAll(async () => {
    // TODO how do options fit in?
    const writableArchive = new WritableArchive(4, 9, 9, `file://${archiveName}`, {
      formats: {
        "tif": "image/tiff"
      },
      maxzoom: 7
    });

    writableArchive.addTile(7, 75, 74, TILE_FIXTURE);
    fs.createReadStream(TILE_FILENAME).pipe(writableArchive.addTile(7, 75, 75));
    writableArchive.addTile(7, 75, 76, fs.createReadStream(TILE_FILENAME));
    writableArchive.addTile(7, 75, 77, "tile");
    writableArchive.addTile(7, 75, 78, {
      tile: true
    });

    await writableArchive.close();

    archive = await tapalcatl.archive(archiveName);
  });

  afterAll(async () => {
    await fs.unlink(archiveName);
  });

  describe("metadata", () => {
    test("requires at least one format", () => {
      try {
        new WritableArchive(4, 9, 9, `file:///tmp/tapalcatl.zip`, {
        });
      } catch (err) {
        return;
      }

      fail();
    })

    test("defaults `minzoom` to the root", () => {
      expect(archive.metadata.minzoom).toEqual(4);
    });

    test("sets `root` to the root coordinates", () => {
      expect(archive.metadata.root).toEqual("4/9/9");
    })

    test("sets `formats`", () => {
      expect(archive.metadata.formats).toEqual({
        "tif": "image/tiff"
      });
    });

    test("minzoom must be <= maxzoom", () => {
      try {
        new WritableArchive(4, 9, 9, `file:///tmp/tapalcatl.zip`, {
          formats: {
            "tif": "image/tiff"
          },
          minzoom: 4,
          maxzoom: 3
        });
      } catch (err) {
        return;
      }

      fail();
    })
  });

  describe("addTile", () => {
    test("zoom must be within range", () => {
      const writableArchive = new WritableArchive(4, 9, 9, `file:///tmp/tapalcatl.zip`, {
        formats: {
          "tif": "image/tiff"
        },
        maxzoom: 7
      });

      try {
        writableArchive.addTile(3, 0, 0, TILE_FIXTURE);
      } catch (err) {
        return;
      }

      fail();
    })

    test("coordinates must be within range", () => {
      const writableArchive = new WritableArchive(4, 9, 9, `file:///tmp/tapalcatl.zip`, {
        formats: {
          "tif": "image/tiff"
        },
        maxzoom: 7
      });

      try {
        writableArchive.addTile(4, 8, 9, TILE_FIXTURE);
      } catch (err) {
        return;
      }

      fail();
    })
  })

  test("creates a zip", async () => {
    expect(await fs.exists("./test/test.zip")).toBe(true);
  });

  test("creates an archive", () => {
    expect(archive).not.toBeNull();
  });

  test("includes a tile written as a Buffer", async () => {
    const { body } = await archive.getTile(7, 75, 74);

    const buf = Buffer.concat(await toArray(body));

    expect(buf).toEqual(TILE_FIXTURE);
  })

  test("includes a tile piped from a ReadableStream", async () => {
    const { body } = await archive.getTile(7, 75, 75);

    const buf = Buffer.concat(await toArray(body));

    expect(buf).toEqual(TILE_FIXTURE);
  })

  test("includes a tile written as a ReadableStream", async () => {
    const { body } = await archive.getTile(7, 75, 76);

    const buf = Buffer.concat(await toArray(body));

    expect(buf).toEqual(TILE_FIXTURE);
  })

  test("includes a tile written as a string", async () => {
    const { body } = await archive.getTile(7, 75, 77);

    const buf = Buffer.concat(await toArray(body));

    expect(buf.toString()).toEqual("tile");
  })

  test("includes a tile written as an object", async () => {
    const { body } = await archive.getTile(7, 75, 78);

    const buf = Buffer.concat(await toArray(body));

    expect(JSON.parse(buf.toString())).toEqual({
      tile: true
    });
  })
});

// describe("Writable Source", () => {

// });