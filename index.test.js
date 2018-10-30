const fs = require("fs");
const path = require("path");

const toArray = require("stream-to-array");

const tapalcatl = require(".");

const ARCHIVE_FIXTURE = "test/fixtures/4_9_9.zip";
const HTTP_ARCHIVE_FIXTURE =
  "http://mojodna.s3.amazonaws.com/tapalcatl-fixtures/4_9_9.zip";
const HTTPS_ARCHIVE_FIXTURE =
  "https://mojodna.s3.amazonaws.com/tapalcatl-fixtures/4_9_9.zip";
const S3_ARCHIVE_FIXTURE = "s3://mojodna-temp/lc/4/9/9.zip";
const TILE_FIXTURE = fs.readFileSync("test/fixtures/7_75_74.tif");
// TODO bbox should be bounds
const META_FIXTURE = require("./test/fixtures/lc_meta.json");

const DEFAULT_HEADERS = [
  {
    "Content-Type": "image/tiff"
  },
  {
    ETag: "486160910"
  },
  {
    "Last-Modified": "Tue, 30 Oct 2018 02:00:54 GMT"
  }
];

describe("HTTP archives", () => {
  let archive;

  beforeAll(async () => {
    archive = await tapalcatl(HTTP_ARCHIVE_FIXTURE);
  });

  afterAll(() => archive.close());

  test("it recognizes remote (HTTP) files", () => {
    expect(archive.source).toEqual(HTTP_ARCHIVE_FIXTURE);
  });

  test("it recognizes remote (HTTPS) files", async () => {
    const archive = await tapalcatl(HTTPS_ARCHIVE_FIXTURE);

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
    archive = await tapalcatl(`file://${path.resolve(ARCHIVE_FIXTURE)}`);
  });

  afterAll(() => archive.close());

  test("it reads local files", () => {
    expect(archive.source).toMatch(/^file:\/\/\/\w/);
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
    archive = await tapalcatl(S3_ARCHIVE_FIXTURE);
  });

  afterAll(() => archive.close());

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
