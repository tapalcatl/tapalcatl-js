const path = require("path");

const tapalcatl = require(".");

const TILE_FIXTURE = "test/fixtures/tile.zip";
const HTTP_TILE_FIXTURE =
  "http://mojodna.s3.amazonaws.com/tapalcatl-fixtures/tile.zip";
const HTTPS_TILE_FIXTURE =
  "https://mojodna.s3.amazonaws.com/tapalcatl-fixtures/tile.zip";

describe("archives", () => {
  test("it tracks extensions", async () => {
    const archive = await tapalcatl(TILE_FIXTURE, "txt");

    expect(archive.extension).toBe("txt");
  });

  test("it skips extensions when omitted", async () => {
    const archive = await tapalcatl(TILE_FIXTURE);

    expect(archive.extension).toBe("");
  });
});

describe("HTTP archives", () => {
  test("it recognizes remote (HTTP) files", async () => {
    const archive = await tapalcatl(HTTP_TILE_FIXTURE);

    expect(archive.source).toEqual(HTTP_TILE_FIXTURE);
  });

  test("it recognizes remote (HTTPS) files", async () => {
    const archive = await tapalcatl(HTTPS_TILE_FIXTURE);

    expect(archive.source).toEqual(HTTPS_TILE_FIXTURE);
  });

  test("it reads metadata", async () => {
    const archive = await tapalcatl(HTTP_TILE_FIXTURE, "txt");

    expect(archive.metadata).toEqual({
      headers: [
        {
          "Content-Type": "text/plain"
        }
      ]
    });
  });

  test("it reads tiles", async () => {
    const archive = await tapalcatl(HTTP_TILE_FIXTURE, "txt");

    const { headers, body } = await archive.getTile(0, 0, 0);

    expect(body.toString()).toEqual("0\n");
  });

  test("it reads headers for tiles", async () => {
    const archive = await tapalcatl(HTTP_TILE_FIXTURE, "txt");

    const { headers, body } = await archive.getTile(0, 0, 0);

    // TODO should this be an array of objects (to support duplicate header keys) or an object?
    expect(headers).toEqual([
      {
        "Content-Type": "text/plain"
      },
      {
        "X-Something": "hello"
      }
    ]);
  });

  test("it defaults to headers from metadata", async () => {
    const archive = await tapalcatl(HTTP_TILE_FIXTURE, "txt");

    const { headers, body } = await archive.getTile(0, 0, 1);

    expect(headers).toEqual([
      {
        "Content-Type": "text/plain"
      }
    ]);
    expect(body.toString()).toEqual("1\n");
  });

  test("it returns empty data for nonexistent tiles", async () => {
    const archive = await tapalcatl(HTTP_TILE_FIXTURE, "txt");

    const tile = await archive.getTile(1, 0, 0);

    expect(tile).toBeNull;
  });
});

describe("local archives", () => {
  test("it reads local files", async () => {
    const archive = await tapalcatl(`file://${path.resolve(TILE_FIXTURE)}`);

    expect(archive.source).toMatch(/^file:\/\/\/\w/);
  });

  test("it handles paths without protocols as local files", async () => {
    const archive = await tapalcatl(TILE_FIXTURE);

    expect(archive.source).toMatch(/^file:\/\/\/\w/);
  });

  test("it reads metadata", async () => {
    const archive = await tapalcatl(TILE_FIXTURE, "txt");

    const metadata = archive.metadata;

    expect(metadata).toEqual({
      headers: [
        {
          "Content-Type": "text/plain"
        }
      ]
    });
  });

  test("it fails on malformed metadata", async () => {
    // TODO
  });

  test("it reads tiles", async () => {
    const archive = await tapalcatl(TILE_FIXTURE, "txt");

    const { headers, body } = await archive.getTile(0, 0, 0);

    expect(body.toString()).toEqual("0\n");
  });

  test("it reads headers for tiles", async () => {
    const archive = await tapalcatl(TILE_FIXTURE, "txt");

    const { headers, body } = await archive.getTile(0, 0, 0);

    // TODO should this be an array of objects (to support duplicate header keys) or an object?
    expect(headers).toEqual([
      {
        "Content-Type": "text/plain"
      },
      {
        "X-Something": "hello"
      }
    ]);
  });

  test("it defaults to headers from metadata", async () => {
    const archive = await tapalcatl(TILE_FIXTURE, "txt");

    const { headers, body } = await archive.getTile(0, 0, 1);

    expect(headers).toEqual([
      {
        "Content-Type": "text/plain"
      }
    ]);
    expect(body.toString()).toEqual("1\n");
  });

  test("it returns empty data for nonexistent tiles", async () => {
    const archive = await tapalcatl(TILE_FIXTURE, "txt");

    const tile = await archive.getTile(1, 0, 0);

    expect(tile).toBeNull();
  });
});
