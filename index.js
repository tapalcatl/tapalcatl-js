const path = require("path");
const url = require("url");
const util = require("util");

const yauzl = require("yauzl");

const Archive = require("./lib/archive");
const httpReader = require("./lib/http_reader");
const s3Reader = require("./lib/s3_reader");

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

module.exports = async (source) => {
  const uri = url.parse(source);

  switch (uri.protocol) {
    case "file:": {
      const zip = await open(uri.pathname);
      return new Archive(source, zip);
    }

    case "http:":
    case "https:": {
      const { reader, size } = await httpReader(source);
      const zip = await open(source, reader, size);
      return new Archive(source, zip);
    }

    case "s3:": {
      const { reader, size } = await s3Reader(source);
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
};
