const { PassThrough } = require("stream");

const fetch = require("node-fetch");
const { RandomAccessReader } = require("yauzl");

class HTTPReader extends RandomAccessReader {
  constructor(source) {
    super();
    this.source = source;
  }

  close() {
    super.close();
    // TODO delete cached content
    console.log("close");
  }

  async fetchInto(start, end, rs) {
    try {
      const rsp = await fetch(this.source, {
        headers: {
          Range: `bytes=${start}-${end - 1}`
        }
      });

      rsp.body.pipe(rs);
    } catch (err) {
      rs.emit("error", err);
    }
  }

  _readStreamForRange(start, end) {
    // console.log(`${start} - ${end}`)
    const rs = new PassThrough();

    this.fetchInto(start, end, rs);

    return rs;
  }
}

module.exports = async source => {
  const rsp = await fetch(source, {
    method: "HEAD"
  });

  if (rsp.status !== 200) {
    throw new Error(`ENOENT: no such file or directory, open '${uri}'`);
  }

  return {
    reader: new HTTPReader(source),
    size: Number(rsp.headers.get("content-length"))
  };
};
