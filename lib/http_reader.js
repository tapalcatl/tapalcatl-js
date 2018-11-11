"use strict";

const fetch = require("node-fetch");

const BlockReader = require("./block_reader");

class HTTPReader extends BlockReader {
  constructor(source, blockSize) {
    super(blockSize);
    this.source = source;
  }

  cacheKey(blockNumber) {
    return `${this.source}#${blockNumber}`;
  }

  async readBlock(blockStart, blockEnd) {
    const rsp = await fetch(this.source, {
      headers: {
        Range: `bytes=${blockStart}-${blockEnd}`
      }
    });

    return rsp.buffer();
  }
}

module.exports = async (source, blockSize) => {
  const rsp = await fetch(source, {
    method: "HEAD"
  });

  if (rsp.status !== 200) {
    throw new Error(`ENOENT: no such file or directory, open '${source}'`);
  }

  return {
    reader: new HTTPReader(source, blockSize),
    size: Number(rsp.headers.get("content-length"))
  };
};
