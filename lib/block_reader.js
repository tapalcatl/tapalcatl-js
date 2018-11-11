"use strict";

const crypto = require("crypto");
const fs = require("fs-extra");
const { PassThrough } = require("stream");

const LRU = require("lru-cache");
const pMap = require("p-map");
const tmp = require("tmp-promise");
const { RandomAccessReader } = require("yauzl");

const DEFAULT_BLOCK_SIZE = process.env.DEFAULT_BLOCK_SIZE || 2 ** 20; // 1MiB

const CACHE = LRU({
  max: 500e6, // 500MB
  length: (n, key) => n.length,
  dispose: async (key, n) => {
    if (await fs.exists(n.filename)) {
      try {
        await fs.unlink(n.filename);
      } catch (err) {
        console.warn(err.stack);
      }
    }
  }
});

tmp.setGracefulCleanup();

class BlockReader extends RandomAccessReader {
  constructor(blockSize = DEFAULT_BLOCK_SIZE) {
    super();

    this.blockSize = blockSize;
    this.blocks = [];
  }

  cacheKey(blockNumber) {
    throw new Error("Not implemented.");
  }

  readBlock() {
    throw new Error("Not implemented.");
  }

  close(callback) {
    super.close(async err => {
      if (err) {
        console.warn(err.stack);
      }

      this.blocks.forEach(i => CACHE.del(this.cacheKey(i)));

      return callback();
    });
  }

  async readFromBlock(start, end, blockNumber) {
    const blockStart = blockNumber * this.blockSize;
    const blockEnd = (blockNumber + 1) * this.blockSize - 1;

    let position;
    if (start < blockStart) {
      position = 0;
    } else {
      position = start % this.blockSize;
    }

    let length;
    if (end > blockEnd) {
      length = this.blockSize - position;
    } else {
      length = (end % this.blockSize) - position;
    }

    const key = this.cacheKey(blockNumber);
    let blockMeta = CACHE.get(key);

    if (blockMeta == null || !(await fs.exists(blockMeta.filename))) {
      const block = await this.readBlock(blockStart, blockEnd);

      const { path: filename } = await tmp.file({
        prefix:
          crypto
            .createHash("sha256")
            .update(key)
            .digest("hex") + "-"
      });

      await fs.writeFile(filename, block);

      CACHE.set(key, {
        filename,
        length: block.length
      });
      this.blocks.push(blockNumber);

      return block.slice(position, position + length);
    } else {
      const fd = await fs.open(blockMeta.filename, "r");

      const { buffer } = await fs.read(
        fd,
        Buffer.allocUnsafe(length),
        0,
        length,
        position
      );

      await fs.close(fd);

      return buffer;
    }
  }

  async fetchInto(start, end, rs) {
    try {
      const firstBlock = Math.floor(start / this.blockSize);
      const lastBlock = Math.floor((end - 1) / this.blockSize);

      // range(firstBlock, lastBlock + 1)
      const blocks = [...Array(lastBlock - firstBlock + 1).keys()].map(
        x => x + firstBlock
      );

      const bufs = await pMap(
        blocks,
        this.readFromBlock.bind(this, start, end),
        {
          concurrency: 8
        }
      );

      bufs.forEach(chunk => rs.write(chunk));
      rs.end();
    } catch (err) {
      console.warn(err.stack);
      rs.emit("error", err);
    }
  }

  async read(buffer, offset, length, position, callback) {
    try {
      const start = position;
      const end = position + length;

      const firstBlock = Math.floor(start / this.blockSize);
      const lastBlock = Math.floor((end - 1) / this.blockSize);

      // range(firstBlock, lastBlock + 1)
      const blocks = [...Array(lastBlock - firstBlock + 1).keys()].map(
        x => x + firstBlock
      );

      const bufs = await pMap(
        blocks,
        this.readFromBlock.bind(this, start, end),
        {
          concurrency: 8
        }
      );

      bufs.reduce(
        (offset, chunk) => offset + chunk.copy(buffer, offset, 0, chunk.length),
        offset
      );

      return callback();
    } catch (err) {
      return callback(err);
    }
  }

  _readStreamForRange(start, end) {
    const rs = new PassThrough();

    this.fetchInto(start, end, rs);

    return rs;
  }
}

module.exports = BlockReader;
