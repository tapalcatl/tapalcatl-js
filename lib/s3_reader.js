"use strict";

const url = require("url");

const AWS = require("aws-sdk");

const BlockReader = require("./block_reader");

const S3 = new AWS.S3();

class S3Reader extends BlockReader {
  constructor(bucket, key, blockSize) {
    super(blockSize);

    this.bucket = bucket;
    this.key = key;
  }

  cacheKey(blockNumber) {
    return `s3://${this.bucket}/${this.key}#${blockNumber}`;
  }

  async readBlock(blockStart, blockEnd) {
    // TODO Requester-Pays support
    const obj = await S3.getObject({
      Bucket: this.bucket,
      Key: this.key,
      Range: `bytes=${blockStart}-${blockEnd}`
    }).promise();

    return obj.Body;
  }
}

module.exports = async (source, blockSize) => {
  const uri = url.parse(source);

  const bucket = uri.hostname;
  const key = uri.pathname.slice(1);

  try {
    const rsp = await S3.headObject({
      Bucket: bucket,
      Key: key
    }).promise();

    return {
      reader: new S3Reader(bucket, key, blockSize),
      size: Number(rsp.ContentLength)
    };
  } catch (err) {
    throw new Error(`ENOENT: no such file or directory, open '${source}'`);
  }
};
