const util = require("util");

const pEvent = require("p-event");
const pImmediate = require("p-immediate");

class Archive {
  constructor(source, zip, extension = "") {
    this.extension = extension;
    this.entries = {};
    this.metadata = {
      headers: []
    }
    this.source = source;

    // private
    this._ready = false;
    this._zip = zip;

    this._initialize();
  }

  _initialize() {
    const zip = this._zip;

    zip.on("entry", entry => {
      this.entries[entry.fileName] = entry;
    })
    
    zip.on("end", () => this._ready = true)

    if (zip.comment != null) {
      try {
        this.metadata = JSON.parse(zip.comment);
      } catch (err) {
        console.warn(`Invalid metadata: "${zip.comment}"\n`, err.stack);
      }
    }
  }

  async _open() {
    while (!this._ready) {
      await pImmediate();
    }
  }

  // TODO variant (make extension local)
  async getTile(zoom, x, y) {
    let filename = `${zoom}/${x}/${y}`;

    if (this.extension !== "") {
      filename += `.${this.extension}`;
    }

    await this._open();

    const entry = this.entries[filename];

    if (entry == null) {
      return null;
    }

    let headers = [];

    if (entry.fileComment !== "") {
      try {
        headers = JSON.parse(entry.fileComment);
      } catch (err) {
        console.warn(`Invalid tile headers: "${entry.fileComment}"`, err.stack);
      }
    }

    const rs = await util.promisify(this._zip.openReadStream.bind(this._zip))(entry);

    const chunks = []
    rs.on("data", chunk => chunks.push(chunk));

    await pEvent(rs, "end");

    return {
      headers: [...this.metadata.headers, ...headers],
      // TODO replace this with a readable stream
      body: Buffer.concat(chunks)
    };
  }
}

module.exports = Archive;
