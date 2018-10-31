const util = require("util");

const pImmediate = require("p-immediate");

class Archive {
  constructor(source, zip) {
    this.entries = {};
    this.metadata = {
      headers: []
    };
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
    });

    zip.on("end", () => (this._ready = true));

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

  close() {
    this._zip.close();
  }

  // TODO variant
  async getTile(zoom, x, y, format = null) {
    if (format == null && Object.keys(this.metadata.formats).length === 1) {
      // only 1 format is defined; use that
      format = Object.keys(this.metadata.formats).pop();
    }

    let filename = `${zoom}/${x}/${y}`;

    if (format != "") {
      // allow format to be ""
      filename += `.${format}`;
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

    const rs = await util.promisify(this._zip.openReadStream.bind(this._zip))(
      entry
    );

    let formatHeaders = this.metadata.formats[format];

    if (typeof formatHeaders === "string") {
      formatHeaders = [
        {
          "Content-Type": formatHeaders
        }
      ];
    }

    const derivedHeaders = [
      {
        ETag: entry.crc32.toString()
      },
      {
        "Last-Modified": entry.getLastModDate().toUTCString()
      }
    ];

    return {
      headers: [...formatHeaders, ...headers, ...derivedHeaders],
      body: rs
    };
  }
}

module.exports = Archive;
