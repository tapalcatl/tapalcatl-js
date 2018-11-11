"use strict";

const Archive = require("./lib/archive");
const Source = require("./lib/source");

module.exports = Source.load;
module.exports.archive = Archive.load;