const fs = require("fs");
const path = require("path");

const source = path.join(__dirname, "..", "docs");
const target = path.join(__dirname, "..", "dist", "docs");
fs.mkdirSync(path.dirname(target), { recursive: true });
fs.cpSync(source, target, { recursive: true });
