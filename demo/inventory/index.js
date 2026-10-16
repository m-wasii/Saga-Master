const express = require("express");
const crypto = require("crypto");

const app = express();
const port = Number(process.env.PORT) || 8000;
const products = new Map();

app.use(express.json());

app.post("/api/products", (req, res) => {
  const id = crypto.randomUUID();
  const record = { id, received: req.body };
  products.set(id, record);
  res.status(201).json(record);
});

app.delete("/api/products/:id", (req, res) => {
  if (!products.has(req.params.id)) {
    res.status(404).json({ message: "Product not found" });
    return;
  }
  products.delete(req.params.id);
  res.json({ id: req.params.id, deleted: true });
});

app.listen(port, "0.0.0.0", () => {
  console.log(`Inventory service listening on ${port}`);
});
