const express = require("express");
const crypto = require("crypto");

const app = express();
const port = Number(process.env.PORT) || 8080;
const invoices = new Map();

app.use(express.json());

app.post("/api/invoices", (req, res) => {
  const id = crypto.randomUUID();
  const record = { id, received: req.body };
  invoices.set(id, record);
  res.status(201).json(record);
});

app.delete("/api/invoices/:id", (req, res) => {
  if (!invoices.has(req.params.id)) {
    res.status(404).json({ message: "Invoice not found" });
    return;
  }
  invoices.delete(req.params.id);
  res.json({ id: req.params.id, deleted: true });
});

app.listen(port, "0.0.0.0", () => {
  console.log(`Order service listening on ${port}`);
});
