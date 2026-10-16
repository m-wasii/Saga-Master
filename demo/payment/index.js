const express = require("express");
const crypto = require("crypto");

const app = express();
const port = Number(process.env.PORT) || 8082;
const payments = new Map();

app.use(express.json());

app.post("/api/payments/rollback", (req, res) => {
  res.json({ rolledBack: true, received: req.body });
});

app.post("/api/payments", (req, res) => {
  if (process.env.FAIL_PAYMENT === "true") {
    res.status(500).json({ message: "Payment failed" });
    return;
  }
  const id = crypto.randomUUID();
  const record = { id, received: req.body };
  payments.set(id, record);
  res.status(201).json(record);
});

app.listen(port, "0.0.0.0", () => {
  console.log(`Payment service listening on ${port}`);
});
