const express = require("express");
const crypto = require("crypto");

const app = express();
const port = Number(process.env.PORT) || 8089;

app.use(express.json());

app.post("/notifications", (req, res) => {
  res.status(201).json({
    id: crypto.randomUUID(),
    delivered: true,
    received: req.body,
  });
});

app.listen(port, "0.0.0.0", () => {
  console.log(`Notification service listening on ${port}`);
});
