const { spawn } = require("child_process");

const commands = [
  "npx ts-node index.ts",
  "node demo/order/index.js",
  "node demo/inventory/index.js",
  "node demo/payment/index.js",
  "node demo/notification/index.js",
];

const children = commands.map((command) =>
  spawn(command, {
    shell: true,
    stdio: "inherit",
    env: process.env,
  })
);

function shutdown() {
  for (const child of children) {
    if (!child.pid) continue;
    if (process.platform === "win32") {
      spawn("taskkill", ["/pid", String(child.pid), "/t", "/f"], { shell: true, stdio: "ignore" });
    } else {
      child.kill("SIGTERM");
    }
  }
  setTimeout(() => process.exit(0), 300);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

console.log("Saga Master demo");
console.log("Saga Master http://localhost:8888");
console.log("Order http://localhost:8080  Inventory http://localhost:8000  Payment http://localhost:8082  Notification http://localhost:8089");
console.log("Post fake-data/ms-test-setup.json to POST /api/orchestrate/setup, then trigger create-payment-intend.");
