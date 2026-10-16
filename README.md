# Saga Master

Saga Master is a declarative JSON saga orchestrator by Muhammad Usman, Khuzaima Nawab, and Muhammad Wasi, supervised by Faisal Feroz (MAJU, Fall 2024). It answers future work from Koyya and Muthukumar (2022).

The public API is two POST routes:

- `POST /api/orchestrate/setup` stores a named saga in memory
- `POST /api/orchestrate/trigger/:url` runs that saga with the JSON body you send

Swagger UI is at `http://localhost:8888/api-docs`. Example, compensate, and setup list/update/delete routes remain as developer helpers. The product is the two routes above.

JSON is the definition. Every setup key is required. Array order is transaction order. Saga Master forwards the client JSON and waits. REST success is HTTP 2xx. Kafka success is a boolean `success` field. If that field is missing, Saga Master keeps waiting. Compensation runs only for steps that succeeded, in reverse. A failed rollback stops the process and returns an error.

When `sendResponseToAPI` is true, the next REST call or Kafka payload includes earlier step responses under `priorResponses` and under each `serviceName`.

## Cross-platform

Saga Master never links to a participant's language or database. A service can join if it accepts JSON over REST, or if it uses Kafka and answers with a boolean `success`. That is why it is not a Java saga library. Node is the orchestrator's runtime because the report wanted a lightweight process with normal HTTP and Kafka clients.

The four demo apps are Node stand-ins so a clone can run the checkout. They are not the chapter 5 proof. That proof was a Java order service, a Python inventory service, a PHP payment script, and a Node notification service, all reached through those same two protocols.

Short comparison with table 5.1:

| | Saga Master |
| --- | --- |
| Participant platform | Open |
| Protocols | REST and Kafka |
| Role | Orchestration only |
| Event bus | Kafka |
| Setup storage | In-memory |
| Definition | JSON, instead of a custom language |

## Limits

Setups live in memory. A restart clears them. An orchestration that is already running can finish. There is no authentication. Kafka is the event bus. One orchestration runs at a time in the single Node process. The demo apps stand in for the original Java, Python, and PHP services. This is a bachelor's project, not a production orchestrator.

## REST demo

`npm run demo` starts Saga Master on port 8888 and the four HTTP apps:

| App | Port | Calls |
| --- | --- | --- |
| Order | 8080 | `POST /api/invoices`, rollback `DELETE /api/invoices/:id` |
| Inventory | 8000 | `POST /api/products`, rollback `DELETE /api/products/:id` |
| Payment | 8082 | `POST /api/payments`, rollback `POST /api/payments/rollback` |
| Notification | 8089 | `POST /notifications` |

The setup file is not loaded automatically. Post it, then trigger the checkout:

```powershell
curl.exe -X POST http://localhost:8888/api/orchestrate/setup -H "Content-Type: application/json" --data-binary "@fake-data/ms-test-setup.json"
curl.exe -X POST http://localhost:8888/api/orchestrate/trigger/create-payment-intend -H "Content-Type: application/json" -d "{\"item\":\"book\",\"amount\":42}"
```

To show compensation, stop the demo and start it again with payment forced to fail. `FAIL_PAYMENT=true` makes `POST /api/payments` return a non-2xx status. Order and inventory are then removed in reverse.

```powershell
$env:FAIL_PAYMENT="true"
npm run demo
```

Post the same setup and trigger `create-payment-intend` again.

Other scripts: `npm run dev` runs Saga Master with ts-node, `npm run build` compiles TypeScript, and `npm start` runs the compiled JavaScript.

## Kafka demo

`docker compose up --build` starts the four HTTP apps, the Kafka participants, a prebuilt Apache Kafka broker, and Saga Master. Inside Compose, Saga Master and the participants use `KAFKA_BROKERS=kafka:29092`. The setup file can keep `localhost:9092` for a broker on the host. `fake-data/kafka-setup.json` gives each step its own command topic, reply topic, compensation command topic, and compensation reply topic. `demo/kafka-participants` reads that file. It replies `{ "success": true }` on each reply topic. With `FAIL_PAYMENT=true` the payment step replies `{ "success": false }`. Compensation topics reply `{ "success": true }`.

From the host:

```powershell
docker compose up --build
curl.exe -X POST http://localhost:8888/api/orchestrate/setup -H "Content-Type: application/json" --data-binary "@fake-data/kafka-setup.json"
curl.exe -X POST http://localhost:8888/api/orchestrate/trigger/create-payment-intend -H "Content-Type: application/json" -d "{\"item\":\"book\",\"amount\":42}"
```

To show Kafka compensation, set `FAIL_PAYMENT` to `true` for `kafka-participants` in `docker-compose.yml` and start that service again, then post the setup and trigger the same URL.

The image is built from `Dockerfile`: a Node base image, `npm install`, PM2, TypeScript compiled to JavaScript, then PM2 runs `dist/index.js`.

## Tests

```powershell
npm test
```

One test runs the REST happy path and the payment-failure rollback through the thin apps. A second test checks that a Kafka reply of `{ "success": false }` starts compensation in reverse.
