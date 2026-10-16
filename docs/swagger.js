const swaggerAutogen = require("swagger-autogen")();

const doc = {
  info: {
    version: "1.0.0",
    title: "Saga Master",
    description: "Declarative JSON saga orchestrator. Product routes: POST /api/orchestrate/setup and POST /api/orchestrate/trigger/{url}.",
  },
  host: "localhost:8888",
  basePath: "/",
  schemes: ["http"],
  consumes: ["application/json"],
  produces: ["application/json"],
};

const outputFile = "./docs/swagger.json";
const endpointsFiles = ["./index.ts"];

swaggerAutogen(outputFile, endpointsFiles, doc);
