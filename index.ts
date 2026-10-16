import "dotenv/config";
import express from "express";
import swaggerUi from "swagger-ui-express";
import swaggerDocument from "./docs/swagger.json";
import { orchestrateRouter } from "./routes/orchestrate";
import { exampleRouter } from "./routes/example";
import { compensateRouter } from "./routes/compensate";
import { errorMiddleware } from "./utils/error-middleware";

const app = express();
const port = Number(process.env.PORT) || 8888;

app.use(express.json());
app.use("/api/orchestrate", orchestrateRouter);
app.use("/api/examples", exampleRouter);
app.use("/api/compensate", compensateRouter);
app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerDocument));
app.use(errorMiddleware);

if (require.main === module) {
  app.listen(port, () => {
    console.log(`Saga Master is running at http://localhost:${port}`);
  });
}

export { app };
