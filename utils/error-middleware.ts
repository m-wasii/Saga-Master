import { NextFunction, Request, Response } from "express";
import { HttpError } from "../types";

export function errorMiddleware(err: HttpError, _req: Request, res: Response, _next: NextFunction): void {
  const status = typeof err.status === "number" ? err.status : 500;
  const message = err.message || "Internal server error";
  console.error(message);
  res.status(status).json({ message });
}
