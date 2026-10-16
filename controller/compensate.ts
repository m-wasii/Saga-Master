import { NextFunction, Request, Response } from "express";

const compensate = async (req: Request, res: Response, _next: NextFunction) => {
  console.log("Compensation helper received:", req.body);
  return res.json({ message: "Compensation Triggered!" });
};

export { compensate };
