#!/usr/bin/env node

import { createHash } from "node:crypto";

const ALLOWED_ENVIRONMENTS = new Set(["dev", "staging", "prod"]);

export function deterministicExecutionName(
  environment: string,
  batchId: string,
  orchestrationAttempt: number,
): string {
  if (!ALLOWED_ENVIRONMENTS.has(environment)) {
    throw new Error("environment must be one of: dev, staging, prod");
  }
  if (batchId.length === 0) {
    throw new Error("batch ID must not be empty");
  }
  if (!Number.isSafeInteger(orchestrationAttempt) || orchestrationAttempt < 1) {
    throw new Error("orchestration attempt must be a positive integer");
  }

  const hash = createHash("sha256")
    .update(`${batchId}\n${orchestrationAttempt}`, "utf8")
    .digest("hex")
    .slice(0, 16);
  const sanitized = batchId
    .replace(/[^A-Za-z0-9_-]+/g, "-")
    .replace(/[-_]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (sanitized.length === 0) {
    throw new Error("batch ID must contain at least one ASCII letter or digit");
  }

  const maximumPrefixLength = 80 - environment.length - hash.length - 2;
  const batchPrefix = sanitized
    .slice(0, maximumPrefixLength)
    .replace(/[-_]+$/g, "");
  if (batchPrefix.length === 0) {
    throw new Error("batch ID does not produce a valid execution-name prefix");
  }
  const name = `${environment}-${batchPrefix}-${hash}`;
  if (name.length > 80 || !/^[A-Za-z0-9_-]+$/.test(name)) {
    throw new Error("generated execution name violates Step Functions constraints");
  }
  return name;
}

function main(): void {
  const [environment, batchId, attemptText] = process.argv.slice(2);
  if (environment === undefined || batchId === undefined || attemptText === undefined) {
    throw new Error("usage: execution-name <environment> <batch-id> <orchestration-attempt>");
  }
  const attempt = Number(attemptText);
  process.stdout.write(`${deterministicExecutionName(environment, batchId, attempt)}\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 2;
  }
}
