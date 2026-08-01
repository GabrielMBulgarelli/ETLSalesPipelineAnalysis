#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { App } from "aws-cdk-lib";
import { CatalogStack } from "../lib/catalog-stack";
import { ObservabilityStack } from "../lib/observability-stack";
import { OrchestrationStack } from "../lib/orchestration-stack";
import { DeploymentAssetPaths, GlueRuntimeSettings, ProcessingStack } from "../lib/processing-stack";
import { StorageStack } from "../lib/storage-stack";
import { WarehouseStack } from "../lib/warehouse-stack";

const ALLOWED_ENVIRONMENTS = new Set(["dev", "staging", "prod"]);
const ALLOWED_WORKER_TYPES = new Set([
  "G.1X", "G.2X", "G.4X", "G.8X", "G.12X", "G.16X",
  "R.1X", "R.2X", "R.4X", "R.8X",
]);

function contextString(app: App, key: string): string {
  const value: unknown = app.node.tryGetContext(key);
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`CDK context ${key} must be a non-empty string`);
  }
  return value.trim();
}

function contextInteger(app: App, key: string, minimum: number, maximum: number): number {
  const value: unknown = app.node.tryGetContext(key);
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`CDK context ${key} must be an integer from ${minimum} through ${maximum}`);
  }
  return parsed;
}

function writeDeploymentAssets(
  repositoryRoot: string,
  cdkRoot: string,
  environmentName: string,
  region: string,
): DeploymentAssetPaths {
  const generatedDirectory = path.join(cdkRoot, "cdk.out", "generated-assets", environmentName);
  fs.mkdirSync(generatedDirectory, { recursive: true });
  const cloudConfig = path.join(generatedDirectory, "cloud-config.yaml");
  fs.writeFileSync(cloudConfig, [
    `environment: ${environmentName}`,
    "endpoint_url: null",
    `region: ${region}`,
    "bucket: configured-at-runtime",
    "raw_prefix: raw/",
    "manifest_prefix: manifests/",
    "audit_prefix: audit/",
    "processed_prefix: processed/",
    "curated_prefix: curated/",
    "rejected_prefix: rejected/",
    "quality_prefix: quality/",
    "staging_prefix: staging/",
    "pipeline_version: 1.0.0",
    'contract_version: "1"',
    "aws_access_key_id: null",
    "aws_secret_access_key: null",
    "",
  ].join("\n"), "utf8");

  const contractSources = {
    rawContract: "contracts/schemas/raw/datasets.yaml",
    processedContract: "contracts/schemas/processed/datasets.yaml",
    curatedContract: "contracts/schemas/curated/datasets.yaml",
    qualityContract: "contracts/rules/quality-thresholds.yaml",
    referenceContract: "contracts/rules/referential-integrity.yaml",
    redshiftPolicy: "contracts/rules/redshift-warehouse.yaml",
  } as const;
  const generatedContracts = {} as Record<keyof typeof contractSources, string>;
  for (const [key, relativeSource] of Object.entries(contractSources) as Array<
    [keyof typeof contractSources, string]
  >) {
    const destination = path.join(generatedDirectory, `${key}.yaml`);
    fs.copyFileSync(path.join(repositoryRoot, relativeSource), destination);
    generatedContracts[key] = destination;
  }

  return {
    sourceDirectory: path.join(repositoryRoot, "platforms/aws/src"),
    entrypoints: {
      processRaw: path.join(repositoryRoot, "platforms/aws/entrypoints/glue-jobs/process_raw.py"),
      validateProcessed: path.join(repositoryRoot, "platforms/aws/entrypoints/glue-jobs/validate_processed.py"),
      buildCurated: path.join(repositoryRoot, "platforms/aws/entrypoints/glue-jobs/build_curated.py"),
      loadWarehouse: path.join(repositoryRoot, "platforms/aws/entrypoints/glue-jobs/load_redshift_warehouse.py"),
    },
    cloudConfig,
    ...generatedContracts,
  };
}

const app = new App();
const environmentName = contextString(app, "environment");
if (!ALLOWED_ENVIRONMENTS.has(environmentName)) {
  throw new Error("CDK context environment must be one of: dev, staging, prod");
}
const region = contextString(app, "awsRegion");
if (!/^[a-z]{2}(?:-[a-z0-9]+)+-\d$/.test(region)) {
  throw new Error("CDK context awsRegion is not a valid AWS region identifier");
}
const workerType = contextString(app, "glueWorkerType");
if (!ALLOWED_WORKER_TYPES.has(workerType)) {
  throw new Error(`CDK context glueWorkerType must be one of: ${[...ALLOWED_WORKER_TYPES].join(", ")}`);
}
const runtime: GlueRuntimeSettings = {
  workerType,
  workerCount: contextInteger(app, "glueWorkerCount", 2, 299),
  timeoutMinutes: contextInteger(app, "glueTimeoutMinutes", 1, 10080),
  maxConcurrency: contextInteger(app, "glueMaxConcurrency", 1, 100),
};
const boundaryContext: unknown = app.node.tryGetContext("permissionsBoundaryArn");
const permissionsBoundaryArn = boundaryContext === undefined ? undefined : String(boundaryContext).trim();
if (permissionsBoundaryArn !== undefined && !/^arn:[^:]+:iam::\d{12}:policy\/.+/.test(permissionsBoundaryArn)) {
  throw new Error("CDK context permissionsBoundaryArn must be an IAM managed-policy ARN");
}

const cdkRoot = path.resolve(__dirname, "..");
const repositoryRoot = path.resolve(cdkRoot, "../../../..");
const stackEnvironment = { account: process.env.CDK_DEFAULT_ACCOUNT, region };
const stackName = (component: string): string => `EcommerceSales-${environmentName}-${component}`;
const commonProps = { env: stackEnvironment };
const redshiftBaseCapacity = contextInteger(app, "redshiftBaseCapacity", 8, 8);
const redshiftMaxCapacity = contextInteger(app, "redshiftMaxCapacity", 16, 16);
const redshiftMonthlyRpuHours = contextInteger(app, "redshiftMonthlyRpuHours", 40, 40);

const storage = new StorageStack(app, stackName("Storage"), {
  ...commonProps,
  environmentName,
});
const catalog = new CatalogStack(app, stackName("Catalog"), {
  ...commonProps,
  dataLakeBucket: storage.dataLakeBucket,
  catalogDirectory: path.join(repositoryRoot, "platforms/aws/catalog"),
});
catalog.addStackDependency(storage);

const warehouse = new WarehouseStack(app, stackName("Warehouse"), {
  ...commonProps,
  environmentName,
  dataLakeBucket: storage.dataLakeBucket,
  prefixes: storage.prefixes,
  baseCapacity: redshiftBaseCapacity,
  maxCapacity: redshiftMaxCapacity,
  monthlyRpuHours: redshiftMonthlyRpuHours,
});
warehouse.addStackDependency(storage);

const processing = new ProcessingStack(app, stackName("Processing"), {
  ...commonProps,
  environmentName,
  dataLakeBucket: storage.dataLakeBucket,
  prefixes: storage.prefixes,
  runtime,
  assets: writeDeploymentAssets(repositoryRoot, cdkRoot, environmentName, region),
  warehouse: warehouse.warehouse,
  permissionsBoundaryArn,
});
processing.addStackDependency(storage);
processing.addStackDependency(catalog);
processing.addStackDependency(warehouse);

const orchestration = new OrchestrationStack(app, stackName("Orchestration"), {
  ...commonProps,
  environmentName,
  dataLakeBucket: storage.dataLakeBucket,
  prefixes: storage.prefixes,
  jobs: processing.jobs,
  stateMachineTemplatePath: path.join(repositoryRoot, "platforms/aws/orchestration/pipeline.asl.json"),
  permissionsBoundaryArn,
});
orchestration.addStackDependency(processing);

const observability = new ObservabilityStack(app, stackName("Observability"), {
  ...commonProps,
  environmentName,
  jobs: processing.jobs,
  glueLogGroupPrefixes: processing.logGroupPrefixes,
  stateMachine: orchestration.stateMachine,
});
observability.addStackDependency(processing);
observability.addStackDependency(orchestration);

app.synth();
