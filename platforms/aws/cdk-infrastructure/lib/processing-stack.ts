import path from "node:path";
import { Arn, ArnFormat, Stack, StackProps } from "aws-cdk-lib";
import { CfnJob } from "aws-cdk-lib/aws-glue";
import {
  Effect,
  ManagedPolicy,
  PermissionsBoundary,
  PolicyStatement,
  Role,
  ServicePrincipal,
} from "aws-cdk-lib/aws-iam";
import { Asset } from "aws-cdk-lib/aws-s3-assets";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";
import { DataLakePrefixes } from "./storage-stack";
import { WarehouseResources } from "./warehouse-stack";

export type GlueJobKey = "processRaw" | "validateProcessed" | "buildCurated" | "loadWarehouse";

export interface GlueRuntimeSettings {
  readonly workerType: string;
  readonly workerCount: number;
  readonly timeoutMinutes: number;
  readonly maxConcurrency: number;
}

export interface DeploymentAssetPaths {
  readonly sourceDirectory: string;
  readonly entrypoints: Record<GlueJobKey, string>;
  readonly cloudConfig: string;
  readonly rawContract: string;
  readonly processedContract: string;
  readonly curatedContract: string;
  readonly qualityContract: string;
  readonly referenceContract: string;
  readonly redshiftPolicy: string;
}

export interface GlueJobResources {
  readonly processRaw: CfnJob;
  readonly validateProcessed: CfnJob;
  readonly buildCurated: CfnJob;
  readonly loadWarehouse: CfnJob;
}

export interface ProcessingStackProps extends StackProps {
  readonly environmentName: string;
  readonly dataLakeBucket: Bucket;
  readonly prefixes: DataLakePrefixes;
  readonly runtime: GlueRuntimeSettings;
  readonly assets: DeploymentAssetPaths;
  readonly warehouse: WarehouseResources;
  readonly permissionsBoundaryArn?: string;
}

interface JobSpecification {
  readonly key: GlueJobKey;
  readonly constructId: string;
  readonly jobName: string;
  readonly logGroupPrefix: string;
  readonly dataPrefixes: string[];
  readonly writablePrefixes: string[];
  readonly mutablePrefixes: string[];
  readonly extraFiles: Array<{ readonly path: string; readonly argument: string }>;
}

function applyPermissionsBoundary(
  scope: Construct,
  role: Role,
  id: string,
  permissionsBoundaryArn: string | undefined,
): void {
  if (permissionsBoundaryArn === undefined) {
    return;
  }
  const boundary = ManagedPolicy.fromManagedPolicyArn(scope, id, permissionsBoundaryArn);
  PermissionsBoundary.of(role).apply(boundary);
}

export class ProcessingStack extends Stack {
  public readonly jobs: GlueJobResources;
  public readonly logGroupPrefixes: Record<GlueJobKey, string>;

  public constructor(scope: Construct, id: string, props: ProcessingStackProps) {
    super(scope, id, props);

    const packageAsset = new Asset(this, "AwsEtlSourcePackage", {
      path: props.assets.sourceDirectory,
    });
    const scriptAssets: Record<GlueJobKey, Asset> = {
      processRaw: new Asset(this, "ProcessRawScriptAsset", { path: props.assets.entrypoints.processRaw }),
      validateProcessed: new Asset(this, "ValidateProcessedScriptAsset", { path: props.assets.entrypoints.validateProcessed }),
      buildCurated: new Asset(this, "BuildCuratedScriptAsset", { path: props.assets.entrypoints.buildCurated }),
      loadWarehouse: new Asset(this, "LoadWarehouseScriptAsset", { path: props.assets.entrypoints.loadWarehouse }),
    };
    const fileAssets = new Map<string, Asset>();
    for (const assetPath of [
      props.assets.cloudConfig,
      props.assets.rawContract,
      props.assets.processedContract,
      props.assets.curatedContract,
      props.assets.qualityContract,
      props.assets.referenceContract,
      props.assets.redshiftPolicy,
    ]) {
      fileAssets.set(assetPath, new Asset(this, `FileAsset${fileAssets.size + 1}`, { path: assetPath }));
    }

    const sharedPrefixes = [
      props.prefixes.quality,
      props.prefixes.manifests,
      props.prefixes.audit,
      props.prefixes.staging,
    ];
    const specifications: JobSpecification[] = [
      {
        key: "processRaw",
        constructId: "ProcessRaw",
        jobName: `ecommerce-sales-${props.environmentName}-process-raw`,
        logGroupPrefix: `/aws-glue/jobs/ecommerce-sales-${props.environmentName}/process-raw`,
        dataPrefixes: [props.prefixes.raw, props.prefixes.processed, props.prefixes.rejected, ...sharedPrefixes],
        mutablePrefixes: [props.prefixes.processed, props.prefixes.rejected, props.prefixes.staging],
        writablePrefixes: [props.prefixes.raw, props.prefixes.processed, props.prefixes.rejected, ...sharedPrefixes],
        extraFiles: [
          { path: props.assets.cloudConfig, argument: "--config" },
          { path: props.assets.rawContract, argument: "--raw-contract" },
          { path: props.assets.processedContract, argument: "--processed-contract" },
        ],
      },
      {
        key: "validateProcessed",
        constructId: "ValidateProcessed",
        jobName: `ecommerce-sales-${props.environmentName}-validate-processed`,
        logGroupPrefix: `/aws-glue/jobs/ecommerce-sales-${props.environmentName}/validate-processed`,
        dataPrefixes: [props.prefixes.processed, props.prefixes.rejected, ...sharedPrefixes],
        mutablePrefixes: [props.prefixes.rejected, props.prefixes.staging],
        writablePrefixes: [props.prefixes.processed, props.prefixes.rejected, ...sharedPrefixes],
        extraFiles: [
          { path: props.assets.cloudConfig, argument: "--config" },
          { path: props.assets.rawContract, argument: "--raw-contract" },
          { path: props.assets.processedContract, argument: "--processed-contract" },
          { path: props.assets.qualityContract, argument: "--quality-contract" },
          { path: props.assets.referenceContract, argument: "--reference-contract" },
        ],
      },
      {
        key: "buildCurated",
        constructId: "BuildCurated",
        jobName: `ecommerce-sales-${props.environmentName}-build-curated`,
        logGroupPrefix: `/aws-glue/jobs/ecommerce-sales-${props.environmentName}/build-curated`,
        dataPrefixes: [props.prefixes.processed, props.prefixes.curated, props.prefixes.rejected, ...sharedPrefixes],
        mutablePrefixes: [props.prefixes.curated, props.prefixes.rejected, props.prefixes.staging],
        writablePrefixes: [props.prefixes.processed, props.prefixes.curated, props.prefixes.rejected, ...sharedPrefixes],
        extraFiles: [
          { path: props.assets.cloudConfig, argument: "--config" },
          { path: props.assets.rawContract, argument: "--raw-contract" },
          { path: props.assets.processedContract, argument: "--processed-contract" },
          { path: props.assets.curatedContract, argument: "--curated-contract" },
        ],
      },
      {
        key: "loadWarehouse",
        constructId: "LoadWarehouse",
        jobName: `ecommerce-sales-${props.environmentName}-load-redshift-warehouse`,
        logGroupPrefix: `/aws-glue/jobs/ecommerce-sales-${props.environmentName}/load-redshift-warehouse`,
        dataPrefixes: [props.prefixes.curated, props.prefixes.quality, props.prefixes.staging, props.prefixes.audit],
        writablePrefixes: [
          `${props.prefixes.staging}warehouse/redshift/`,
          `${props.prefixes.audit}warehouse/redshift/`,
        ],
        mutablePrefixes: [`${props.prefixes.staging}warehouse/redshift/`],
        extraFiles: [
          { path: props.assets.cloudConfig, argument: "--config" },
          { path: props.assets.rawContract, argument: "--raw-contract" },
          { path: props.assets.processedContract, argument: "--processed-contract" },
          { path: props.assets.curatedContract, argument: "--curated-contract" },
          { path: props.assets.redshiftPolicy, argument: "--redshift-policy" },
        ],
      },
    ];

    const jobs = {} as Record<GlueJobKey, CfnJob>;
    const logGroupPrefixes = {} as Record<GlueJobKey, string>;
    for (const specification of specifications) {
      const role = this.createJobRole(props, specification, packageAsset, scriptAssets[specification.key], fileAssets);
      const extraFileUrls = specification.extraFiles.map(({ path: assetPath }) => {
        const asset = fileAssets.get(assetPath);
        if (asset === undefined) {
          throw new Error(`missing generated deployment asset: ${assetPath}`);
        }
        return asset.s3ObjectUrl;
      });
      const defaultArguments: Record<string, string> = {
        "--job-language": "python",
        "--TempDir": `s3://${props.dataLakeBucket.bucketName}/${props.prefixes.glueTemporary}`,
        "--enable-metrics": "true",
        "--enable-observability-metrics": "true",
        "--custom-logGroup-prefix": specification.logGroupPrefix,
        "--extra-py-files": packageAsset.s3ObjectUrl,
        "--extra-files": extraFileUrls.join(","),
        "--customer-driver-env-vars": specification.key === "loadWarehouse"
          ? [
            `CUSTOMER_AWS_ETL_BUCKET=${props.dataLakeBucket.bucketName}`,
            `CUSTOMER_REDSHIFT_WORKGROUP=${props.warehouse.workgroupName}`,
            `CUSTOMER_REDSHIFT_DATABASE=${props.warehouse.databaseName}`,
            `CUSTOMER_REDSHIFT_COPY_ROLE_ARN=${props.warehouse.copyRoleArn}`,
          ].join(",")
          : `CUSTOMER_AWS_ETL_BUCKET=${props.dataLakeBucket.bucketName}`,
      };
      for (const extraFile of specification.extraFiles) {
        const asset = fileAssets.get(extraFile.path);
        if (asset === undefined) {
          throw new Error(`missing generated deployment asset: ${extraFile.path}`);
        }
        defaultArguments[extraFile.argument] = path.posix.basename(asset.s3ObjectKey);
      }

      const job = new CfnJob(this, `${specification.constructId}Job`, {
        name: specification.jobName,
        description: `Managed Glue 5.0 entrypoint for ${specification.jobName}`,
        role: role.roleArn,
        glueVersion: "5.0",
        command: {
          name: "glueetl",
          pythonVersion: "3",
          scriptLocation: scriptAssets[specification.key].s3ObjectUrl,
        },
        defaultArguments,
        executionProperty: { maxConcurrentRuns: specification.key === "loadWarehouse" ? 1 : props.runtime.maxConcurrency },
        maxRetries: 0,
        numberOfWorkers: props.runtime.workerCount,
        timeout: props.runtime.timeoutMinutes,
        workerType: props.runtime.workerType,
      });
      job.node.addDependency(packageAsset, scriptAssets[specification.key], ...fileAssets.values());
      jobs[specification.key] = job;
      logGroupPrefixes[specification.key] = specification.logGroupPrefix;
    }

    this.jobs = jobs;
    this.logGroupPrefixes = logGroupPrefixes;
  }

  private createJobRole(
    props: ProcessingStackProps,
    specification: JobSpecification,
    packageAsset: Asset,
    scriptAsset: Asset,
    fileAssets: Map<string, Asset>,
  ): Role {
    const role = new Role(this, `${specification.constructId}ExecutionRole`, {
      assumedBy: new ServicePrincipal("glue.amazonaws.com"),
      description: `Least-privilege role for ${specification.jobName}`,
    });
    applyPermissionsBoundary(
      this,
      role,
      `${specification.constructId}PermissionsBoundary`,
      props.permissionsBoundaryArn,
    );

    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["s3:GetBucketLocation", "s3:ListBucket"],
      resources: [props.dataLakeBucket.bucketArn],
      conditions: {
        StringLike: {
          "s3:prefix": specification.dataPrefixes.flatMap((prefix) => [prefix, `${prefix}*`]),
        },
      },
    }));
    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["s3:GetObject"],
      resources: specification.dataPrefixes.map((prefix) => props.dataLakeBucket.arnForObjects(`${prefix}*`)),
    }));
    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
      resources: specification.writablePrefixes.map((prefix) => props.dataLakeBucket.arnForObjects(`${prefix}*`)),
    }));
    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["s3:DeleteObject"],
      resources: specification.mutablePrefixes.map((prefix) => props.dataLakeBucket.arnForObjects(`${prefix}*`)),
    }));
    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      resources: [
        Arn.format({
          service: "logs",
          resource: "log-group",
          resourceName: `${specification.logGroupPrefix}/*`,
          arnFormat: ArnFormat.COLON_RESOURCE_NAME,
        }, this),
        Arn.format({
          service: "logs",
          resource: "log-group",
          resourceName: `${specification.logGroupPrefix}/*:*`,
          arnFormat: ArnFormat.COLON_RESOURCE_NAME,
        }, this),
      ],
    }));

    packageAsset.grantRead(role);
    scriptAsset.grantRead(role);
    for (const extraFile of specification.extraFiles) {
      const asset = fileAssets.get(extraFile.path);
      if (asset === undefined) {
        throw new Error(`missing file asset grant for ${extraFile.path}`);
      }
      asset.grantRead(role);
    }
    if (specification.key === "loadWarehouse") {
      role.addToPolicy(new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "redshift-data:ExecuteStatement",
          "redshift-data:BatchExecuteStatement",
          "redshift-data:DescribeStatement",
          "redshift-data:GetStatementResult",
          "redshift-data:CancelStatement",
        ],
        resources: [props.warehouse.workgroupArn],
      }));
      role.addToPolicy(new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ["redshift-serverless:GetCredentials"],
        resources: [props.warehouse.workgroupArn],
      }));
    }
    return role;
  }
}
