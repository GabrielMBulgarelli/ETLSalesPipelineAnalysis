import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { BlockPublicAccess, Bucket, BucketEncryption } from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";

export const DATA_LAKE_PREFIXES = {
  raw: "raw/",
  processed: "processed/",
  curated: "curated/",
  rejected: "rejected/",
  quality: "quality/",
  manifests: "manifests/",
  audit: "audit/",
  staging: "staging/",
  glueTemporary: "staging/glue-temp/",
} as const;

export type DataLakePrefixes = typeof DATA_LAKE_PREFIXES;

export interface StorageStackProps extends StackProps {
  readonly environmentName: string;
}

export class StorageStack extends Stack {
  public readonly dataLakeBucket: Bucket;
  public readonly prefixes: DataLakePrefixes = DATA_LAKE_PREFIXES;

  public constructor(scope: Construct, id: string, props: StorageStackProps) {
    super(scope, id, props);

    this.dataLakeBucket = new Bucket(this, "DataLakeBucket", {
      bucketName: `ecommerce-sales-${props.environmentName}-${this.account}-${this.region}`,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      versioned: true,
      removalPolicy: RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
      lifecycleRules: [
        {
          id: "AbortIncompleteMultipartUploads",
          abortIncompleteMultipartUploadAfter: Duration.days(7),
          noncurrentVersionExpiration: Duration.days(90),
        },
        {
          id: "ExpireStagingObjects",
          prefix: DATA_LAKE_PREFIXES.staging,
          expiration: Duration.days(7),
          noncurrentVersionExpiration: Duration.days(90),
        },
      ],
    });

    new CfnOutput(this, "DataLakeBucketName", {
      value: this.dataLakeBucket.bucketName,
      description: "Managed S3 data-lake bucket; no resources have been deployed by synthesis.",
    });
  }
}
