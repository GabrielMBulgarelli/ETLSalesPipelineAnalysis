import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { Stack, StackProps } from "aws-cdk-lib";
import { CfnDatabase, CfnTable } from "aws-cdk-lib/aws-glue";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";

const DATABASE_NAME = "ecommerce_sales_curated";
const BUCKET_TOKEN = "${AWS_ETL_BUCKET}";

interface CatalogDatabaseTemplate {
  readonly DatabaseInput: {
    readonly Name: string;
    readonly Description?: string;
  };
}

interface CatalogTableTemplate {
  readonly DatabaseName: string;
  readonly TableInput: Record<string, unknown> & {
    readonly Name: string;
    readonly PartitionKeys: unknown[];
  };
}

export interface CatalogStackProps extends StackProps {
  readonly dataLakeBucket: Bucket;
  readonly catalogDirectory: string;
}

function lowerCamelKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(lowerCamelKeys);
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, nested]) => [
        `${key.charAt(0).toLowerCase()}${key.slice(1)}`,
        lowerCamelKeys(nested),
      ]),
    );
  }
  return value;
}

function resolveBucketToken(value: unknown, bucketName: string): unknown {
  if (typeof value === "string") {
    return value.replaceAll(BUCKET_TOKEN, bucketName);
  }
  if (Array.isArray(value)) {
    return value.map((nested) => resolveBucketToken(nested, bucketName));
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, nested]) => [key, resolveBucketToken(nested, bucketName)]),
    );
  }
  return value;
}

export class CatalogStack extends Stack {
  public readonly database: CfnDatabase;
  public readonly tables: CfnTable[];

  public constructor(scope: Construct, id: string, props: CatalogStackProps) {
    super(scope, id, props);

    const databasePath = path.join(props.catalogDirectory, "database.json");
    const databaseTemplate = JSON.parse(
      readFileSync(databasePath, "utf8"),
    ) as CatalogDatabaseTemplate;
    if (databaseTemplate.DatabaseInput.Name !== DATABASE_NAME) {
      throw new Error(`catalog database must remain ${DATABASE_NAME}`);
    }
    this.database = new CfnDatabase(this, "CuratedDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: databaseTemplate.DatabaseInput.Name,
        description: databaseTemplate.DatabaseInput.Description,
      },
    });

    const tableDirectory = path.join(props.catalogDirectory, "tables");
    const tableFiles = readdirSync(tableDirectory)
      .filter((fileName) => fileName.endsWith(".json"))
      .sort();
    if (tableFiles.length !== 16) {
      throw new Error(`expected 16 committed catalog tables, found ${tableFiles.length}`);
    }

    this.tables = tableFiles.map((fileName) => {
      const template = JSON.parse(
        readFileSync(path.join(tableDirectory, fileName), "utf8"),
      ) as CatalogTableTemplate;
      if (template.DatabaseName !== DATABASE_NAME) {
        throw new Error(`${fileName} references unexpected database ${template.DatabaseName}`);
      }
      if (template.TableInput.PartitionKeys.length !== 0) {
        throw new Error(`${fileName} must preserve zero partition keys`);
      }
      const serialized = JSON.stringify(template.TableInput);
      if (!serialized.includes(BUCKET_TOKEN)) {
        throw new Error(`${fileName} does not contain the required bucket deployment token`);
      }
      const tableInput = lowerCamelKeys(
        resolveBucketToken(template.TableInput, props.dataLakeBucket.bucketName),
      ) as CfnTable.TableInputProperty;
      const table = new CfnTable(this, `Table${template.TableInput.Name.replace(/(^|_)([a-z])/g, (_match, _prefix, letter: string) => letter.toUpperCase())}`, {
        catalogId: this.account,
        databaseName: this.database.ref,
        tableInput,
      });
      table.addResourceDependency(this.database);
      return table;
    });
  }
}
