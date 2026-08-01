import fs from "node:fs";
import { Arn, CfnOutput, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import {
  Effect,
  ManagedPolicy,
  PermissionsBoundary,
  PolicyStatement,
  Role,
  ServicePrincipal,
} from "aws-cdk-lib/aws-iam";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { CfnStateMachine } from "aws-cdk-lib/aws-stepfunctions";
import { Construct } from "constructs";
import { GlueJobResources } from "./processing-stack";
import { DataLakePrefixes } from "./storage-stack";

export interface OrchestrationStackProps extends StackProps {
  readonly environmentName: string;
  readonly dataLakeBucket: Bucket;
  readonly prefixes: DataLakePrefixes;
  readonly jobs: GlueJobResources;
  readonly stateMachineTemplatePath: string;
  readonly permissionsBoundaryArn?: string;
}

function applyPermissionsBoundary(
  scope: Construct,
  role: Role,
  permissionsBoundaryArn: string | undefined,
): void {
  if (permissionsBoundaryArn === undefined) {
    return;
  }
  PermissionsBoundary.of(role).apply(
    ManagedPolicy.fromManagedPolicyArn(scope, "OrchestrationPermissionsBoundary", permissionsBoundaryArn),
  );
}

export class OrchestrationStack extends Stack {
  public readonly stateMachine: CfnStateMachine;
  public readonly stateMachineLogGroup: LogGroup;

  public constructor(scope: Construct, id: string, props: OrchestrationStackProps) {
    super(scope, id, props);

    const template = fs.readFileSync(props.stateMachineTemplatePath, "utf8");
    const templateTokens: string[] = template.match(/\$\{[^}]+\}/g) ?? [];
    const expectedTemplateTokens = [
      "${...}",
      "${ProcessRawGlueJobName}",
      "${ValidateProcessedGlueJobName}",
      "${BuildCuratedGlueJobName}",
    ];
    if (
      templateTokens.length !== expectedTemplateTokens.length
      || !expectedTemplateTokens.every((token) => templateTokens.includes(token))
    ) {
      throw new Error(`unexpected ASL deployment-token set: ${templateTokens.join(", ")}`);
    }
    const definition = template
      .replace("${...}", "the declared Glue job-name tokens")
      .replaceAll("${ProcessRawGlueJobName}", props.jobs.processRaw.ref)
      .replaceAll("${ValidateProcessedGlueJobName}", props.jobs.validateProcessed.ref)
      .replaceAll("${BuildCuratedGlueJobName}", props.jobs.buildCurated.ref);
    const parsedDefinition = JSON.parse(definition) as { readonly States?: Record<string, unknown> };
    if (Object.keys(parsedDefinition.States ?? {}).length !== 29) {
      throw new Error("the committed orchestration definition must contain exactly 29 top-level states");
    }

    this.stateMachineLogGroup = new LogGroup(this, "StateMachineLogGroup", {
      logGroupName: `/aws/vendedlogs/states/ecommerce-sales-${props.environmentName}-pipeline`,
      retention: RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.RETAIN,
    });

    const role = new Role(this, "StateMachineExecutionRole", {
      assumedBy: new ServicePrincipal("states.amazonaws.com"),
      description: "Runs the approved ecommerce sales Glue orchestration and immutable S3 evidence protocol",
    });
    applyPermissionsBoundary(this, role, props.permissionsBoundaryArn);

    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
      resources: Object.values(props.jobs).map((job) => Arn.format({
        service: "glue",
        resource: "job",
        resourceName: job.ref,
      }, this)),
    }));
    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["s3:GetBucketLocation", "s3:ListBucket"],
      resources: [props.dataLakeBucket.bucketArn],
      conditions: {
        StringLike: {
          "s3:prefix": [
            `${props.prefixes.staging}orchestration/claims/*`,
            `${props.prefixes.quality}*`,
            `${props.prefixes.audit}*`,
          ],
        },
      },
    }));
    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["s3:GetObject", "s3:PutObject"],
      resources: [
        props.dataLakeBucket.arnForObjects(`${props.prefixes.staging}orchestration/claims/*`),
        props.dataLakeBucket.arnForObjects(`${props.prefixes.audit}*`),
      ],
    }));
    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["s3:GetObject"],
      resources: [props.dataLakeBucket.arnForObjects(`${props.prefixes.quality}*`)],
    }));
    // Step Functions log delivery APIs do not support resource-level permissions.
    role.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "logs:CreateLogDelivery",
        "logs:GetLogDelivery",
        "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries",
        "logs:PutResourcePolicy",
        "logs:DescribeResourcePolicies",
        "logs:DescribeLogGroups",
      ],
      resources: ["*"],
    }));

    this.stateMachine = new CfnStateMachine(this, "PipelineStateMachine", {
      stateMachineName: `ecommerce-sales-${props.environmentName}-pipeline`,
      stateMachineType: "STANDARD",
      definitionString: definition,
      roleArn: role.roleArn,
      loggingConfiguration: {
        destinations: [{ cloudWatchLogsLogGroup: { logGroupArn: `${this.stateMachineLogGroup.logGroupArn}:*` } }],
        includeExecutionData: true,
        level: "ALL",
      },
    });
    this.stateMachine.node.addDependency(this.stateMachineLogGroup);

    new CfnOutput(this, "StateMachineArn", { value: this.stateMachine.attrArn });
    new CfnOutput(this, "DeterministicExecutionNameFormat", {
      value: `${props.environmentName}-<sanitized-batch-prefix>-<sha256-prefix-16>`,
      description: "Generate with make aws-execution-name; supply as StartExecution name",
    });
  }
}
