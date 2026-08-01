import { Duration, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { Alarm, ComparisonOperator, Metric, TreatMissingData } from "aws-cdk-lib/aws-cloudwatch";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";
import { GlueJobKey, GlueJobResources } from "./processing-stack";
import { CfnStateMachine } from "aws-cdk-lib/aws-stepfunctions";

export interface ObservabilityStackProps extends StackProps {
  readonly environmentName: string;
  readonly jobs: GlueJobResources;
  readonly glueLogGroupPrefixes: Record<GlueJobKey, string>;
  readonly stateMachine: CfnStateMachine;
}

export class ObservabilityStack extends Stack {
  public constructor(scope: Construct, id: string, props: ObservabilityStackProps) {
    super(scope, id, props);

    for (const [jobKey, prefix] of Object.entries(props.glueLogGroupPrefixes)) {
      for (const streamType of ["error", "output"] as const) {
        new LogGroup(this, `${jobKey}${streamType}LogGroup`, {
          logGroupName: `${prefix}/${streamType}`,
          retention: RetentionDays.ONE_MONTH,
          removalPolicy: RemovalPolicy.RETAIN,
        });
      }
    }

    new Alarm(this, "StepFunctionsFailureAlarm", {
      alarmName: `ecommerce-sales-${props.environmentName}-step-functions-failures`,
      alarmDescription: "Actionless alarm for failed ecommerce sales workflow executions",
      metric: new Metric({
        namespace: "AWS/States",
        metricName: "ExecutionsFailed",
        dimensionsMap: { StateMachineArn: props.stateMachine.attrArn },
        statistic: "Sum",
        period: Duration.minutes(5),
      }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    for (const [jobKey, job] of Object.entries(props.jobs)) {
      new Alarm(this, `${jobKey}FailureAlarm`, {
        alarmName: `ecommerce-sales-${props.environmentName}-${jobKey}-failures`,
        alarmDescription: `Actionless alarm for failed ${job.ref} Glue tasks`,
        metric: new Metric({
          namespace: "Glue",
          metricName: "glue.driver.aggregate.numFailedTasks",
          dimensionsMap: { JobName: job.ref, JobRunId: "ALL", Type: "count" },
          statistic: "Sum",
          period: Duration.minutes(5),
        }),
        threshold: 1,
        evaluationPeriods: 1,
        comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: TreatMissingData.NOT_BREACHING,
      });
    }
  }
}
