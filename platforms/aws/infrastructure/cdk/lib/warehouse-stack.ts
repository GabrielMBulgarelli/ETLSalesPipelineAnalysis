import { CfnOutput, CfnResource, Fn, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { Alarm, ComparisonOperator, Metric, TreatMissingData } from "aws-cdk-lib/aws-cloudwatch";
import {
  CfnRouteTable,
  CfnSecurityGroup,
  CfnSubnet,
  CfnSubnetRouteTableAssociation,
  CfnVPC,
  CfnVPCEndpoint,
} from "aws-cdk-lib/aws-ec2";
import { Effect, PolicyStatement, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { CfnNamespace, CfnWorkgroup } from "aws-cdk-lib/aws-redshiftserverless";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";
import { DataLakePrefixes } from "./storage-stack";

export interface WarehouseStackProps extends StackProps {
  readonly environmentName: string;
  readonly dataLakeBucket: Bucket;
  readonly prefixes: DataLakePrefixes;
  readonly baseCapacity: number;
  readonly maxCapacity: number;
  readonly monthlyRpuHours: number;
}

export interface WarehouseResources {
  readonly workgroupName: string;
  readonly workgroupArn: string;
  readonly namespaceArn: string;
  readonly databaseName: string;
  readonly copyRoleArn: string;
}

export class WarehouseStack extends Stack {
  public readonly warehouse: WarehouseResources;

  public constructor(scope: Construct, id: string, props: WarehouseStackProps) {
    super(scope, id, props);
    if (props.baseCapacity !== 8 || props.maxCapacity !== 16 || props.monthlyRpuHours !== 40) {
      throw new Error("Phase 10 Redshift capacity must be exactly 8 base RPU, 16 max RPU, and 40 monthly RPU-hours");
    }

    const vpc = new CfnVPC(this, "WarehouseVpc", {
      cidrBlock: "10.42.0.0/24",
      enableDnsHostnames: true,
      enableDnsSupport: true,
      instanceTenancy: "default",
      tags: [{ key: "Name", value: `ecommerce-sales-${props.environmentName}-warehouse` }],
    });
    const subnetCidrs = ["10.42.0.0/27", "10.42.0.32/27", "10.42.0.64/27"];
    const subnets: CfnSubnet[] = [];
    const routeTables: CfnRouteTable[] = [];
    subnetCidrs.forEach((cidrBlock, index) => {
      const subnet = new CfnSubnet(this, `WarehouseIsolatedSubnet${index + 1}`, {
        availabilityZone: Fn.select(index, Fn.getAzs()),
        cidrBlock,
        mapPublicIpOnLaunch: false,
        vpcId: vpc.ref,
        tags: [{ key: "Name", value: `ecommerce-sales-${props.environmentName}-warehouse-${index + 1}` }],
      });
      const routeTable = new CfnRouteTable(this, `WarehouseRouteTable${index + 1}`, { vpcId: vpc.ref });
      new CfnSubnetRouteTableAssociation(this, `WarehouseRouteAssociation${index + 1}`, {
        routeTableId: routeTable.ref,
        subnetId: subnet.ref,
      });
      subnets.push(subnet);
      routeTables.push(routeTable);
    });

    const allowedPrefixes = [
      `${props.prefixes.curated}*`,
      `${props.prefixes.quality}*`,
      `${props.prefixes.staging}warehouse/redshift/*`,
      `${props.prefixes.audit}warehouse/redshift/*`,
    ];
    new CfnVPCEndpoint(this, "WarehouseS3GatewayEndpoint", {
      serviceName: Fn.sub("com.amazonaws.${AWS::Region}.s3"),
      vpcEndpointType: "Gateway",
      vpcId: vpc.ref,
      routeTableIds: routeTables.map((routeTable) => routeTable.ref),
      policyDocument: {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Principal: "*",
            Action: ["s3:GetBucketLocation", "s3:ListBucket"],
            Resource: props.dataLakeBucket.bucketArn,
            Condition: { StringLike: { "s3:prefix": allowedPrefixes } },
          },
          {
            Effect: "Allow",
            Principal: "*",
            Action: ["s3:GetObject", "s3:PutObject"],
            Resource: allowedPrefixes.map((prefix) => props.dataLakeBucket.arnForObjects(prefix)),
          },
        ],
      },
    });

    const securityGroup = new CfnSecurityGroup(this, "WarehouseSecurityGroup", {
      groupDescription: "Private Redshift Serverless workgroup; no public ingress",
      securityGroupEgress: [{ ipProtocol: "tcp", fromPort: 443, toPort: 443, cidrIp: "0.0.0.0/0" }],
      vpcId: vpc.ref,
    });
    const copyRole = new Role(this, "RedshiftCopyRole", {
      assumedBy: new ServicePrincipal("redshift-serverless.amazonaws.com"),
      description: "Read-only COPY access to attempt-isolated Redshift staging objects and manifests",
    });
    copyRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["s3:GetBucketLocation", "s3:ListBucket"],
      resources: [props.dataLakeBucket.bucketArn],
      conditions: { StringLike: { "s3:prefix": [`${props.prefixes.staging}warehouse/redshift/*`] } },
    }));
    copyRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ["s3:GetObject"],
      resources: [props.dataLakeBucket.arnForObjects(`${props.prefixes.staging}warehouse/redshift/*`)],
    }));

    const namespaceName = `ecommerce-sales-${props.environmentName}-warehouse`;
    const databaseName = "ecommerce_sales";
    const retainNamespace = props.environmentName === "prod";
    const namespace = new CfnNamespace(this, "WarehouseNamespace", {
      namespaceName,
      dbName: databaseName,
      adminUsername: "warehouse_admin",
      manageAdminPassword: true,
      finalSnapshotName: retainNamespace ? undefined : `${namespaceName}-final-snapshot`,
      finalSnapshotRetentionPeriod: retainNamespace ? undefined : 7,
      defaultIamRoleArn: copyRole.roleArn,
      iamRoles: [copyRole.roleArn],
      logExports: ["userlog", "connectionlog", "useractivitylog"],
    });
    namespace.applyRemovalPolicy(retainNamespace ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY);

    const workgroup = new CfnWorkgroup(this, "WarehouseWorkgroup", {
      workgroupName: namespaceName,
      namespaceName: namespace.ref,
      baseCapacity: props.baseCapacity,
      maxCapacity: props.maxCapacity,
      enhancedVpcRouting: true,
      publiclyAccessible: false,
      subnetIds: subnets.map((subnet) => subnet.ref),
      securityGroupIds: [securityGroup.attrGroupId],
      configParameters: [
        { parameterKey: "require_ssl", parameterValue: "true" },
        { parameterKey: "enable_user_activity_logging", parameterValue: "true" },
      ],
    });
    workgroup.addDependency(namespace);

    const usageLimit = new CfnResource(this, "MonthlyComputeUsageLimit", {
      type: "AWS::RedshiftServerless::UsageLimit",
      properties: {
        Amount: props.monthlyRpuHours,
        BreachAction: "deactivate",
        Period: "monthly",
        ResourceArn: workgroup.attrWorkgroupWorkgroupArn,
        UsageType: "serverless-compute",
      },
    });
    usageLimit.addDependency(workgroup);
    new Alarm(this, "MaximumCapacityAlarm", {
      alarmName: `ecommerce-sales-${props.environmentName}-redshift-capacity`,
      alarmDescription: "Actionless alarm at the approved 16-RPU ceiling",
      metric: new Metric({
        namespace: "AWS/Redshift-Serverless",
        metricName: "ComputeCapacity",
        dimensionsMap: { Workgroup: namespaceName },
      }),
      threshold: props.maxCapacity,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    this.warehouse = {
      workgroupName: namespaceName,
      workgroupArn: workgroup.attrWorkgroupWorkgroupArn,
      namespaceArn: namespace.attrNamespaceNamespaceArn,
      databaseName,
      copyRoleArn: copyRole.roleArn,
    };
    new CfnOutput(this, "WarehouseDatabaseName", { value: databaseName });
    new CfnOutput(this, "WarehouseNamespaceName", { value: namespaceName });
    new CfnOutput(this, "WarehouseNamespaceArn", { value: namespace.attrNamespaceNamespaceArn });
    new CfnOutput(this, "WarehouseWorkgroupName", { value: namespaceName });
    new CfnOutput(this, "WarehouseWorkgroupArn", { value: workgroup.attrWorkgroupWorkgroupArn });
    new CfnOutput(this, "WarehouseCopyRoleArn", { value: copyRole.roleArn });
  }
}
