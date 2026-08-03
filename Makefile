.PHONY: baseline-test contract-test baseline-validate contract-validate catalog-generate catalog-validate \
	aws-local-up aws-local-seed aws-local-process aws-local-validate aws-local-curate aws-local-run aws-local-status aws-local-down \
	aws-postgres-up aws-postgres-load aws-postgres-validate aws-postgres-status aws-postgres-down aws-postgres-clean aws-local-warehouse \
	aws-state-machine-validate aws-redshift-sql-validate aws-redshift-warehouse-validate aws-cdk-install aws-cdk-build aws-cdk-synth aws-execution-name \
	python-compile project-validate

AWS_LOCAL_DIR := platforms/aws/local-runtime
AWS_PYTHONPATH := platforms/aws/src
AWS_CDK_DIR := platforms/aws/cdk-infrastructure
include $(AWS_LOCAL_DIR)/glue.env
export GLUE_IMAGE

baseline-test:
	python3 -m unittest discover -s tests -v
	python3 -m unittest discover -s platforms/azure/tests -v
	python3 scripts/baseline_fixture.py --check

baseline-validate: baseline-test

contract-validate:
	python3 scripts/validate_contracts.py --fixture baseline >/dev/null

contract-test: contract-validate

catalog-generate:
	python3 scripts/catalog_metadata.py --generate

catalog-validate:
	python3 scripts/catalog_metadata.py

aws-state-machine-validate:
	PYTHONPATH=$(AWS_PYTHONPATH) python3 -m aws_etl.orchestration platforms/aws/orchestration/pipeline.asl.json
	PYTHONPATH=$(AWS_PYTHONPATH) python3 $(AWS_LOCAL_DIR)/pipeline_runner.py --self-check

aws-redshift-sql-validate:
	python3 scripts/validate_redshift_sql.py

aws-redshift-warehouse-validate:
	python3 scripts/validate_redshift_warehouse.py

aws-cdk-install:
	npm --prefix $(AWS_CDK_DIR) ci

aws-cdk-build:
	npm --prefix $(AWS_CDK_DIR) run build

aws-cdk-synth:
	npm --prefix $(AWS_CDK_DIR) run synth

python-compile:
	python3 -m compileall -q platforms/aws/src platforms/aws/glue-jobs scripts

project-validate: python-compile baseline-validate contract-validate catalog-validate aws-state-machine-validate \
	aws-redshift-sql-validate aws-redshift-warehouse-validate aws-cdk-build aws-cdk-synth

aws-execution-name:
	@test -n "$(BATCH_ID)" || { echo "BATCH_ID is required" >&2; exit 2; }
	npm --prefix $(AWS_CDK_DIR) run execution-name -- "$(or $(ENVIRONMENT),dev)" "$(BATCH_ID)" "$(or $(ATTEMPT),1)"

aws-local-up:
	bash $(AWS_LOCAL_DIR)/bootstrap.sh

aws-local-seed:
	@test -n "$(DATASET_DIR)" || { echo "DATASET_DIR is required (for example: make aws-local-seed DATASET_DIR=/path/to/olist)" >&2; exit 2; }
	PYTHONPATH=$(AWS_PYTHONPATH) DATASET_DIR="$(DATASET_DIR)" BATCH_ID="$(BATCH_ID)" BATCH_TIMESTAMP="$(BATCH_TIMESTAMP)" \
		python3 $(AWS_LOCAL_DIR)/seed_s3.py --config $(AWS_LOCAL_DIR)/config.yaml

aws-local-process:
	@test -n "$(BATCH_ID)" || { echo "BATCH_ID is required (for example: make aws-local-process BATCH_ID=my-batch)" >&2; exit 2; }
	BATCH_ID="$(BATCH_ID)" bash $(AWS_LOCAL_DIR)/run_glue_job.sh

aws-local-validate:
	@test -n "$(BATCH_ID)" || { echo "BATCH_ID is required" >&2; exit 2; }
	BATCH_ID="$(BATCH_ID)" GLUE_JOB=validate_processed bash $(AWS_LOCAL_DIR)/run_glue_job.sh

aws-local-curate:
	@test -n "$(BATCH_ID)" || { echo "BATCH_ID is required" >&2; exit 2; }
	BATCH_ID="$(BATCH_ID)" GLUE_JOB=build_curated bash $(AWS_LOCAL_DIR)/run_glue_job.sh

aws-local-run:
	@test -n "$(DATASET_DIR)" || { echo "DATASET_DIR is required (for example: make aws-local-run DATASET_DIR=/path/to/olist)" >&2; exit 2; }
	PYTHONPATH=$(AWS_PYTHONPATH) DATASET_DIR="$(DATASET_DIR)" BATCH_ID="$(BATCH_ID)" BATCH_TIMESTAMP="$(BATCH_TIMESTAMP)" EXECUTION_ID="$(EXECUTION_ID)" \
		WAREHOUSE="$(WAREHOUSE)" \
		python3 $(AWS_LOCAL_DIR)/pipeline_runner.py --config $(AWS_LOCAL_DIR)/config.yaml

aws-local-status:
	docker compose -f $(AWS_LOCAL_DIR)/docker-compose.yml ps
	PYTHONPATH=$(AWS_PYTHONPATH) python3 $(AWS_LOCAL_DIR)/seed_s3.py --config $(AWS_LOCAL_DIR)/config.yaml --status

aws-local-down:
	docker compose -f $(AWS_LOCAL_DIR)/docker-compose.yml down

aws-postgres-up:
	bash $(AWS_LOCAL_DIR)/postgres.sh up

aws-postgres-load:
	@test -n "$(BATCH_ID)" || { echo "BATCH_ID is required" >&2; exit 2; }
	BATCH_ID="$(BATCH_ID)" GLUE_JOB=load_warehouse AWS_ETL_WAREHOUSE_MODE=load bash $(AWS_LOCAL_DIR)/run_glue_job.sh

aws-postgres-validate:
	@test -n "$(BATCH_ID)" || { echo "BATCH_ID is required" >&2; exit 2; }
	BATCH_ID="$(BATCH_ID)" GLUE_JOB=load_warehouse AWS_ETL_WAREHOUSE_MODE=validate bash $(AWS_LOCAL_DIR)/run_glue_job.sh

aws-postgres-status:
	bash $(AWS_LOCAL_DIR)/postgres.sh status

aws-postgres-down:
	bash $(AWS_LOCAL_DIR)/postgres.sh down

aws-postgres-clean:
	bash $(AWS_LOCAL_DIR)/postgres.sh clean

aws-local-warehouse: aws-postgres-load
