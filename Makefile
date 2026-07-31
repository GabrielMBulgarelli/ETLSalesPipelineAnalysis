.PHONY: baseline-test contract-test phase-2-test baseline-validate contract-validate phase-2-validate

baseline-test:
	python3 -m unittest discover -s platforms/azure/tests -v
	python3 -m unittest discover -s platforms/azure/tests -v
	python3 scripts/baseline_fixture.py --check

baseline-validate: baseline-test

contract-validate:
	python3 scripts/validate_contracts.py --fixture baseline >/dev/null

contract-test: contract-validate

phase-2-test: baseline-test contract-test

phase-2-validate: baseline-validate contract-validate
