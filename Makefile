.PHONY: baseline-test

baseline-test:
	python3 -m unittest discover -s platforms/azure/tests -v
	python3 scripts/baseline_fixture.py --check
