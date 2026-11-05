.PHONY: test demo audit check
export PYTHONPATH := src

test:
	python -m unittest discover -s tests -v

check: test
	python -m compileall -q src tests

demo:
	python scripts/demo.py

audit:
	python -m ledgerflow audit
