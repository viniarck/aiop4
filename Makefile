# You can set args for test selection. For example: make test-unit args='-k client'

test-unit:
	poetry run pytest -vv tests/ $(args)

test-cov:
	# TODO reraise cov to 85 later on
	poetry run pytest -vv --cov=aiop4 --cov-fail-under=30 tests/ $(args)

lint:
	poetry run black --check aiop4/ tests/

test-tox:
	poetry run tox
