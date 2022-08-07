# You can set args for test selection. For example: make test-unit args='-k client'

test-unit:
	poetry run pytest tests/ $(args)

test-cov:
	poetry run pytest --cov=aiop4 --cov-fail-under=85 tests/ $(args)

lint:
	poetry run black --check aiop4/ tests/

test-tox:
	poetry run tox
