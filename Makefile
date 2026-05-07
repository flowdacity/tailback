.PHONY: all clean build install uninstall test publish redis-up redis-down

# Default target
all: clean build

# Remove Python + build artifacts
clean:
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete
	find . -name "*~" -delete
	rm -rf dist build *.egg-info

# Build package
build:
	uv run python -m build

# Install locally built package
install:
	pip install --force-reinstall dist/*.whl

# Uninstall Tailback completely
uninstall:
	pip uninstall -y tailback

# Run tests — prefers pytest, falls back to python modules
test: redis-up
	@status=0; \
	uv run pytest tests || uv run python -m unittest discover -s tests || status=$$?; \
	$(MAKE) redis-down; \
	exit $$status

publish: clean
	uv sync --group dev
	uv run python -m build
# 	@if [ -z "$$PYPI_API_TOKEN" ]; then echo "PYPI_API_TOKEN must be set"; exit 1; fi
# 	uv run python -m twine upload dist/* -u __token__ -p "$$PYPI_API_TOKEN"
	uv run python -m twine upload dist/*

# Start Redis container
redis-up:
	docker compose up -d redis

# Stop Redis container
redis-down:
	docker compose down
