clean-pyc: ## Clean Python cache files
	find . \( -name '*.pyc' -o -name '*.pyo' -o -name '*~' -o -name '__pycache__' -o -name '.pytest_cache' \) -exec rm -rf {} +

clean-test: ## Cleanup pytest leftovers
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr test_results/
	rm -f *report.html log.html test-results.html output.xml

docker-restart: ## Restart containers
	docker compose down
	docker compose up --build -d

docker-test: docker-restart ## Restart containers & run tests
	pytest

docker-test-verbose: docker-restart ## Restart containers & run tests with verbosity
	pytest -s

pre-commit-setup: ## Install pre-commit
	python3 -m pip install pre-commit
	pre-commit --version

requirements: ## Install all requirements
	python3 -m pip install -r requirements.txt
	python3 -m pip install -r requirements-test.txt
	python3 -m pip install ruff

setup: requirements pre-commit-setup## Set up all requirements

docs: ## Open your browser to the web apps testing docs
	@echo "Opening documentation..."
	xdg-open http://localhost:5000/docs || open http://localhost:5000/docs
