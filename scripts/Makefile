.DEFAULT_GOAL := help

.PHONY: help build clean install rebuild

help: ## Show usages
	@echo "Usage:"
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build: ## Package dbt-fabricspark-costom & Clean temporary files
	@echo "\033[0;32mStarting Build...\033[0m"
	@python setup.py sdist bdist_wheel
	@echo "\033[0;32mBuild complete!\033[0m"

clean: ## Clean up all built packages & metadata
	@echo "\033[0;31mCleaning up...\033[0m"
	@rm -rf ../build ../dist ../dbt_fabricspark_custom.egg-info
	@echo "\033[0;31mCleanup complete.\033[0m"

install: ## Install package dbt-fabricspark-custom
	@python -m pip install $(shell ls -t dist/dbt_fabricspark_custom-*.whl | head -n 1)

rebuild: clean build ## Clean up & Build again