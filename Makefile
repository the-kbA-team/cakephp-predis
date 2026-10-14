SHELL := /bin/sh
.DEFAULT_GOAL := help

COMPOSER_IMAGE ?= composer:2
COMPOSER_CACHE_DIR ?= $(HOME)/.cache/composer
DOCKER_USER := --user "$$(id -u)":"$$(id -g)"
DOCKER_MOUNT := --volume "$$(pwd)":/app --workdir /app

COMPOSER_UPDATE_FLAGS := --prefer-dist --no-interaction --no-progress
ifeq ($(DEPENDENCIES_LOWEST),1)
COMPOSER_UPDATE_FLAGS += --prefer-lowest
endif

# Trust an optional corporate CA without replacing the container's public CAs.
ifdef CA_CERT_FILE
CA_MOUNT := --volume "$(CA_CERT_FILE)":/tmp/extra-ca.crt:ro
CA_TRUST_CMD := cat /etc/ssl/certs/ca-certificates.crt /tmp/extra-ca.crt > /tmp/ca-bundle.pem && export COMPOSER_CAFILE=/tmp/ca-bundle.pem CURL_CA_BUNDLE=/tmp/ca-bundle.pem SSL_CERT_FILE=/tmp/ca-bundle.pem &&
endif

.PHONY: help clean check-php-version install lint audit validate

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  %-20s %s\n", $$1, $$2}'

clean: ## Remove installed dependencies and composer.lock
	rm -rf vendor
	rm -f composer.lock

check-php-version: ## Require an explicit PHP_VERSION
	@if [ -z "$(PHP_VERSION)" ]; then \
		echo "Error: PHP_VERSION must be set, e.g. PHP_VERSION=5.5." >&2; \
		exit 1; \
	fi

install: check-php-version ## Resolve dependencies (DEPENDENCIES_LOWEST=1 selects lowest versions)
	@mkdir -p "$(COMPOSER_CACHE_DIR)"
	@docker run --rm --init $(DOCKER_USER) \
		--env HTTP_PROXY --env HTTPS_PROXY --env NO_PROXY \
		--env COMPOSER_CACHE_DIR=/tmp/composer-cache \
		--volume "$(COMPOSER_CACHE_DIR)":/tmp/composer-cache \
		$(CA_MOUNT) $(DOCKER_MOUNT) $(COMPOSER_IMAGE) sh -c '\
			status=0; \
			$(CA_TRUST_CMD) \
			composer config platform.php "$(PHP_VERSION)" || status=$$?; \
			if [ $$status -eq 0 ]; then \
				composer update $(COMPOSER_UPDATE_FLAGS) || status=$$?; \
			fi; \
			composer config --unset platform.php || { cleanup_status=$$?; [ $$status -ne 0 ] || status=$$cleanup_status; }; \
			composer config --unset platform || { cleanup_status=$$?; [ $$status -ne 0 ] || status=$$cleanup_status; }; \
			if php -r '"'"'$$manifest = json_decode(file_get_contents("composer.json")); exit(isset($$manifest->config) && count(get_object_vars($$manifest->config)) === 0 ? 0 : 1);'"'"'; then \
				composer config --unset config || { cleanup_status=$$?; [ $$status -ne 0 ] || status=$$cleanup_status; }; \
			fi; \
			rm -f composer.lock || { cleanup_status=$$?; [ $$status -ne 0 ] || status=$$cleanup_status; }; \
			exit $$status \
		'; \
	status=$$?; \
	rm -f composer.lock || { cleanup_status=$$?; [ $$status -ne 0 ] || status=$$cleanup_status; }; \
	exit $$status

lint: check-php-version ## Syntax-check Lib/ with the requested PHP runtime
	docker run --rm --init $(DOCKER_USER) $(DOCKER_MOUNT) \
		"php:$(PHP_VERSION)-cli" sh -c "find Lib -type f -name '*.php' -print0 | xargs -0 -n1 php -l"

audit: ## Audit installed dependencies for known vulnerabilities
	@test -f vendor/composer/installed.json || { echo "Error: run make install successfully before make audit." >&2; exit 1; }
	docker run --rm --init $(DOCKER_USER) \
		--env HTTP_PROXY --env HTTPS_PROXY --env NO_PROXY \
		$(CA_MOUNT) $(DOCKER_MOUNT) $(COMPOSER_IMAGE) \
		sh -c '$(CA_TRUST_CMD) composer audit'

validate: ## Validate composer.json strictly
	docker run --rm --init $(DOCKER_USER) $(DOCKER_MOUNT) \
		$(COMPOSER_IMAGE) composer validate --strict
