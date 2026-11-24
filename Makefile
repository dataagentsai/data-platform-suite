# Run the same gates locally that CI runs.
# Setup: `make setup` (one-time). Then `make check` before any commit.
# See docs/quality_gates.md.

.PHONY: test-props test-model help setup check lint type security test smoke-test demo demo-full sbom clean install-hooks world world-api world-test engine-conformance run gold-known-answers dashboards ci-local ci-lint shiftleft ci-test explainer

# uv handles its own venv at ./.venv; we don't activate manually.
UV ?= uv
SRC_HAS_PY := $(shell find src -name '*.py' 2>/dev/null | head -1)
SQL_FILES  := $(shell find . -name '*.sql' -not -path './.venv/*' 2>/dev/null)
HAS_TESTS  := $(shell find tests -name 'test_*.py' 2>/dev/null | head -1)

help:
	@echo "make setup            install deps + pre-commit hooks (one-time)"
	@echo "make check            run all gates (CI parity)"
	@echo "make lint             ruff + sqlfluff"
	@echo "make type             mypy --strict"
	@echo "make security         bandit + semgrep + pip-audit + gitleaks"
	@echo "make test             pytest with coverage (unit + integration; smoke skipped)"
	@echo "make smoke-test       end-to-end Docker postgres → ADF Mimic → local Delta"
	@echo "make demo             end-to-end local demo: Bronze → Silver (works on Py 3.14)"
	@echo "make demo-full        end-to-end local demo: + Gold (needs Py 3.11-3.13 + dbt)"
	@echo "make world DAYS=7     the tyre world: N simulated days into the four mock sources + oracle totals"
	@echo "make world-api        serve the dealer interface of the last world run on :8765"
	@echo "make run DAYS=7       world + ingestion + silver + gold day by day: sources -> bronze -> silver -> gold, checked against the oracle"
	@echo "make gold-known-answers  W5's known-answer tests on the metric views' numbers from the last run's gold"
	@echo "make engine-conformance  the stage and gold-build tests on Spark, then on DuckDB (SQLFrame)"
	@echo "make world-test       the dataplatform kernel tests (slice 1 + world + source-port contracts + obs/twin)"
	@echo "make dashboards       regenerate the ops dashboards, ops metric views and native set (realms/databricks/dashboards/)"
	@echo "make ci-local         what .github/workflows/aws-databricks.yml checks: ci-lint + shiftleft + ci-test"
	@echo "make ci-lint          the workflow's lint job: ruff (its exact args), mypy --strict on the cloud adapters, bundle check"
	@echo "make shiftleft        the workflow's shift-left job: kernel scan against the baseline, realm scan"
	@echo "make ci-test          the workflow's test job: pytest without Spark (Spark tests skip), S3/Kinesis on moto, then the stage and gold-build tests on DuckDB"
	@echo "make sbom             generate CycloneDX SBOM"
	@echo "make install-hooks    re-install pre-commit hooks"
	@echo "make clean            remove build artifacts and caches"

setup:
	$(UV) sync --extra dev
	$(UV) run pre-commit install
	@echo
	@echo "Setup complete. Run 'make check' to validate."

install-hooks:
	$(UV) run pre-commit install

# ---------------------------------------------------------------------------
# Aggregate target — same shape as CI.
# ---------------------------------------------------------------------------
check: lint type security test
	@echo
	@echo "All gates passed."

# ---------------------------------------------------------------------------
# Individual targets.
# ---------------------------------------------------------------------------
lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .
ifneq ($(SQL_FILES),)
	$(UV) run sqlfluff lint --dialect tsql .
else
	@echo "No SQL files yet; skipping sqlfluff."
endif

type:
ifneq ($(SRC_HAS_PY),)
	$(UV) run mypy src/
else
	@echo "No Python source yet; skipping mypy."
endif

security:
ifneq ($(SRC_HAS_PY),)
	$(UV) run bandit -c pyproject.toml -r src/
	$(UV) run semgrep --config p/python --config .semgrep.yml --error src/
else
	@echo "No Python source yet; skipping bandit + semgrep."
endif
	$(UV) run pip-audit
	@command -v gitleaks >/dev/null 2>&1 && gitleaks detect --no-banner --redact \
		|| echo "gitleaks not installed; install via 'brew install gitleaks' or skip locally (CI runs it)."

test:
ifneq ($(HAS_TESTS),)
	$(UV) run pytest --cov --cov-report=term --cov-report=xml:coverage.xml
else
	@echo "No tests yet; skipping pytest."
endif

# ---------------------------------------------------------------------------
# Local demo — runnable showcase of the framework's owned arc.
# `demo` runs Source → Bronze → Silver against Docker postgres (works on
# any supported Python). `demo-full` adds the Gold/dbt leg, which needs
# Python 3.11-3.13 (dbt-core doesn't yet support 3.14).
# ---------------------------------------------------------------------------
demo:
	@command -v docker >/dev/null 2>&1 || { \
		echo "Docker not on PATH. Install Docker Desktop and retry."; exit 1; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "Docker daemon unreachable. Start Docker Desktop and retry."; exit 1; \
	}
	$(UV) sync --extra postgres --extra local --extra lineage
	$(UV) run python scripts/demo_end_to_end.py --no-gold

demo-full:
	@command -v docker >/dev/null 2>&1 || { \
		echo "Docker not on PATH. Install Docker Desktop and retry."; exit 1; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "Docker daemon unreachable. Start Docker Desktop and retry."; exit 1; \
	}
	$(UV) sync --extra postgres --extra local --extra lineage --extra gold_dev
	$(UV) run python scripts/demo_end_to_end.py

# ---------------------------------------------------------------------------
# End-to-end smoke test — requires Docker Desktop running + [postgres,local]
# extras installed. See tests/smoke/README.md for setup.
# ---------------------------------------------------------------------------
smoke-test:
	@command -v docker >/dev/null 2>&1 || { \
		echo "Docker not on PATH. See tests/smoke/README.md for setup."; exit 1; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "Docker daemon unreachable. Start Docker Desktop and retry."; exit 1; \
	}
	$(UV) pip install -e '.[postgres,local]'
	$(UV) run pytest -m smoke -v tests/smoke/

# ---------------------------------------------------------------------------
# The tyre world (goal G4): simulated days into the four mock sources, with the
# oracle's totals per day in build/world/oracle/. See src/dataplatform/world/README.md.
# Postgres on this machine by default (WORLD_PG_DSN, default dbname=tyre_world);
# WORLD_DB=sqlite needs nothing installed. SCALE=0.05 for a quick look.
# ---------------------------------------------------------------------------
DAYS ?= 3  # daily dev loop; the 14-day proof with every incident runs on Databricks (user, 13 Nov)
SCALE ?= 0.1
WORLD_DB ?= postgres
WORLD_PY = $(UV) run --no-project --python 3.12 --with pyyaml --with 'psycopg[binary]'

world:
	$(WORLD_PY) python scripts/run_world.py --days $(DAYS) --scale $(SCALE) --db $(WORLD_DB)

# World and platform together (W1b ingestion, W1c silver, W1d gold): N days, each written by the world, ingested to
# bronze (Delta tables in build/run/bronze), taken to silver on a local Spark (build/run/silver) and to gold
# (build/run/gold); bronze, silver and gold are compared with the oracle per day, and the metric views' known answers
# are asked of gold at the end (build/run/gold_metrics.json). Metadata store: DP_RUN_META_DSN (default
# dbname=dp_meta_run, recreated each run); DP_RUN_META=memory needs no server. Needs Java 17 for Spark; SILVER=0 runs
# ingestion only, GOLD=0 stops at silver. On an 8 GB machine run it with nice and SCALE=0.1 (docs/W1D-GOLD.md).
RUN_PY = $(WORLD_PY) --with deltalake --with pyarrow --with 'pyspark==3.5.9' --with 'delta-spark==3.2.0'
# ENGINE=duckdb: silver and gold frames on DuckDB through SQLFrame, Delta by delta-rs; no Spark, no Java (laptop and
# CI only, docs/DUCKDB-SPIKE.md). Versions pinned at releases older than two weeks.
DUCK_DEPS = --with 'sqlframe[duckdb]==4.5.0' --with 'duckdb==1.5.6' --with pytz
ENGINE ?= spark
ifeq ($(ENGINE),duckdb)
ENGINE_PY = $(WORLD_PY) --with deltalake --with pyarrow $(DUCK_DEPS)
else
ENGINE_PY = $(RUN_PY)
endif

SILVER ?= 1
GOLD ?= 1
# SLICE=03-clinical runs the clinical trial (slices/<SLICE> of the catalog, DP_CATALOG or ~/data-platform-catalog);
# CATALOG=/path/to/slice picks a slice folder by path; CALENDAR=compressed puts every incident on days 2-3.
# The world in the slice picks the domain model, the gold models (models/<domain>) and the gold judge. Default: tyre.
SLICE ?=
CATALOG ?=
CALENDAR ?=
run:
	$(ENGINE_PY) python scripts/run_platform.py --engine $(ENGINE) --days $(DAYS) --scale $(SCALE) --db $(WORLD_DB) $(if $(SLICE),--slice $(SLICE),) $(if $(CATALOG),--catalog $(CATALOG),) $(if $(CALENDAR),--calendar $(CALENDAR),) $(if $(filter 0,$(SILVER)),--no-silver,) $(if $(filter 0,$(GOLD)),--no-gold,)

# The same days with ingestion, silver and gold each as its own process (scripts/run_hop.py), the shape deployed jobs
# have; the day is judged from the reports the hops save. Needs local PostgreSQL for the metadata store.
run-hops:
	$(ENGINE_PY) python scripts/run_platform.py --engine $(ENGINE) --days $(DAYS) --scale $(SCALE) --db sqlite --meta postgres --calendar compressed --hops process --housekeeping-days 1 --prove-rebuild --out build/run-hops

# W5's known-answer tests against gold: the metric views' numbers from the last `make run` (DP_GOLD_METRICS) against
# the manifest's known answers (true at SCALE=1.0 and DAYS=14) and against the same run's oracle (any scale).
gold-known-answers:
	DP_GOLD_METRICS=build/run/gold_metrics.json DP_GOLD_ORACLE=build/run/world/oracle/day-$$(printf %02d $(DAYS)).json \
		PYTHONPATH=src $(WORLD_PY) --with pytest pytest -q -p no:cacheprovider -o addopts="" \
		tests/dataplatform/test_semantic_metrics.py -k pending_gold

world-api:
	$(WORLD_PY) python scripts/run_world.py --serve-api

# One logic, two engines (docs/DUCKDB-SPIKE.md): the same stage and gold-build test tables on local Spark, then on
# DuckDB through SQLFrame. SQLFrame takes over the pyspark imports for its whole process, so each engine is its own
# pytest process.
CONFORMANCE = tests/dataplatform/test_silver_stages.py tests/dataplatform/test_gold_engine.py \
	tests/dataplatform/test_baseline_stage.py tests/dataplatform/test_privacy_stage.py
engine-conformance:
	PYTHONPATH=src nice $(RUN_PY) --with pytest pytest -q -p no:cacheprovider -o addopts="" $(CONFORMANCE) --engine spark
	PYTHONPATH=src nice $(WORLD_PY) $(DUCK_DEPS) --with pytest pytest -q -p no:cacheprovider -o addopts="" $(CONFORMANCE) --engine duckdb

world-test:
	PYTHONPATH=src $(RUN_PY) --with pytest --with opentelemetry-sdk pytest -q -p no:cacheprovider -o addopts="" tests/dataplatform

dashboards:
	PYTHONPATH=src $(WORLD_PY) python realms/databricks/dashboards/generate.py

# ---------------------------------------------------------------------------
# CI parity for .github/workflows/aws-databricks.yml: the same commands as its lint, shift-left and test jobs, so a red
# CI is seen before the push. Keep these in step with the workflow. The bundle comes from GitHub as in CI; point it at
# a local checkout with SHIFTLEFT_SRC=file:///path/to/shift-left-bundle.
# ---------------------------------------------------------------------------
CI_PY ?= 3.12
REALM ?= realms/aws-databricks
SHIFTLEFT_SRC ?= git+https://github.com/dataagentsai/shift-left-bundle@cb56555b15d6f1432ec221df255ddb79126a7c00
SL = nice $(UV) run -q --no-project --python $(CI_PY) --with "shiftleft[tools] @ $(SHIFTLEFT_SRC)" shiftleft

ci-local: ci-lint shiftleft ci-test
	@echo
	@echo "aws-databricks workflow checks passed locally."

ci-lint:
	uvx ruff check --line-length 120 src/dataplatform/cloud_ports.py src/dataplatform/adapters/s3.py \
		src/dataplatform/adapters/kinesis.py "$(REALM)" scripts/run_world.py tests/dataplatform/test_cloud_adapters.py \
		tests/aws_databricks
	PYTHONPATH=src $(UV) run -q --no-project --python $(CI_PY) --with mypy mypy --strict --ignore-missing-imports \
		--config-file /dev/null src/dataplatform/cloud_ports.py src/dataplatform/adapters/s3.py \
		src/dataplatform/adapters/kinesis.py
	$(UV) run -q --no-project --python $(CI_PY) --with pyyaml python "$(REALM)/generate_bundle.py" --check

shiftleft:
	$(SL) scan src/dataplatform --stage pr --baseline "$(REALM)/shiftleft-baseline.json" --md build/shiftleft-kernel.md
	$(SL) scan "$(REALM)" --stage pr --md build/shiftleft-realm.md
	$(SL) scan realms/databricks --stage pr --md build/shiftleft-databricks.md

ci-test:
	PYTHONPATH=src nice $(UV) run -q --no-project --python $(CI_PY) --with pytest --with pyyaml --with boto3 \
		--with 'moto[s3,kinesis]' pytest -q -p no:cacheprovider -o addopts="" tests/dataplatform tests/aws_databricks
	PYTHONPATH=src nice $(UV) run -q --no-project --python $(CI_PY) --with pytest --with pyyaml $(DUCK_DEPS) \
		pytest -q -p no:cacheprovider -o addopts="" --engine duckdb $(CONFORMANCE)

sbom:
	$(UV) run cyclonedx-py environment --output-format json -o sbom.json
	@echo "SBOM written to sbom.json"

clean:
	rm -rf build dist .pytest_cache .mypy_cache .ruff_cache htmlcov \
	       coverage.xml bandit.json semgrep.sarif sbom.json
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# The explainer ships with the platform: a static tour of this repository, built offline, every code box cut from the real
# file and linked to its line. DP_CATALOG names the specification catalog checkout (default: a sibling folder).
explainer:
	python3 explainer/build.py --out build/explainer
	@echo "open build/explainer/index.html"

# ---------------------------------------------------------------------------
# Test tiers (docs/TEST-PLAN.md). The test dependencies are declared in tests/requirements-platform.txt.
# ---------------------------------------------------------------------------
TEST_ENV = PYTHONPATH=src nice $(UV) run -q --no-project --python 3.12 --with-requirements tests/requirements-platform.txt

# The reference model against every contract case (plain Python, seconds).
test-model:
	$(TEST_ENV) pytest -q -p no:cacheprovider -o addopts="" tests/dataplatform/test_history_model.py

# Property tests for bronze to silver history on the fast DuckDB engine: a fixed set of generated histories. More:
# HYPOTHESIS_MAX_EXAMPLES=300; skip the slow shrink step on a long exploration: HYPOTHESIS_SHRINK=0.
test-props:
	$(TEST_ENV) pytest -q -p no:cacheprovider -o addopts="" --engine duckdb tests/dataplatform/test_silver_properties.py \
		"tests/dataplatform/test_silver_stages.py::test_history_type2_more"
