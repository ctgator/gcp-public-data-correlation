# ============================================================================
# Configuration
# ============================================================================

PROJECT_ID ?= pub-data-correlation-dev
DATASET    ?= raw
TABLE      ?= raw_pageviews

SQL_DIR    := sql
CREATE_SQL := $(SQL_DIR)/create_raw_pageviews_table.sql

BQ := bq --project_id=$(PROJECT_ID)

# ============================================================================
# Targets
# ============================================================================

.PHONY: help create drop reset

help:
	@echo ""
	@echo "Available targets:"
	@echo "  make create   - Create raw pageviews table"
	@echo "  make drop     - Drop raw pageviews table"
	@echo "  make reset    - Drop and recreate raw pageviews table"
	@echo ""
	@echo "Configurable variables:"
	@echo "  PROJECT_ID=$(PROJECT_ID)"
	@echo "  DATASET=$(DATASET)"
	@echo "  TABLE=$(TABLE)"
	@echo ""

# --------------------------------------------------------------------------
# Create table
# --------------------------------------------------------------------------

create:
	@echo "Creating table $(PROJECT_ID):$(DATASET).$(TABLE)"
	$(BQ) query --use_legacy_sql=false < $(CREATE_SQL)

# --------------------------------------------------------------------------
# Drop table
# --------------------------------------------------------------------------

drop:
	@echo "Dropping table $(PROJECT_ID):$(DATASET).$(TABLE)"
	$(BQ) rm -f -t $(PROJECT_ID):$(DATASET).$(TABLE)

# --------------------------------------------------------------------------
# Reset table (drop + create)
# --------------------------------------------------------------------------

reset: drop create
	@echo "Reset complete: $(PROJECT_ID):$(DATASET).$(TABLE)"

