#!/bin/bash
set -e

# Validate test outputs from Keboola platform tests
# This script checks that expected tables exist and have the correct structure

# Required environment variables
: "${KBC_HOST:?}"
: "${KBC_STORAGE_TOKEN:?}"
: "${COMPONENT_ID:?}"
: "${CONFIG_IDS:?}"

AUTH_HEADER="X-StorageApi-Token: ${KBC_STORAGE_TOKEN}"
BASE_URL="https://${KBC_HOST}/v2/storage"

# Convert component ID to bucket prefix (dots become hyphens)
BUCKET_PREFIX=$(echo "$COMPONENT_ID" | tr '.' '-')

FAILED_VALIDATIONS=()
SUCCESS_COUNT=0
TOTAL_COUNT=0

# Expected tables per component type
get_expected_tables() {
    local component_id=$1
    local config_id=$2
    
    case "$component_id" in
        keboola.ex-facebook-ads-v2)
            echo "accounts"
            ;;
        keboola.ex-facebook-pages)
            # Pages smoke test produces multiple tables
            echo "accounts"
            ;;
        keboola.ex-instagram-v2)
            echo "accounts"
            ;;
        *)
            echo "accounts"
            ;;
    esac
}

# Required columns per table
get_required_columns() {
    local table_name=$1
    
    case "$table_name" in
        accounts)
            echo "id name"
            ;;
        *)
            echo "id"
            ;;
    esac
}

# Split config IDs by space or comma and iterate
IFS=', ' read -ra CONFIG_ARRAY <<< "$CONFIG_IDS"

for CONFIG_ID in "${CONFIG_ARRAY[@]}"; do
    CONFIG_ID=$(echo "$CONFIG_ID" | xargs)
    [ -z "$CONFIG_ID" ] && continue
    
    BUCKET_ID="in.c-${BUCKET_PREFIX}-${CONFIG_ID}"
    
    echo "Validating outputs for ${COMPONENT_ID}/${CONFIG_ID}..."
    echo "  Bucket: ${BUCKET_ID}"
    
    # Get expected tables for this component
    EXPECTED_TABLES=$(get_expected_tables "$COMPONENT_ID" "$CONFIG_ID")
    
    for TABLE_NAME in $EXPECTED_TABLES; do
        TOTAL_COUNT=$((TOTAL_COUNT + 1))
        TABLE_ID="${BUCKET_ID}.${TABLE_NAME}"
        
        echo "  Checking table: ${TABLE_NAME}"
        
        # Fetch table details
        set +e
        TABLE_RESPONSE=$(curl -sf -H "${AUTH_HEADER}" "${BASE_URL}/tables/${TABLE_ID}")
        FETCH_EXIT=$?
        set -e
        
        if [ $FETCH_EXIT -ne 0 ]; then
            echo "    FAIL: Table ${TABLE_NAME} does not exist"
            FAILED_VALIDATIONS+=("${CONFIG_ID}:${TABLE_NAME}:missing")
            continue
        fi
        
        # Check row count
        ROW_COUNT=$(echo "$TABLE_RESPONSE" | jq -r '.rowsCount // 0')
        if [ "$ROW_COUNT" -eq 0 ]; then
            echo "    WARN: Table ${TABLE_NAME} has 0 rows"
            # Don't fail on empty tables - some queries may legitimately return empty
        else
            echo "    OK: Table has ${ROW_COUNT} row(s)"
        fi
        
        # Check required columns
        REQUIRED_COLS=$(get_required_columns "$TABLE_NAME")
        ACTUAL_COLS=$(echo "$TABLE_RESPONSE" | jq -r '.columns[]' 2>/dev/null | tr '\n' ' ')
        
        MISSING_COLS=""
        for COL in $REQUIRED_COLS; do
            if ! echo "$ACTUAL_COLS" | grep -qw "$COL"; then
                MISSING_COLS="${MISSING_COLS} ${COL}"
            fi
        done
        
        if [ -n "$MISSING_COLS" ]; then
            echo "    FAIL: Missing required columns:${MISSING_COLS}"
            FAILED_VALIDATIONS+=("${CONFIG_ID}:${TABLE_NAME}:missing_columns${MISSING_COLS}")
        else
            echo "    OK: All required columns present"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        fi
        
        # Check freshness (table was updated recently - within last hour)
        LAST_IMPORT=$(echo "$TABLE_RESPONSE" | jq -r '.lastImportDate // empty')
        if [ -n "$LAST_IMPORT" ]; then
            # Convert to epoch and compare
            LAST_IMPORT_EPOCH=$(date -d "$LAST_IMPORT" +%s 2>/dev/null || echo "0")
            NOW_EPOCH=$(date +%s)
            AGE_SECONDS=$((NOW_EPOCH - LAST_IMPORT_EPOCH))
            AGE_MINUTES=$((AGE_SECONDS / 60))
            
            if [ $AGE_MINUTES -gt 60 ]; then
                echo "    WARN: Table was last updated ${AGE_MINUTES} minutes ago (may be stale)"
            else
                echo "    OK: Table freshness (${AGE_MINUTES} minutes ago)"
            fi
        fi
    done
done

echo ""
echo "Validation Summary: ${SUCCESS_COUNT}/${TOTAL_COUNT} tables validated successfully"

if [ ${#FAILED_VALIDATIONS[@]} -gt 0 ] && [ -n "${FAILED_VALIDATIONS[0]}" ]; then
    echo "Failed validations:"
    for FAIL in "${FAILED_VALIDATIONS[@]}"; do
        echo "  - $FAIL"
    done
    exit 1
fi

echo "All validations passed!"
exit 0
