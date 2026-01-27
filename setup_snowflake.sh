#!/bin/bash
# Setup script to create raw source tables in Snowflake

set -e

echo "======================================================================"
echo "Bain Capital dbt Project - Snowflake Setup"
echo "======================================================================"
echo ""

# Check if Snowflake environment variables are set
if [ -z "$SNOWFLAKE_ACCOUNT" ]; then
    echo "Error: SNOWFLAKE_ACCOUNT environment variable not set"
    echo "Usage: export SNOWFLAKE_ACCOUNT='your-account'"
    exit 1
fi

if [ -z "$SNOWFLAKE_USER" ]; then
    echo "Error: SNOWFLAKE_USER environment variable not set"
    echo "Usage: export SNOWFLAKE_USER='your-user'"
    exit 1
fi

if [ -z "$SNOWFLAKE_PASSWORD" ]; then
    echo "Error: SNOWFLAKE_PASSWORD environment variable not set"
    echo "Usage: export SNOWFLAKE_PASSWORD='your-password'"
    exit 1
fi

echo "✓ Environment variables set"
echo "  Account: $SNOWFLAKE_ACCOUNT"
echo "  User: $SNOWFLAKE_USER"
echo ""

# Check if snowsql is installed
if ! command -v snowsql &> /dev/null; then
    echo "Warning: snowsql not found. You'll need to run the SQL manually."
    echo ""
    echo "Option 1: Install snowsql"
    echo "  https://docs.snowflake.com/en/user-guide/snowsql-install-config.html"
    echo ""
    echo "Option 2: Copy/paste setup/create_raw_tables.sql into Snowflake UI"
    echo "  https://app.snowflake.com"
    echo ""
    exit 1
fi

echo "✓ snowsql found"
echo ""

# Execute the SQL script
echo "Creating raw source tables in Snowflake..."
echo "  Database: DBT_DEMO"
echo "  Schema: DEV"
echo ""

snowsql \
    -a "$SNOWFLAKE_ACCOUNT" \
    -u "$SNOWFLAKE_USER" \
    -f setup/create_raw_tables.sql \
    -o exit_on_error=true \
    -o friendly=false

if [ $? -eq 0 ]; then
    echo ""
    echo "======================================================================"
    echo "✓ Setup completed successfully!"
    echo "======================================================================"
    echo ""
    echo "Next steps:"
    echo "  1. Install dbt dependencies:  dbt deps"
    echo "  2. Load seed data:            dbt seed"
    echo "  3. Run all models:            dbt run --full-refresh"
    echo "  4. Run tests:                 dbt test"
    echo ""
    echo ""
else
    echo ""
    echo "======================================================================"
    echo "✗ Setup failed!"
    echo "======================================================================"
    echo ""
    echo "Please check the error messages above and try again."
    echo ""
    exit 1
fi
