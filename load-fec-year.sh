#!/usr/bin/env bash

# Variables for database credentials
DB_USER='postgres'
DB_PASS='climbing'
DB_NAME='fec_data'

# Path to the Python preprocessing script
PYTHON_SCRIPT_PATH='./preprocess_data.py'

# URL for S3 bucket with the data
S3_BUCKET_URL='https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads'

# List of dataset tuples (abbreviation and table name)
DATASET_TUPLES=(
   "cn candidate_master" \
   "ccl candidate_committee_linkages" \
   "webl house_senate_current_campaigns" \
   "cm committee_master" \
   "webk pac_summary" \
   "indiv individual_contributions" \
   "pas2 committee_candidate_contributions" \
   "oth committee_transactions" \
   "oppexp operating_expenditures"
)

# Create or update the database and user
create_db_and_user() {
  echo "Creating or updating user and database..."
  psql -U postgres -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASS' LOGIN;" 2>/dev/null || echo "User $DB_USER already exists."
  psql -U postgres -c "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null
  psql -U postgres -c "CREATE DATABASE $DB_NAME;"
  psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;"
  echo "Database $DB_NAME is ready and accessible by $DB_USER."
}

# Function to download and preprocess data for a specific year
download_and_preprocess_year() {
  year=$1
  mkdir -p "./data/$year"
  echo "Processing files for year $year:"
  for dataset_tuple in "${DATASET_TUPLES[@]}"
  do
    set -- $dataset_tuple
    fec_abbreviation=$1
    table_name=$2
    url=$(full_url "$fec_abbreviation" "$year")
    echo "Processing $table_name ..."
    curl -s "$url" | funzip | iconv -c -t UTF-8 | tr -d '\010' > "./data/$year/$table_name.txt"
  done
  echo "Preprocessing data... $tablename"
  python $PYTHON_SCRIPT_PATH "./data/$year" "./sql" $year

}

# Generate full URL for downloading datasets
full_url() {
  dataset_abbreviation=$1
  year=$2
  echo "$S3_BUCKET_URL/$year/$dataset_abbreviation${year: -2}.zip"
}

# Function to create tables from SQL definition files
pg_create_tables() {
  echo "Creating tables from SQL definition files..."
  for table_def_file in `find ./sql -type f -name "*.sql"`; do
    echo "Creating table from $table_def_file ..."
    psql -d $DB_NAME -f "$table_def_file"
  done
}

pg_load_year() {
  year=$1
  echo "Loading data for year $year ..."
  for dataset_tuple in "${DATASET_TUPLES[@]}"
  do
    set -- $dataset_tuple
    table_name=$2
    echo "Loading table $table_name into PostgreSQL..."
    pg_load_table_year "$table_name" "$year"
  done
}

pg_load_table_year() {
  table_name=$1
  year=$2
  file_path="$PWD/data/$year/$table_name.txt"
  temp_table_name="temp_${table_name}"

  echo "Creating temporary table ${temp_table_name}..."
  psql -d $DB_NAME -e -c "DROP TABLE IF EXISTS ${temp_table_name}; CREATE TABLE ${temp_table_name} (LIKE ${table_name} INCLUDING ALL);"

  echo "Loading data into temporary table ${temp_table_name} from ${file_path}..."
  psql -d $DB_NAME -e -c "\copy ${temp_table_name} FROM '${file_path}' WITH (FORMAT CSV, DELIMITER '|', HEADER TRUE, QUOTE E'\b');"

  echo "Inserting data from ${temp_table_name} to ${table_name}..."
  # Example insert command, may need to adjust based on table schema
  psql -d $DB_NAME -e -c "INSERT INTO ${table_name} SELECT * FROM ${temp_table_name} ON CONFLICT DO NOTHING;"

  echo "Dropping temporary table ${temp_table_name}..."
  psql -d $DB_NAME -e -c "DROP TABLE ${temp_table_name};"
}



# Adjusted Function to Drop Existing Tables and Then Create New Ones
pg_drop_and_create_tables() {
  echo "Dropping existing tables if they exist..."
  psql -d $DB_NAME -c "DROP TABLE IF EXISTS candidate_master, candidate_committee_linkages, house_senate_current_campaigns, committee_master, pac_summary, individual_contributions, committee_candidate_contributions, committee_transactions, operating_expenditures CASCADE;"

  echo "Creating tables from SQL definition files..."
  for table_def_file in `find ./sql -type f -name "*.sql"`; do
    echo "Creating table from $table_def_file ..."
    psql -d $DB_NAME -f "$table_def_file"
  done
}

# Main execution block adjustments
create_db_and_user
pg_drop_and_create_tables  # Updated function call

for year in "$@"
do
  if (( $year % 2 == 0 ))
  then
    echo "Processing FEC data for year $year..."
    download_and_preprocess_year "$year"
    pg_load_year "$year"
  else
    echo "FEC data is indexed by federal election cycles, which occur every other year."
  fi
done