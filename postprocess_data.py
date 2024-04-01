
import psycopg2
import pandas as pd
import datetime
import requests
import zipfile
import io
import xml.etree.ElementTree as ET

def normalize_date(date_str):
    print("date_str: ", date_str)
    if pd.isnull(date_str) or len(str(date_str)) < 7:
        return '2024-01-01'  # Default date for invalid or missing data
    
    date_str = str(int(float(date_str)))  # Handle the decimal and convert to string
    year = date_str[-4:]
    
    if len(date_str) == 7:  # Assuming MDDYYYY format
        month = date_str[:-6]
        day = date_str[1:3]
    elif len(date_str) == 8:  # Assuming MMDDYYYY format
        month = date_str[:-6]
        day = date_str[2:4]
    else:
        return '2024-01-01'  # Default date for any other unexpected format
    
    if len(month) < 2:
        month = '0' + month  # Prepend 0 if month is a single digit
    print(f'{year}-{month}-{day}')
    return f'{year}-{month}-{day}'

def calculate_metrics(conn):
    cur = conn.cursor()
    # SQL query to calculate and update metrics
    cur.execute("""
        WITH ranked_contributions AS (
            SELECT
                sub_id,
                name,
                zip_code,
                formatted_transaction_dt,
                transaction_amt,
                LAG(formatted_transaction_dt) OVER (
                    PARTITION BY name, zip_code ORDER BY formatted_transaction_dt
                ) AS prev_transaction_dt
            FROM individual_contributions
        ),
        periodicity_calculations AS (
            SELECT
                name,
                zip_code,
                sub_id,
                formatted_transaction_dt,
                transaction_amt,
                prev_transaction_dt,
                CASE 
                    WHEN formatted_transaction_dt IS NOT NULL AND prev_transaction_dt IS NOT NULL THEN 
                        (formatted_transaction_dt - prev_transaction_dt) 
                    ELSE NULL
                END AS periodicity_days
            FROM ranked_contributions
        ),
        aggregated_metrics AS (
            SELECT
                pc.name,
                pc.zip_code,
                SUM(pc.transaction_amt) AS total_transaction_amt,
                AVG(pc.periodicity_days) FILTER (WHERE pc.periodicity_days IS NOT NULL) AS average_periodicity,
                ARRAY_AGG(pc.formatted_transaction_dt ORDER BY pc.formatted_transaction_dt) FILTER (WHERE pc.formatted_transaction_dt IS NOT NULL) AS transaction_dates
            FROM periodicity_calculations pc
            GROUP BY pc.name, pc.zip_code
        )
        UPDATE individual_contributions ic SET
            total_transaction_amt = am.total_transaction_amt,
            average_periodicity = am.average_periodicity,
            transaction_dates = am.transaction_dates
        FROM aggregated_metrics am
        WHERE ic.name = am.name AND ic.zip_code = am.zip_code;

    """)
    cur.close()

def convert_date_format(date_float_str):
    """
    Converts a date string from 'MDDYYYY.0' or 'MMDDYYYY.0' to 'YYYY-MM-DD' format.
    """
    if not date_float_str or date_float_str.lower() == 'null':
        return None

    # Remove the decimal part and zero-pad the string to ensure it's at least 8 characters long
    date_str = str(int(float(date_float_str))).zfill(7)  # Ensure at least 7 digits for MDDYYYY
    
    # Correctly format the string to 'MMDDYYYY' by zero-padding
    if len(date_str) == 7:  # MDDYYYY
        date_str = '0' + date_str  # Zero-pad month
    
    # Parse the date
    # date_obj = datetime.strptime(date_str, '%m%d%Y')
    date_obj = datetime.datetime.strptime(date_str, '%m%d%Y')
    
    # Convert to 'YYYY-MM-DD' format
    return date_obj.strftime('%Y-%m-%d')

def update_formatted_transaction_dt(conn):
    """
    Fetches each transaction_dt, converts it to the correct date format, 
    and updates the formatted_transaction_dt in the database.
    """
    # Connect to the database
    cur = conn.cursor()

    # Fetch transaction_dt values
    cur.execute("SELECT sub_id, transaction_dt FROM individual_contributions WHERE transaction_dt IS NOT NULL AND transaction_dt != 'NULL'")
    rows = cur.fetchall()

    # Prepare update query
    update_query = "UPDATE individual_contributions SET formatted_transaction_dt = %s WHERE sub_id = %s"
    
    # Process and update each row
    for sub_id, transaction_dt in rows:
        # Convert the date format
        formatted_date = convert_date_format(transaction_dt)
        if formatted_date:  # Proceed only if conversion was successful
            cur.execute(update_query, (formatted_date, sub_id))
            print(f"Updated sub_id {sub_id}: {transaction_dt} -> {formatted_date}")

    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    
def calculate_transaction_dates(conn):
    cur = conn.cursor()

    # Use SQL to aggregate transaction dates and amounts, and update them in a single operation
    cur.execute("""
    WITH contributions_agg AS (
        SELECT
            name,
            zip_code,
            ARRAY_AGG(formatted_transaction_dt ORDER BY formatted_transaction_dt) AS transaction_dates,
            ARRAY_AGG(transaction_amt ORDER BY formatted_transaction_dt) AS transaction_amounts
        FROM individual_contributions
        GROUP BY name, zip_code
    )
    UPDATE individual_contributions ic
    SET
        transaction_dates = ca.transaction_dates,
        transaction_amounts = ca.transaction_amounts
    FROM contributions_agg ca
    WHERE ic.name = ca.name AND ic.zip_code = ca.zip_code;
    """)

    conn.commit()

def calculate_periodicity(conn):
    sql_command = """
    DO $$
    DECLARE
        rec record;
    BEGIN
        FOR rec IN (SELECT sub_id, transaction_dates FROM individual_contributions WHERE cardinality(transaction_dates) > 1)
        LOOP
            UPDATE individual_contributions ic SET periodicity = sub.avg_periodicity
            FROM (
                SELECT avg(diff) as avg_periodicity
                FROM (
                    SELECT sub_id, dt, lead(dt) OVER (ORDER BY dt) - dt as diff
                    FROM (
                        SELECT sub_id, unnest(transaction_dates) as dt
                        FROM individual_contributions
                        WHERE sub_id = rec.sub_id
                    ) s1
                ) s2
                WHERE diff IS NOT NULL
            ) sub
            WHERE ic.sub_id = rec.sub_id;
        END LOOP;
    END $$;
    """

    cur = conn.cursor()
    cur.execute(sql_command)
    conn.commit()
    
def set_committee_totals(conn):
    # First, add the new column if it doesn't exist
    add_column_command = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name='candidate_committee_linkages' AND column_name='committee_total'
        ) THEN
            ALTER TABLE candidate_committee_linkages ADD COLUMN committee_total NUMERIC;
        END IF;
    END $$;
    """
    
    # Then, update the committee_total column with the sum of transaction amounts
    update_totals_command = """
    DO $$
    BEGIN
        UPDATE candidate_committee_linkages ccl
        SET committee_total = ct.total_amt
        FROM (
            SELECT cmte_id, SUM(transaction_amt) AS total_amt
            FROM committee_transactions
            GROUP BY cmte_id
        ) ct
        WHERE ccl.cmte_id = ct.cmte_id;
    END $$;
    """

    cur = conn.cursor()
    # Execute the command to add the column if it doesn't exist
    cur.execute(add_column_command)
    # Execute the command to update totals
    cur.execute(update_totals_command)
    conn.commit()
    
def create_committee_grid_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS committee_grid (
            com_nam VARCHAR(255),
            com_id VARCHAR(50) PRIMARY KEY,
            com_typ CHAR(1),
            com_des CHAR(1),
            org_tp CHAR(1),
            tot_rec NUMERIC,
            tot_dis NUMERIC,
            cas_on_han_clo_of_per NUMERIC,
            cov_end_dat DATE
        );
    """)
    conn.commit()
    cur.close()
    
def create_candidate_grid_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidate_grid (
            can_nam VARCHAR(255),
            lin_ima VARCHAR(255),
            can_off CHAR(1),
            can_off_sta CHAR(2),
            can_off_dis VARCHAR(10),
            can_par_aff VARCHAR(50),
            can_inc_cha_ope_sea VARCHAR(50),
            tot_rec NUMERIC,
            tot_dis NUMERIC,
            cas_on_han_clo_of_per NUMERIC,
            deb_owe_by_com NUMERIC,
            cov_end_dat DATE
        );
    """)
    conn.commit()
    cur.close()

def download_and_import_committee_grid(conn, year):
    # Download ZIP file
    url = f"https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/{year}/committee_summary_grid{year}.zip"
    response = requests.get(url)
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    
    # Extract XML file from ZIP archive
    zip_file.extractall("/tmp")  # Extract to /tmp directory or change as needed

    # Assuming there's only one XML file in the ZIP
    xml_file_name = zip_file.namelist()[0]
    xml_path = f"/tmp/{xml_file_name}"

    # Parse XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Connect to PostgreSQL database and insert data
    cursor = conn.cursor()

    for com_sum in root.findall('com_sum'):
        com_nam = com_sum.find('com_nam').text
        com_id = com_sum.find('com_id').text
        com_typ = com_sum.find('com_typ').text if com_sum.find('com_typ') is not None else None
        com_des = com_sum.find('com_des').text if com_sum.find('com_des') is not None else None
        org_tp = com_sum.find('org_tp').text if com_sum.find('org_tp') is not None else None
        tot_rec = float(com_sum.find('tot_rec').text) if com_sum.find('tot_rec') is not None and com_sum.find('tot_rec').text is not None else 0
        tot_dis = float(com_sum.find('tot_dis').text) if com_sum.find('tot_dis') is not None and com_sum.find('tot_dis').text is not None else 0
        cas_on_han_clo_of_per = float(com_sum.find('cas_on_han_clo_of_per').text) if com_sum.find('cas_on_han_clo_of_per') is not None and com_sum.find('cas_on_han_clo_of_per').text is not None else 0
        cov_end_dat = com_sum.find('cov_end_dat').text if com_sum.find('cov_end_dat') is not None else None


        # Insert data into your database
        # Ensure you have created a table with the appropriate schema to store this data
        cursor.execute("""
            INSERT INTO committee_grid (com_nam, com_id, com_typ, com_des, org_tp, tot_rec, tot_dis, cas_on_han_clo_of_per, cov_end_dat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (com_id) DO NOTHING;  -- Assuming com_id is a unique identifier
        """, (com_nam, com_id, com_typ, com_des, org_tp, tot_rec, tot_dis, cas_on_han_clo_of_per, cov_end_dat))

    # Commit the transaction and close the cursor
    conn.commit()
    cursor.close()

def download_and_import_candidate_grid(conn, year):
    # Download ZIP file
    url = f"https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/{year}/candidate_summary_grid{year}.zip"
    response = requests.get(url)
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    
    # Extract XML file from ZIP archive
    zip_file.extractall("/tmp")  # Extract to /tmp directory or change as needed

    # Assuming there's only one XML file in the ZIP
    xml_file_name = zip_file.namelist()[0]
    xml_path = f"/tmp/{xml_file_name}"

    # Parse XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Connect to PostgreSQL database and insert data
    cursor = conn.cursor()

    for candidate in root.findall('candidate'):
        # Extract data for each candidate
        can_nam = candidate.find('can_nam').text
        lin_ima = candidate.find('lin_ima').text if candidate.find('lin_ima') is not None else None
        can_off = candidate.find('can_off').text if candidate.find('can_off') is not None else None
        can_off_sta = candidate.find('can_off_sta').text if candidate.find('can_off_sta') is not None else None
        can_off_dis = candidate.find('can_off_dis').text if candidate.find('can_off_dis') is not None else None
        can_par_aff = candidate.find('can_par_aff').text if candidate.find('can_par_aff') is not None else None
        can_inc_cha_ope_sea = candidate.find('can_inc_cha_ope_sea').text if candidate.find('can_inc_cha_ope_sea') is not None else None
        tot_rec = float(candidate.find('tot_rec').text) if candidate.find('tot_rec') is not None and candidate.find('tot_rec').text is not None else 0
        tot_dis = float(candidate.find('tot_dis').text) if candidate.find('tot_dis') is not None and candidate.find('tot_dis').text is not None else 0
        cas_on_han_clo_of_per = float(candidate.find('cas_on_han_clo_of_per').text) if candidate.find('cas_on_han_clo_of_per') is not None and candidate.find('cas_on_han_clo_of_per').text is not None else 0
        deb_owe_by_com = float(candidate.find('deb_owe_by_com').text) if candidate.find('deb_owe_by_com') is not None and candidate.find('deb_owe_by_com').text is not None else 0
        cov_end_dat = candidate.find('cov_end_dat').text if candidate.find('cov_end_dat') is not None else None

        # Insert data into your database
        cursor.execute("""
            INSERT INTO candidate_grid (can_nam, lin_ima, can_off, can_off_sta, can_off_dis, can_par_aff, can_inc_cha_ope_sea, tot_rec, tot_dis, cas_on_han_clo_of_per, deb_owe_by_com, cov_end_dat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (can_nam, lin_ima, can_off, can_off_sta, can_off_dis, can_par_aff, can_inc_cha_ope_sea, tot_rec, tot_dis, cas_on_han_clo_of_per, deb_owe_by_com, cov_end_dat))
    
    conn.commit()
    cursor.close()

def drop_table(conn, table_name):
    """
    Drop a table from the PostgreSQL database.
    
    Parameters:
    - conn: A connection object to the PostgreSQL database.
    - table_name: The name of the table to be dropped.
    """
    try:
        # Open a cursor to perform database operations
        cur = conn.cursor()
        
        # Construct the SQL command
        drop_command = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        
        # Execute the command
        cur.execute(drop_command)
        
        # Commit the changes
        conn.commit()
        
        print(f"Table {table_name} has been dropped successfully.")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        # Close the cursor
        if cur is not None:
            cur.close()

def main():
    # Database connection details
    db_name = "fec_data"
    db_user = "postgres"
    db_password = "climbing"

    # Connect to the database
    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password)

    # Normalize and Update Dates - Implement the normalization and update here
    # This would use the normalize_date function and execute_batch for updates

    # After updating the dates, calculate additional metrics
    # calculate_metrics(conn)
    # update_formatted_transaction_dt(conn)
    # calculate_transaction_dates(conn)
    # calculate_periodicity(conn)
    # set_committee_totals(conn)
    drop_table(conn, 'committee_grid')
    create_committee_grid_table(conn)
    download_and_import_committee_grid(conn, 2024)
    drop_table(conn, 'candidate_grid')
    create_candidate_grid_table(conn)
    download_and_import_candidate_grid(conn, 2024)

    # Commit changes and close the connection
    # conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
