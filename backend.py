from flask import Flask, request, jsonify
from flask_cors import CORS  # Import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import pgeocode
import pandas as pd  # Make sure pandas is imported


app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database connection parameters
DB_HOST = "localhost"
DB_NAME = "fec_data"
DB_USER = "postgres"
DB_PASSWORD = "climbing"

# Establish a database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, cursor_factory=RealDictCursor)
    except psycopg2.Error as e:
        print(f"Error: Could not connect to the database: {e}")
        return None
    return conn

@app.route('/committee-contributions', methods=['GET'])
def get_committee_contributions():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            SUM(ccc.transaction_amt) AS transaction_total,
            cm.cmte_nm AS committee_name,
            cm.file_year,
            cand.cand_name AS candidate_name
        FROM committee_candidate_contributions ccc
        JOIN committee_master cm ON cm.cmte_id = ccc.cmte_id AND cm.file_year = ccc.file_year
        JOIN candidate_master cand ON cand.cand_id = ccc.cand_id AND cand.file_year = ccc.file_year
        WHERE (ccc.transaction_tp = '24A' OR ccc.transaction_tp = '24N')
        GROUP BY cm.file_year, ccc.cand_id, cand.cand_name, cm.cmte_nm
        ORDER BY transaction_total DESC
        LIMIT 100
    """)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(results)

@app.route('/candidates/names', methods=['GET'])
def get_candidate_names():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT cand_name, cand_id, cand_name, cand_pty_affiliation, cand_election_yr, cand_office_st, cand_office, cand_office_district, cand_ici, cand_status, cand_pcc, cand_st1, cand_st2, cand_city, cand_st, cand_zip, candidate_latitude, candidate_longitude, file_year
        FROM candidate_master
        ORDER BY cand_name ASC
    """)
    candidates = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(candidates)


@app.route('/individual-contributions/all', methods=['GET'])
def get_all_individual_contributions():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            ci.zip_code AS zipcode,
            ci.name,
            ci.transaction_dt,
            ci.transaction_amt,
            cm.cmte_nm,
            cm.tres_nm,
            ccl.cand_id,
            cand.cand_name,
            cand.cand_pty_affiliation,
            cand.cand_zip
        FROM individual_contributions ci
        JOIN committee_master cm ON ci.cmte_id = cm.cmte_id
        JOIN candidate_committee_linkages ccl ON ci.cmte_id = ccl.cmte_id
        JOIN candidate_master cand ON ccl.cand_id = cand.cand_id
        ORDER BY ci.transaction_dt DESC
        LIMIT 1000
    """)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(results)

@app.route('/individual-contributions', methods=['GET'])
def get_individual_contributions():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT employer, file_year, COUNT(*) AS distinct_contributions
        FROM individual_contributions ci
        JOIN committee_master cm USING(cmte_id, file_year)
        GROUP BY employer, file_year
        ORDER BY distinct_contributions DESC
        LIMIT 100
    """)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(results)

@app.route('/contributions/by-candidate', methods=['GET'])
def contributions_by_candidate():
    candidate_name = request.args.get('name')  # Get candidate name from URL parameter
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Execute the query with case-insensitive search for candidate name
    
    cursor.execute("""
        WITH candidate_ids AS (
            SELECT cand_id, cand_name, cand_zip, candidate_latitude, candidate_longitude
            FROM candidate_master
            WHERE cand_name ILIKE %s
        ),
        committee_ids AS (
            SELECT ccl.cand_id, ccl.cmte_id
            FROM candidate_committee_linkages ccl
            JOIN candidate_ids ON candidate_ids.cand_id = ccl.cand_id
        ),
        candidate_transaction_sums AS (
            SELECT 
                committee_ids.cand_id,
                SUM(ci.transaction_amt) AS total_candidate_amt
            FROM individual_contributions ci
            JOIN committee_ids ON ci.cmte_id = committee_ids.cmte_id
            GROUP BY committee_ids.cand_id
        )
        SELECT 
            ci.*,
            cm.cand_name, 
            cm.cand_zip, 
            cm.candidate_latitude, 
            cm.candidate_longitude,
            cts.total_candidate_amt
        FROM individual_contributions ci
        JOIN committee_ids ON ci.cmte_id = committee_ids.cmte_id
        JOIN candidate_ids cm ON committee_ids.cand_id = cm.cand_id
        JOIN candidate_transaction_sums cts ON cm.cand_id = cts.cand_id
        ORDER BY ci.transaction_dt DESC;
    """, ('%' + candidate_name + '%',))

    contributions = cursor.fetchall()
    
    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Initialize pgeocode for the United States
    # nomi = pgeocode.Nominatim('us')
    
    # enhanced_contributions = []
    # Initialize caches for candidate and donor locations
    # candidate_locations_cache = {}
    # doner_locations_cache = {}

    # for contribution in contributions:
    #     # Get zip codes from your data
    #     doner_zipcode = contribution['zip_code'][0:5]
    #     candidate_zipcode = contribution['cand_zip'][0:5]

    #     # Check cache for candidate location
    #     if candidate_zipcode in candidate_locations_cache:
    #         candidate_location = candidate_locations_cache[candidate_zipcode]
    #     else:
    #         # If not in cache, query and update cache
    #         candidate_location = nomi.query_postal_code(candidate_zipcode)
    #         candidate_locations_cache[candidate_zipcode] = candidate_location

    #     # Check cache for donor location
    #     if doner_zipcode in doner_locations_cache:
    #         doner_location = doner_locations_cache[doner_zipcode]
    #     else:
    #         # If not in cache, query and update cache
    #         doner_location = nomi.query_postal_code(doner_zipcode)
    #         doner_locations_cache[doner_zipcode] = doner_location

        # Process locations
        # Assuming `candidate_location` and `doner_location` are Series objects returned from `pgeocode`
        # if candidate_location is not None and not candidate_location.empty:
        #     # Check if latitude or longitude are NaN. Adjust field names if necessary.
        #     if pd.notna(candidate_location.latitude) and pd.notna(candidate_location.longitude):
        #         candidate_lat, candidate_lon = candidate_location.latitude, candidate_location.longitude
        #     else:
        #         candidate_lat, candidate_lon = 0.0, 0.0

        # if doner_location is not None and not doner_location.empty:
        #     # Same check for donor location
        #     if pd.notna(doner_location.latitude) and pd.notna(doner_location.longitude):
        #         doner_lat, doner_lon = doner_location.latitude, doner_location.longitude
        #     else:
        #         doner_lat, doner_lon = 0.0, 0.0

        # Add lat/lon to the contribution data
        # contribution['candidate_latitude'] = candidate_lat
        # contribution['candidate_longitude'] = candidate_lon
        # contribution['doner_latitude'] = doner_lat
        # contribution['doner_longitude'] = doner_lon

        # enhanced_contributions.append(contribution)
        # print(contribution)

    print(candidate_name, contributions)
    return jsonify(contributions)

if __name__ == '__main__':
    app.run(debug=True)
