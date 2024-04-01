import os
import pandas as pd
import pgeocode
from collections.abc import Iterable
import datetime

def preprocess_file(data_file_path, sql_file_path, year, tablename):
    column_names = extract_column_names_from_sql(sql_file_path)
    if 'file_year' not in column_names:
        column_names.extend(['file_year', 'recurring_contributions', 'periodicity', 'formatted_transaction_dt'])

    df = pd.read_csv(data_file_path, delimiter='|', header=None, low_memory=False)
    df = df.iloc[:, :len(column_names)]
    df.columns = column_names[:len(df.columns)]

    nomi = pgeocode.Nominatim('us')
    lat_cache, lon_cache = {}, {}
    apply_geocoding(df, 'cand_zip', lat_cache, lon_cache, nomi)
    apply_geocoding(df, 'zip_code', lat_cache, lon_cache, nomi, donor=True)
    print("Table name: ", tablename)
    if tablename == 'individual_contributions.txt':
        print("Processing individual contributions")
        # Normalize and format transaction_dt
        df['formatted_transaction_dt'] = df['transaction_dt'].apply(normalize_date)

        # Calculate recurring contributions and periodicity
        df = calculate_recurring_contributions_for_testing(df)

    df['file_year'] = year
    df.to_csv(data_file_path, index=False, sep='|')

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


def is_iterable(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, str)

def format_dates_for_sql(date_list):
    """
    Convert a list of dates to a string formatted as a PostgreSQL DATE[] array.
    Handles cases where date_list is not iterable (e.g., NaN or float).
    """
    if not is_iterable(date_list):
        # Handle non-iterable inputs gracefully; return an empty array representation
        return '{}'

    # Convert each date to a string in 'YYYY-MM-DD' format
    date_strs = [date.strftime('%Y-%m-%d') for date in date_list]
    # Format as a PostgreSQL array
    return "{" + ",".join(date_strs) + "}"


def calculate_recurring_contributions(df):
    # Ensure the 'transaction_dt' is in the correct datetime format
    df['formatted_transaction_dt'] = pd.to_datetime(df['formatted_transaction_dt'], errors='coerce')
    
    # Sort by name, zip_code, and formatted_transaction_dt
    df.sort_values(by=['name', 'zip_code', 'formatted_transaction_dt'], inplace=True)
    
    # Calculate gaps in contributions for periodicity
    df['prev_transaction_dt'] = df.groupby(['name', 'zip_code'])['formatted_transaction_dt'].shift(1)
    df['periodicity'] = (df['formatted_transaction_dt'] - df['prev_transaction_dt']).dt.days.fillna(0).astype(int)
    
    # Aggregate recurring dates and amounts
    agg_funcs = {
        'formatted_transaction_dt': lambda x: list(x),  # Corrected to aggregate dates into a list
        'transaction_amt': ['sum', lambda x: list(x)],  # Sum and list of transaction amounts
        'periodicity': 'mean',  # Average periodicity
    }
    df_agg = df.groupby(['name', 'zip_code']).agg(agg_funcs).reset_index()

    # Flatten MultiIndex columns resulting from aggregation
    df_agg.columns = ['_'.join(col).rstrip('_') if col[1] else col[0] for col in df_agg.columns.values]

    # Rename columns to reflect the aggregated data
    df_agg.rename(columns={
        'formatted_transaction_dt_<lambda>': 'transaction_dates',
        'transaction_amt_sum': 'total_transaction_amt',
        'transaction_amt_<lambda_0>': 'transaction_amounts',
        'periodicity_mean': 'average_periodicity',
    }, inplace=True)
    
    df_agg['transaction_amounts'] = df_agg['transaction_amounts'].apply(lambda x: '{' + ','.join(map(str, x)) + '}')

    
    # Merge the aggregated data back with the original dataframe on 'name' and 'zip_code'
    df_merged = pd.merge(df, df_agg, on=['name', 'zip_code'], how='left')

    # Apply format_dates_for_sql if 'transaction_dates' exists
    if 'transaction_dates' in df_merged.columns:
        df_merged['transaction_dates'] = df_merged['transaction_dates'].apply(format_dates_for_sql)
    else:
        print("Error: 'transaction_dates' column not found.")
    
    # Drop temporary column
    df_merged.drop(columns=['prev_transaction_dt'], inplace=True)
    
    return df_merged

def calculate_recurring_contributions_for_testing(df):
    # Setting a specific date for the date fields
    specific_date = datetime.date(2020, 1, 1)

    # Assign default values directly for scalar types
    df['formatted_transaction_dt'] = specific_date
    df['periodicity'] = 0
    df['total_transaction_amt'] = 0.0
    df['average_periodicity'] = 0.0

    # Assuming df is your existing DataFrame and you've already done other necessary operations on it
    df['transaction_dates'] = '{}'

    df['transaction_amounts'] = '{}'

    return df

def calculate_periodicity(df):
    # Assuming 'name' is the contributor's name and 'transaction_dt' is the transaction date
    df['transaction_dt'] = pd.to_datetime(df['transaction_dt'])
    df.sort_values(by=['name', 'transaction_dt'], inplace=True)

    # Calculate the difference in days between consecutive contributions for each contributor
    df['periodicity'] = df.groupby('name')['transaction_dt'].diff().dt.days

    # Fill NaN values with a default value or leave as is based on your requirements
    df['periodicity'] = df['periodicity'].fillna(0)  

def apply_geocoding(df, column_name, lat_cache, lon_cache, nomi, donor=False):
    if column_name in df.columns:
        df[f'{"donor_" if donor else "candidate_"}latitude'] = df[column_name].apply(lambda z: geocode(z, lat_cache, nomi))
        df[f'{"donor_" if donor else "candidate_"}longitude'] = df[column_name].apply(lambda z: geocode(z, lon_cache, nomi, lat=False))

def geocode(zip_code, cache, nomi, lat=True):
    if zip_code not in cache:
        result = nomi.query_postal_code(str(zip_code)[:5])
        cache[zip_code] = (result.latitude if pd.notnull(result.latitude) else 0.0, result.longitude if pd.notnull(result.longitude) else 0.0)
    print("zipcode: ", zip_code)
    return cache[zip_code][0 if lat else 1]

def extract_column_names_from_sql(sql_file_path):
    column_names = []
    with open(sql_file_path, 'r') as file:
        in_create_table_block = False
        for line in file:
            if line.strip().lower().startswith("create table"):
                in_create_table_block = True
            elif in_create_table_block and line.strip().startswith(");"):
                in_create_table_block = False
                break
            elif in_create_table_block:
                column_name = line.strip().split(" ")[0].replace("`", "").replace(",", "")
                column_names.append(column_name)
    return column_names

def preprocess_directory(data_directory, sql_directory, year):
    for item in os.listdir(data_directory):
        data_full_path = os.path.join(data_directory, item)
        if os.path.isdir(data_full_path):
            preprocess_directory(data_full_path, sql_directory, year)
        elif item.endswith(".txt"):
            print(f"Item {item }")
            sql_file_name = os.path.splitext(item)[0] + ".sql"
            sql_full_path = os.path.join(sql_directory, sql_file_name)
            print(f"Processing {data_full_path} for year {year}")
            preprocess_file(data_full_path, sql_full_path, year, item)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print(f"Usage: python script.py {sys.argv[1:4]}")
        sys.exit(1)
    
    data_directory, sql_directory, year = sys.argv[1:4]
    preprocess_directory(data_directory, sql_directory, year)
