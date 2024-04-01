
-- https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/

-- Recommended: Play with the data and build indices based on your planned access patterns.

CREATE TABLE IF NOT EXISTS individual_contributions (
    cmte_id TEXT NOT NULL, 
    amndt_ind TEXT, 
    rpt_tp TEXT, 
    transaction_pgi TEXT, 
    image_num TEXT, 
    transaction_tp TEXT, 
    entity_tp TEXT, 
    name TEXT, 
    city TEXT, 
    state TEXT, 
    zip_code TEXT, 
    employer TEXT, 
    occupation TEXT, 
    transaction_dt TEXT, -- Keep as text to store original MMDDYYYY format data
    transaction_amt NUMERIC, 
    other_id TEXT, 
    tran_id TEXT, 
    file_num INTEGER, 
    memo_cd TEXT, 
    memo_text TEXT, 
    sub_id TEXT, 
    donor_latitude NUMERIC, 
    donor_longitude NUMERIC, 
    formatted_transaction_dt DATE, -- Formatted transaction date
    periodicity INTEGER, -- Assuming this represents the gaps in days between contributions
    total_transaction_amt NUMERIC, -- Total amount of transactions
    average_periodicity NUMERIC, -- Average periodicity in days
    transaction_dates DATE[], -- Array of transaction dates
    transaction_amounts NUMERIC[], -- Assuming you want an array of transaction amounts
    file_year INTEGER,
    PRIMARY KEY (sub_id, file_year)
);
