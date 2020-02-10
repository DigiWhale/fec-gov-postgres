# Load FEC Bulk Data into Postgres

The FEC publishes individual and PAC contributions to candidates in a
[searchable web app](https://www.fec.gov/data/) and as
[bulk downloads](https://www.fec.gov/data/browse-data/?tab=bulk-data). The goal of this project
is to make the published bulk data easily accessible for analysis in a database.

## Getting Started

This repo consists of three main functional components right now:
1. A set of postgres table definitions that are compatible with the schema published by the FEC and
   the data available in their bulk downloads. These are annotated with the comments in the
   FEC's data definition files.
2. A postgres function to load data from a flat file from the FEC into one of the tables defined by (1.)
3. A bash function that downloads files from the FEC's public S3 bucket.

All of these components are stitched together in `load-fec-year.sh` and it can be used like this
to load download data from 2020 and load it into a postgres database:
```bash
./load-fec-year.sh 2020
```

If this fails you may need to run `chmod +x ./load-fec-year.sh`

Specify postgres configurations in environment variables by following the official
[postgres documentation](https://www.postgresql.org/docs/9.3/libpq-envars.html).

## Schema Changes

All tables have an additional column added called `file_year`. This corresponds to the election
cycle that the bulk download files were associated with. Adding `file_year` makes it easy to add
files from multiple election cycles into the same database and run queries across all of them.

## Starter Queries

The [FEC documentation](https://www.fec.gov/data/browse-data/?tab=bulk-data) is very thorough so reviewing that is essential for
navigating the schema -- just be aware that the `file_year` is being added to all tables.

For example, this query will show expenditures by committees in opposition of a candidate
by election cycle:

```sql
SELECT
    SUM(ccc.transaction_amt) transaction_total,
    cm.cmte_nm committee_name,
    cm.file_year,
    cand.cand_name candidate_name
FROM committee_candidate_contributions ccc
JOIN committee_master cm
ON cm.cmte_id = ccc.cmte_id AND cm.file_year = ccc.file_year
JOIN candidate_master cand
ON cand.cand_id = ccc.cand_id AND cand.file_year = ccc.file_year
WHERE (ccc.transaction_tp = '24A' OR ccc.transaction_tp = '24N') -- https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/
GROUP BY
    cm.file_year,
    ccc.cand_id,
    cand.cand_name,
    cm.cmte_nm
ORDER BY transaction_total DESC
LIMIT 100
```