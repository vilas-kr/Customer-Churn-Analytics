import logging

from service import redshift
from config.settings import BUCKET_NAME, IAM_ROLE_ARN

logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------
# Load data from S3 to Redshift staging table
# -----------------------------------------------------------------------
s3_key = f's3://{BUCKET_NAME}/raw/telecom_customer_churn.csv'
iam_role_arn = IAM_ROLE_ARN

# Clear staging table before loading new data
TRUNCATE_STAGING_CUSTOMER_CHURN = 'TRUNCATE TABLE staging_customer_churn;'

# Execute truncation query
result = redshift.execute_query(TRUNCATE_STAGING_CUSTOMER_CHURN)
if result:
    logging.info("Staging table truncated successfully.")
else:
    logging.error("Failed to truncate staging table.")
    
COPY_QUERY = '''
    COPY staging_customer_churn
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS CSV
    DELIMITER ','
    EMPTYASNULL
    BLANKSASNULL
    IGNOREHEADER 1;
'''.format(s3_key, iam_role_arn)

result = redshift.execute_query(COPY_QUERY)
if result:
    logging.info("Data loaded successfully into 'staging_customer_churn' table.")
else:
    logging.error("Failed to load data into 'staging_customer_churn' table.")

# -------------------------------------------------------------------------
# Load zip code population data from S3 to Redshift staging table
# -------------------------------------------------------------------------
s3_key = f's3://{BUCKET_NAME}/raw/telecom_zipcode_population.csv'

# Clear zip_population table before loading new data
TRUNCATE_ZIP_POPULATION = 'TRUNCATE TABLE zip_population;'
result = redshift.execute_query(TRUNCATE_ZIP_POPULATION)
if result:
    logging.info("Zip population table truncated successfully.")
else:
    logging.error("Failed to truncate zip population table.")
    
COPY_QUERY = '''
    COPY zip_population
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS CSV
    DELIMITER ','
    EMPTYASNULL
    BLANKSASNULL
    IGNOREHEADER 1;
'''.format(s3_key, iam_role_arn)
result = redshift.execute_query(COPY_QUERY)
if result:
    logging.info("Data loaded successfully into 'zip_population' table.")
else:
    logging.error("Failed to load data into 'zip_population' table.")
    
# -----------------------------------------------------------------------
# Load data from staging table to final customer churn table in Redshift
# -----------------------------------------------------------------------
# Clear customer_churn table before loading new data
TRUNCATE_CUSTOMER_CHURN = 'TRUNCATE TABLE customer_churn;'
result = redshift.execute_query(TRUNCATE_CUSTOMER_CHURN)
if result:
    logging.info("Customer churn table truncated successfully.")
else:
    logging.error("Failed to truncate customer churn table.")
    
INSERT_QUERY = '''
    INSERT INTO customer_churn (
        customer_id,
        gender,
        age,
        city,
        zip_code,
        tenure,
        monthly_charges,
        total_charges,
        customer_status
    )
    SELECT 
        customer_id,
        gender,
        age,
        city,
        zip_code,
        tenure_in_months,
        monthly_charge,
        total_charges,
        customer_status
    FROM staging_customer_churn;
'''
result = redshift.execute_query(INSERT_QUERY)
if result:
    logging.info("Data inserted successfully into 'customer_churn' table.")
else:
    logging.error("Failed to insert data into 'customer_churn' table.")
    
            