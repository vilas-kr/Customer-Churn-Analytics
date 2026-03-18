import logging
import service.redshift as redshift

logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------
# Create staging table in Redshift
# -----------------------------------------------------------------------
# table for staging customer churn data
CREATE_TABLE_QUERY = '''
    CREATE TABLE IF NOT EXISTS staging_customer_churn (
        customer_id VARCHAR(20),
        gender VARCHAR(10),
        age INT,
        married VARCHAR(5),
        number_of_dependents INT,
        city VARCHAR(100),
        zip_code VARCHAR(10),
        latitude FLOAT,
        longitude FLOAT,
        number_of_referrals INT,
        tenure_in_months INT,
        offer VARCHAR(20),
        phone_service VARCHAR(10),
        avg_monthly_long_distance_charges FLOAT,
        multiple_lines VARCHAR(10),
        internet_service VARCHAR(10),
        internet_type VARCHAR(50),
        avg_monthly_gb_download INT,
        online_security VARCHAR(10),
        online_backup VARCHAR(10),
        device_protection_plan VARCHAR(10),
        premium_tech_support VARCHAR(10),
        streaming_tv VARCHAR(10),
        streaming_movies VARCHAR(10),
        streaming_music VARCHAR(10),
        unlimited_data VARCHAR(10),
        contract VARCHAR(20),
        paperless_billing VARCHAR(10),
        payment_method VARCHAR(50),
        monthly_charge FLOAT,
        total_charges FLOAT,
        total_refunds FLOAT,
        total_extra_data_charges FLOAT,
        total_long_distance_charges FLOAT,
        total_revenue FLOAT,
        customer_status VARCHAR(20),
        churn_category VARCHAR(50),
        churn_reason VARCHAR(200)
    );
'''
result = redshift.execute_query(CREATE_TABLE_QUERY)
if result:
    logging.info("Table 'staging_customer_churn' created successfully.")
else:
    logging.error("Failed to create table 'staging_customer_churn'.")
    
# -----------------------------------------------------------------------
# Create customer churn table in Redshift
# -----------------------------------------------------------------------
CREATE_TABLE_QUERY = '''
    CREATE TABLE IF NOT EXISTS customer_churn (
        customer_id VARCHAR(20),
        gender VARCHAR(10),
        age INT,
        city VARCHAR(50),
        zip_code VARCHAR(10),
        tenure INT,
        monthly_charges FLOAT8,
        total_charges FLOAT8,
        customer_status VARCHAR(20)
    );
'''
result = redshift.execute_query(CREATE_TABLE_QUERY)
if result:
    logging.info("Table 'customer_churn' created successfully.")
else:
    logging.error("Failed to create table 'customer_churn'.")
    
# -----------------------------------------------------------------------
# Create zip population table in Redshift
# -----------------------------------------------------------------------
CREATE_TABLE_QUERY = '''
    CREATE TABLE IF NOT EXISTS zip_population (
        zip_code VARCHAR(10),
        population INT
    );
'''
result = redshift.execute_query(CREATE_TABLE_QUERY)
if result:
    logging.info("Table 'zip_population' created successfully.")
else:
    logging.error("Failed to create table 'zip_population'.")

    