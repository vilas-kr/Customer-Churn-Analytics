import logging

from service import redshift

logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------
# Create customer churn analytics table in Redshift
# -----------------------------------------------------------------------
CREATE_TABLE_QUERY = '''
    CREATE TABLE IF NOT EXISTS customer_churn_analytics (
        customer_id VARCHAR(10),
        city VARCHAR(50),
        zip_code VARCHAR(10),
        population INT,
        tenure INT,
        monthly_charges FLOAT8,
        total_charges FLOAT8,
        customer_status VARCHAR(20)
    )
    DISTSTYLE KEY
    DISTKEY (customer_id)
    SORTKEY (customer_status);
'''
result = redshift.execute_query(CREATE_TABLE_QUERY)
if result:
    logging.info("Table 'customer_churn_analytics' created successfully.")  
else:
    logging.error("Failed to create table 'customer_churn_analytics'.")

# -----------------------------------------------------------------------
# Truncate customer_churn_analytics table before loading new data
# -----------------------------------------------------------------------
TRUNCATE_QUERY = 'TRUNCATE TABLE customer_churn_analytics;'
result = redshift.execute_query(TRUNCATE_QUERY)
if result:
    logging.info("Table 'customer_churn_analytics' truncated successfully.")
else:
    logging.error("Failed to truncate table 'customer_churn_analytics'.")
    
# -----------------------------------------------------------------------
# Load data from customer_churn table and zip_population 
# table to customer_churn_analytics table in Redshift
# -----------------------------------------------------------------------
INSERT_QUERY = '''
    INSERT INTO customer_churn_analytics (
        customer_id,
        city,
        zip_code,
        population,
        tenure,
        monthly_charges,
        total_charges,
        customer_status
    )
    SELECT 
        c.customer_id,
        c.city,
        c.zip_code,
        z.population,
        c.tenure,
        c.monthly_charges,
        c.total_charges,
        c.customer_status
    FROM customer_churn c
    LEFT JOIN zip_population z ON c.zip_code = z.zip_code;
'''
result = redshift.execute_query(INSERT_QUERY)
if result:
    logging.info("Data inserted successfully into 'customer_churn_analytics' table.")
else:
    logging.error("Failed to insert data into 'customer_churn_analytics' table.")
        