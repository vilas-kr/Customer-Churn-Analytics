import logging
from service import redshift


logging.basicConfig(level=logging.INFO)

TABLE_NAME = "customer_churn_analytics"

# -------------------------------------------------------------------------
# Analyze Table
# -------------------------------------------------------------------------
analyze_query = f"ANALYZE {TABLE_NAME};"

result = redshift.execute_query(analyze_query)

if result:
    logging.info(f"ANALYZE completed for table: {TABLE_NAME}")
else:
    logging.error(f"ANALYZE failed for table: {TABLE_NAME}")


# -------------------------------------------------------------------------
# VACUUM TABLE
# -------------------------------------------------------------------------
vacuum_query = f"VACUUM {TABLE_NAME};"

result = redshift.execute_query(vacuum_query)

if result:
    logging.info(f"VACUUM completed for table: {TABLE_NAME}")
else:
    logging.error(f"VACUUM failed for table: {TABLE_NAME}")
    
# -------------------------------------------------------------------------
# Explain briefly how they improve Redshift performance
# -------------------------------------------------------------------------
# ANALYZE :
# - Updates table statistics (data distribution, column values)
# - Helps Redshift query optimizer choose the best execution plan

# Faster queries because Redshift knows:
# - which joins to use
# - how to scan data efficiently

# VACUUM :
# - Removes deleted/unused rows
# - Re-sorts data based on SORTKEY
# - Reduces unnecessary data scanning
# - Improves query speed (especially range queries)
# - Saves storage space