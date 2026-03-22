import logging
from service import redshift

# Configure logging
logging.basicConfig(level=logging.INFO)


def get_value(field):
    """Safely extract value from Redshift Data API response."""
    return (
        field.get("stringValue")
        or field.get("longValue")
        or field.get("doubleValue")
    )


# -------------------------------------------------------------------------
# 1. Churn rate across all customers
# -------------------------------------------------------------------------
churn_rate_query = """
    SELECT 
        ROUND(
            (SUM(CASE WHEN customer_status = 'Churned' THEN 1 ELSE 0 END) 
                * 100.0) / COUNT(*), 2
        ) AS churn_rate_percentage
    FROM customer_churn_analytics;
"""

result = redshift.execute_query(churn_rate_query)

if result and result.get("Records"):
    logging.info("Churn rate query executed successfully.")
    print("-" * 60)
    churn_rate = get_value(result["Records"][0][0])
    print(f"Churn Rate (%): {churn_rate}")
    print("-" * 60)
else:
    logging.error("Failed to calculate churn rate.")


# -------------------------------------------------------------------------
# 2. Top cities with highest churn
# -------------------------------------------------------------------------
top_cities_query = """
    SELECT city, COUNT(*) AS churned_customers
    FROM customer_churn_analytics
    WHERE customer_status = 'Churned'
    GROUP BY city
    ORDER BY churned_customers DESC
    LIMIT 10;
"""

result = redshift.execute_query(top_cities_query)

if result and result.get("Records"):
    logging.info("Top cities query executed successfully.")
    print("Top Cities with Highest Churned Customers:")
    print("-" * 60)
    print(f"{'City':<20} {'Churned Customers'}")
    print("-" * 60)
    
    for record in result["Records"]:
        city = get_value(record[0])
        churned_count = get_value(record[1])
        print(f"{city:<20} {churned_count}")

    print("-" * 60)
else:
    logging.error("Failed to retrieve top cities.")


# -------------------------------------------------------------------------
# 3. Churn distribution by tenure group
# -------------------------------------------------------------------------
churn_distribution_query = """
    SELECT 
        CASE 
            WHEN tenure < 12 THEN '0-11'
            WHEN tenure BETWEEN 12 AND 24 THEN '12-24'
            WHEN tenure BETWEEN 25 AND 36 THEN '25-36'
            ELSE '36+'
        END AS tenure_group,
        COUNT(*) AS churned_customers
    FROM customer_churn_analytics
    WHERE customer_status = 'Churned'
    GROUP BY tenure_group
    ORDER BY tenure_group;
"""

result = redshift.execute_query(churn_distribution_query)

if result and result.get("Records"):
    logging.info("Churn distribution query executed successfully.")
    print("Customer Churn Distribution by Tenure Group:")
    print("-" * 60)
    print(f"{'Tenure in months':<20} {'Churned Customers'}")
    print("-" * 60)
    
    for record in result["Records"]:
        tenure_group = get_value(record[0])
        churned_count = get_value(record[1])
        print(f"{tenure_group:<20} {churned_count}")

    print("-" * 60)
else:
    logging.error("Failed to retrieve churn distribution.")


# -------------------------------------------------------------------------
# 4. Total revenue lost due to churn
# -------------------------------------------------------------------------
revenue_lost_query = """
    SELECT 
        ROUND(SUM(total_charges), 2) AS total_revenue_lost
    FROM customer_churn_analytics
    WHERE customer_status = 'Churned';
"""

result = redshift.execute_query(revenue_lost_query)

if result and result.get("Records"):
    logging.info("Revenue loss query executed successfully.")
    print("-" * 60)
    revenue_lost = get_value(result["Records"][0][0])
    print(f"Total Revenue Lost Due to Churn: {revenue_lost}")
    print("-" * 60)
else:
    logging.error("Failed to calculate revenue loss.")


# -------------------------------------------------------------------------
# 5. Population vs customer count by zip code
# -------------------------------------------------------------------------
population_customer_query = """
    SELECT 
        zip_code,
        population,
        COUNT(customer_id) AS customer_count
    FROM customer_churn_analytics
    GROUP BY zip_code, population;
"""

result = redshift.execute_query(population_customer_query)

if result and result.get("Records"):
    logging.info("Population vs customer query executed successfully.")
    print("Population vs Customer Count by Zip Code:")
    print("-" * 60)
    print(f"{'Zip Code':<10} {'Population':<15} {'Customer Count'}")
    print("-" * 60)

    for record in result["Records"]:
        zip_code = get_value(record[0])
        population = get_value(record[1])
        customer_count = get_value(record[2])
        print(f"{zip_code:<10} {population:<15} {customer_count}")

    print("-" * 60)
else:
    logging.error("Failed to retrieve population vs customer data.")