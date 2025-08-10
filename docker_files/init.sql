-- schema for the tables
create schema if not EXISTS data;

-- datetime table
CREATE TABLE IF NOT EXISTS data.currency_conversions (
    from_currency varchar(10),
    to_currency varchar(10),
    rate numeric(20,10),
    rate_date date
);

-- raw data, table - allows me to make the view when the table is made
CREATE TABLE IF NOT EXISTS data.raw_data (
    booking_id text,
    check_in_date timestamp,
    check_out_date timestamp,
    owner_company text,
    owner_company_country varchar(10)
);

-- view that hosts the final table
create or replace view data.final_table as (
WITH booking_revenue AS (
-- first get the currency and cost per booking
    SELECT 
        rd.owner_company,
        rd.check_out_date::date,
        CASE 
            WHEN rd.owner_company_country = 'UK' THEN 'GBP'
            WHEN rd.owner_company_country = 'USA' THEN 'USD'
            ELSE 'EUR'
        END as original_currency,
        CASE 
            WHEN rd.owner_company_country = 'UK' THEN 10.00
            WHEN rd.owner_company_country = 'USA' THEN 14.00
            ELSE 12.00
        END as rate_per_booking,
        COUNT(*) as booking_count
    FROM data.raw_data rd
    WHERE rd.check_out_date IS NOT NULL
    GROUP BY 
        rd.owner_company, 
        rd.owner_company_country,
        rd.check_out_date::date
),
daily_revenue AS (
-- Calculate total revenue in original currency for each day
    SELECT 
        *,
        booking_count * rate_per_booking as original_revenue
    FROM booking_revenue
),
to_gbp_date as (
-- get only to gbp conversions into a table
	select * 
	from data.currency_conversions
	where to_currency = 'GBP'
),
daily_revenue_with_rates AS (
-- Find the to_gbp rate on each day
    SELECT 
        dr.*,
        (SELECT togdp.rate 
         FROM to_gbp_date togdp 
         WHERE togdp.from_currency = dr.original_currency 
           AND togdp.rate_date = dr.check_out_date 
         ORDER BY togdp.rate_date DESC 
         LIMIT 1) as exchange_rate_to_gbp,
         -- keep this column in to verify it works
        (SELECT togdp.rate_date 
         FROM to_gbp_date togdp 
         WHERE togdp.from_currency = dr.original_currency 
           AND togdp.rate_date = dr.check_out_date 
         ORDER BY togdp.rate_date DESC 
         LIMIT 1) as rate_date_used
    FROM daily_revenue dr
),
monthly_summary AS (
-- Get the values in a monthly aggregate
    SELECT 
        owner_company,
        TO_CHAR(DATE_TRUNC('month', check_out_date), 'YYYY-MM') as month,
        original_currency,
        AVG(rate_per_booking) as original_rate,
        SUM(booking_count) as booking_count,
        SUM(original_revenue) as original_revenue,
        SUM(original_revenue * exchange_rate_to_gbp) as revenue_gbp
    FROM daily_revenue_with_rates
    GROUP BY 
        owner_company, 
        DATE_TRUNC('month', check_out_date),
        original_currency
),
last_day_month AS (
-- get the last day of the month to find the minimum fee in new currency
	SELECT distinct (date_trunc('month', rate_date) + interval '1 month - 1 day')::date AS last_day FROM to_gbp_date 
),
last_day_conversion as (
-- get the conversion rate on the last day of the month
	select og.*,
		TO_CHAR(DATE_TRUNC('month', rate_date), 'YYYY-MM') as month
	from to_gbp_date og
	join last_day_month lday 
		on lday.last_day = og.rate_date
),
monthly_costs as (
-- get the monthly fee and conversion to be used onto the month table
	select ms.*,
		ldc.rate,
        CASE 
            WHEN ms.original_currency = 'GBP' THEN 100.00
            WHEN ms.original_currency = 'USD' THEN 140.00
            ELSE 120.00
        END as minimum_fee
	from monthly_summary ms
	join last_day_conversion ldc using(month)
	where ms.original_currency = ldc.from_currency
),
monthly_summary_final as (
-- get minimum fee
	select mc.*, 
		minimum_fee * rate as minimum_fee_gbp
	from monthly_costs mc
)


select owner_company,month,original_currency,booking_count,round(revenue_gbp,2) as gbp_revenue,round(minimum_fee_gbp,2) as gbp_costs
from monthly_summary_final
order by month,owner_company)