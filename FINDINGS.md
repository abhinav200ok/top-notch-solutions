# Data Quality Issues in raw_orders.csv

## Issue 1
**What is the issue?**

Missing amount_usd for order_id 1012
- This means that when the CSV is read, the column value will be NULL after casting it to double.


**Why is it a problem for the pipeline?**
- A NULL order amount cannot contribute to total_revenue or avg_order_value.
If this is allowed, it would inflate total_orders and completed_orders without adding any revenue, this will silently distort all per-customer aggregations.

**How does your pipeline currently handle it?**

stg_orders.transform() casts order_amount to DoubleType (NULL if the string
is empty) and then filters out the NULL values.

Row 1012 is excluded at the staging layer and never reaches int_orders_enriched
or customer_orders.  Data quality check 4 (order_amount NOT NULL) also guards
against this.

## Issue 2
**What is the issue?**
Mixed-case status for order_id 1014 i.e. inconsistent casing
- All other rows have lowercase status values. Row 1014 uses capital letters i.e. COMPLETED

**Why is it a problem for the pipeline?**
- String comparisons in Spark are case-sensitive. Without normalisation the string comparison status == 'completed' would return FALSE and would make a completed order invisible for revenue calculation.

**How does your pipeline currently handle it?**
- stg_orders.transform() applies F.lower(...) after load and thus normalising it.
- Data quality check 3 would flag this as unknown value status.

## Issue 3
**What is the issue?**
Zero amount_usd for order_id 1015
- The order amount for this row is zero. The row has a status of completed, meaning it would be counted as a completed order. An order value should be a positive number.

**Why is it a problem for the pipeline?**
- It would erroranously inflate completed_orders value thus distorting results.
- Distort avg_order_value

**How does your pipeline currently handle it?**
- Filter in filter in stg_orders.transform() also removes Zero value records.
- data quality check 7 i.e.  (total_revenue > 0)