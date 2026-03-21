# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace view hilton_hotmail.default.daily_usage
# MAGIC as 
# MAGIC with
# MAGIC usage_with_ws_filtered_by_date as (
# MAGIC   select
# MAGIC     *
# MAGIC   from system.billing.usage
# MAGIC ),
# MAGIC -- apply workspace filter
# MAGIC usage_filtered as (
# MAGIC   select
# MAGIC     *
# MAGIC   from usage_with_ws_filtered_by_date
# MAGIC   where workspace_id = '2740990112560287'
# MAGIC ),
# MAGIC -- calc list priced usage in USD
# MAGIC prices as (
# MAGIC   select   sku_name,
# MAGIC     usage_unit,
# MAGIC     price_start_time,
# MAGIC     coalesce(price_end_time, timestamp '2999-12-31') as coalesced_price_end_time,
# MAGIC     pricing.effective_list.default as list_price
# MAGIC   from system.billing.list_prices
# MAGIC   where currency_code = 'USD'
# MAGIC )
# MAGIC   select
# MAGIC     date_trunc('DAY', usage_date) as usage_date,
# MAGIC     billing_origin_product,
# MAGIC     u.sku_name,
# MAGIC     p.list_price,
# MAGIC     sum(u.usage_quantity) usage,
# MAGIC     sum(coalesce(u.usage_quantity * p.list_price, 0)) as usage_usd
# MAGIC   from usage_filtered as u
# MAGIC   left join prices as p
# MAGIC     on u.sku_name=p.sku_name
# MAGIC     and u.usage_unit=p.usage_unit
# MAGIC     and (u.usage_end_time between p.price_start_time and p.coalesced_price_end_time)
# MAGIC   group by usage_date, billing_origin_product, list_price, u.sku_name
# MAGIC   order by usage_date desc, billing_origin_product desc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_usage

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     *
# MAGIC   from system.billing.usage

# COMMAND ----------

