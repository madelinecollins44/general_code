# run the following in the terminal to log into bq
gcloud auth application-default login

## setting up r 
library(bigrquery)
library(DBI)

# bq project setup
billing <- 'etsy-bigquery-adhoc-prod'


# Authenticate to bq
bq_auth()

# pull in relevant data
gcloud auth application-default login
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_acbv`"
### code how to create these tables: https://github.com/madelinecollins44/gift_mode/blob/main/Merch_MDAY24_HubvsGM_test.sql

#  Runs query and saves it to a temp table
tb <- bq_project_query(billing, sql)

# download temp table to a data frame
df <- bq_table_download(tb,page_size=500) 


# Tells r what to test
# # PERCENT OF BROWSERS
prop.test(c(57673,58689), c(74793,75460))

# MEANS
treat_f <- df[df$ab_variant == "on", ]
control_f <- df[df$ab_variant == "off", ]

# Does stat sig test
# t.test(treat_f$visits, control_f$visits)

## Proportion metrics for rates  
# #  prop.test(c(success_treatment, success_control),c(sample_size_treatment,sample_size_control)
prop.test(c(57673,58689), c(74793,75460))
# Total browsers, browsers with that specific action 
# # ex/ 57673 = total browsers, 58689= browsers that convert 

            
## Use this r code to find the stat sig 
## acbv
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_acbv`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=1000) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## gms
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_gms`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=500) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## offsite ads
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_offsite_ads`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=500) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## prolist
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_prolist`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=500) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## order value
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_order_value`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=500) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)
control_f <- df[df$ab_variant == "off", ]

t.test(treat_f$event_count, control_f$event_count)

    
-------------------------------------------------------------------------------------------
CALC MEANS FOR METRICS MANUALLY 
-------------------------------------------------------------------------------------------
SELECT
    variant_id,
    sum(case when event_id in ('total_winsorized_order_value') then event_count end)/ sum(case when event_id in ('backend_cart_payment') then event_count end) as Winsorized_aov,
    sum(case when event_id in ('total_winsorized_gms') then event_count end)/ count(case when event_id in ('total_winsorized_gms') and event_count != 0 then event_id end) as Winsorized_acbv, 

FROM
    `etsy-data-warehouse-dev.madelinecollins.all_units_events_segments`
group by all


