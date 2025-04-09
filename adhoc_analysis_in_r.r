## setting up r 
--bq project setup
billing <- 'etsy-bigquery-adhoc-prod'

--Authenticate to bq
bq_auth()

--pull in relevant data
gcloud auth application-default login
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_acbv`"

-- Runs query and saves it to a temp table
tb <- bq_project_query(billing, sql)

--download temp table to a data frame
df <- bq_table_download(tb,page_size=500) 


--Tells r what to test
--PERCENT OF BROWSERS
prop.test(c(57673,58689), c(74793,75460))

--MEANS
treat_f <- df[df$ab_variant == "on", ]
control_f <- df[df$ab_variant == "off", ]

--Does stat sig test
t.test(treat_f$visits, control_f$visits)

##Proportion metrics for rates  
--prop.test(c(success_treatment, success_control),c(sample_size_treatment,sample_size_control)
prop.test(c(57673,58689), c(74793,75460))
---Total browsers, browsers with that specific action 
---ex/ 57673 = total browsers, 58689= browsers that convert 

            
## Use this r code to find the stat sig 
-----acbv
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_acbv`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=1000) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)



