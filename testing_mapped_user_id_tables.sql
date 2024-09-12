select count(distinct mapped_user_id) as mapped, count(distinct user_id) as users from etsy-data-warehouse-prod.user_mart.user_mapping
--mapped: 622394151, users: 957667498

select count(distinct mapped_user_id) as mapped, count(distinct user_id) as users from etsy-data-warehouse-prod.user_mart.mapped_user_profile 
--mapped: 622394151, users: 364366289

select mapped_user_id, count(distinct user_id) as users from etsy-data-warehouse-prod.user_mart.user_mapping group by all order by 2 desc
-- max is 3544647

select mapped_user_id, count(distinct user_id) as users from etsy-data-warehouse-prod.user_mart.mapped_user_profile group by all 
-- max is 1 

select 957667498 - 364366289 
--593301209
