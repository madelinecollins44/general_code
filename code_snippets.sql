--Definitions of search bins– bins are based on volume, not attributes of the query itself
'Tail': .ktile <= 7000 THEN 'tail'
'Torso': base.ktile BETWEEN 7001 AND 9600
'Head': base.ktile BETWEEN 9601 AND 9990
'Top.1': base.ktile BETWEEN 9991 AND 9999 THEN 'top.1'
 'top.01': base.ktile > 9999
it's purely popularity based! so a top.01 query is going to be super popular that a lot of users search for, whereas tail queries are generally more niche and they're not being searched for as much
  
-- Grouping events 
when a.ref_tag in ('search', 'async_listings_search', 'browselistings', 'search_results') then 'search / listing results'
when a.ref_tag in ('home', 'homescreen') then 'home'
when a.ref_tag in ('shop_home') then 'shop home view'
when a.ref_tag in ('view_listing') then 'listing view'
when a.ref_tag in ('category_click','category_page') then 'category search'
when a.ref_tag in ('similar_listings','search_similar_items') then 'similar listings'
when a.ref_tag in ('favorites','favorites_and_lists','profile_favorite_listings_tab','favorites_shops','profile_favorite_shops_tab','favorite_item','backend_favorite_item2') then 'favorites / favorite shops'
when a.ref_tag in ('favorites_tapped_list','collections_view') then 'lists'
when a.ref_tag in ('cart_view', 'add_to_cart') then 'cart/ add to cart'
when a.ref_tag in ('cart_saved_view','cart_saved_for_later') then 'saved for later'
when a.ref_tag in ('you_screen','you_tab_viewed','your_purchases','yr_purchases') then 'you tab + your purchases'
when a.ref_tag in ('member_conversations_landing','convo_main','convo_view','conversations_message_read') then 'convos'
when a.ref_tag in ('your_account_settings','user_settings','account_setting') then 'account settings'
when a.ref_tag in ('backend_cart_payment') then 'payment'
when a.ref_tag in ('start_single_listing_checkout','single_listing_overlay_open') then ''
when a.ref_tag in ('view_receipt') then 'post purchase'
when a.ref_tag in ('Deals Tab') then 'deals tab'
else a.ref_tag 

-- Get visit level buyer segment for mapped user id
CREATE OR REPLACE TEMP TABLE buyer_segments as (
with purchase_stats as (
  SELECT
      a.mapped_user_id, 
      ex.first_app_visit, 
      min(date) AS first_purchase_date, 
      max(date) AS last_purchase_date,
      coalesce(sum(gms_net),0) AS lifetime_gms,
      coalesce(count(DISTINCT date),0) AS lifetime_purchase_days, 
      coalesce(count(DISTINCT receipt_id),0) AS lifetime_orders,
      round(cast(round(coalesce(sum(CASE
          WHEN date between date_sub(first_app_visit, interval 365 DAY) and first_app_visit THEN gms_net
      END), CAST(0 as NUMERIC)),20) as numeric),2) AS past_year_gms,
      count(DISTINCT CASE
          WHEN date between date_sub(first_app_visit, interval 365 DAY) and first_app_visit THEN date
      END) AS past_year_purchase_days,
      count(DISTINCT CASE
          WHEN date between date_sub(first_app_visit, interval 365 DAY) and first_app_visit THEN receipt_id
      END) AS past_year_orders
    from 
      `etsy-data-warehouse-prod.user_mart.mapped_user_profile` a
    join
       first_visits ex 
        ON ex.mapped_user_id = a.mapped_user_id
    join 
      `etsy-data-warehouse-prod.user_mart.user_mapping` b
        on a.mapped_user_id = b.mapped_user_id
    join 
      `etsy-data-warehouse-prod.user_mart.user_first_visits` c
        on b.user_id = c.user_id
    left join 
      `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` e
        on a.mapped_user_id = e.mapped_user_id 
        and e.date <= ex.first_app_visit-1 and market <> 'ipp'
    GROUP BY all
    having (ex.first_app_visit >= min(date(timestamp_seconds(a.join_date))) or ex.first_app_visit >= min(date(c.start_datetime)))
  )
  select
    mapped_user_id, 
    first_app_visit,
    CASE  
      when p.lifetime_purchase_days = 0 or p.lifetime_purchase_days is null then 'Zero Time'  
      when date_diff(first_app_visit, p.first_purchase_date, DAY)<=180 and (p.lifetime_purchase_days=2 or round(cast(round(p.lifetime_gms,20) as numeric),2) >100.00) then 'High Potential' 
      WHEN p.lifetime_purchase_days = 1 and date_diff(first_app_visit, p.first_purchase_date, DAY) <=365 then 'OTB'
      when p.past_year_purchase_days >= 6 and p.past_year_gms >=200 then 'Habitual' 
      when p.past_year_purchase_days>=2 then 'Repeat' 
      when date_diff(first_app_visit , p.last_purchase_date, DAY) >365 then 'Lapsed'
      else 'Active' 
      end as buyer_segment,
  from purchase_stats p
); 



-- Query to find error in event_hub events
SELECT *
FROM `etsy-eventpipe-prod.data_collection.beacon_validation_log`
WHERE
  DATE(_PARTITIONTIME) = CURRENT_DATE 
  AND event_name = 'gift_mode_results'
LIMIT 1000

-- Query to give you all the listings that are active in stash library 
SELECT distinct l.listing_id,
FROM `etsy-data-warehouse-prod`.listing_mart.listings AS l
LEFT OUTER JOIN `etsy-data-warehouse-prod`.etsy_shard.merch_listings AS m on l.listing_id = m.listing_id
where m.status = 0
and l.is_active = 1

