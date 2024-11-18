    select
    count(distinct visit_id) as visits,
    case 
      when landing_event in ("home","login_view", "homescreen", "recommended",'home__home__tapped','home_pager') then "home"
      when landing_event in ("view_profile",'you') then "view_profile"
      when landing_event in ("search", "browselistings", "search_results", "async_listings_search", "autosuggest") then "search"
      when landing_event in ("your_purchases", "yr_purchases",'order__orders__tapped','view_receipt','view_receipt_downloads_page') then "your_purchases / orders"
      when landing_event in ("view_listing", "image_zoom",'listing_page_recommendations','view_sold_listing','view_unavailable_listing','listing__listing_hub__tapped','appreciation_photo_detail') then "listing"
      when landing_event in ("mc_seller_dashboard_legacy",'mission_control_orders_legacy','stats','listing-manager','mc_seller_dashboard','seller_handbook_view') then "seller"
      when landing_event in ("cart_view") then "cart"
      when landing_event in ("convo_view",'message__messages__tapped','messages','convo_main','member_conversations_detail','member_conversations_landing','branch_convo_landing','mc_conversations_detail') then "convo"
      when landing_event in ("shop_home_inactive",'shop_home','shop_reviews') then "shop_home"
      when landing_event in ("favorites_and_lists",'favorites','favorites_view","collection_view','view_favorites_landing') then "favorites / collections"
      else landing_event
      -- when referring_page_event in ("view_listing", "appreciation_photo_detail", "cart_view", "shop_home", "view_receipt", "favorites", "favorites_and_lists", "in_app_notifications", "convo_main", "you_screen", "list_suggestions", "favorites_view","collection_view") then referring_page_event
   end as referring_page_event
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-5
group by all 


----pull out events surrounding landing event to find real primary page
with visits as (
select
  distinct visit_id
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-5
and landing_event in ('shop_sold')
limit 1
)
select
event_type,
sequence_number
from etsy-data-warehouse-prod.weblog.events
inner join visits using (visit_id)
order by 2 asc
