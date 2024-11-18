select
    count(distinct visit_id) as visits,
    case 
      when landing_event in ("home", "homescreen", "recommended",'home__home__tapped','home_pager') then "home"
      when landing_event in ("login_view", "homescreen", "recommended",'home__home__tapped','home_pager') then "home"
      when landing_event in ("view_profile",'you') then "view_profile"
      when landing_event in ("search", "browselistings", "search_results", "async_listings_search", "autosuggest") then "search"
      when landing_event in ("your_purchases", "yr_purchases") then "your_purchases"
      when landing_event in ("view_listing", "image_zoom",'listing_page_recommendations','view_sold_listing','view_unavailable_listing') then "listing"
      when landing_event in ("cart_view") then "cart"
      when landing_event in ("convo_view",'message__messages__tapped','messages','convo_main') then "convo"
      when landing_event in ("shop_home_inactive",'shop_home') then "shop_home"
      else landing_event
      -- when referring_page_event in ("view_listing", "appreciation_photo_detail", "cart_view", "shop_home", "view_receipt", "favorites", "favorites_and_lists", "in_app_notifications", "convo_main", "you_screen", "list_suggestions", "favorites_view","collection_view") then referring_page_event
   end as referring_page_event
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-5
group by all 
