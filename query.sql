[[bqsql]]
dag_id = 'wishlist_price_drop'
task_id = 'wishlist_backend_vnimmakuri'
sql = '''
CREATE OR REPLACE TABLE noonbigmerch.product_noon.wishlist_backend as 
select
event_time,
right(locale,2) as country,
visitor_id,
uid,
event_misc,
event_type,
platform,
coalesce(json_extract_scalar(event_misc,"$.sku"),json_extract_scalar(event_misc,"$.sku_config")) as sku,
json_extract_scalar(event_misc,"$.ofc") as ofc,
from `noonprd-mp-analytics.noon_analytics_tool.raw_events`
where 1=1
and event_date>= DATE_SUB(CURRENT_DATE(), INTERVAL 95 DAY)
and date(timestamp_add(event_time,interval 4 hour)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 95 DAY)
and json_extract_scalar(event_misc,"$.mc") = "noon"
and event_type in ("add_to_savedcart","remove_from_savedcart","delete_from_wishlist")
and right(locale,2) in ('ae','eg','sa')
'''
failure_email_list = ['vnimmakuri@noon.com']


[[bqsql]]
dag_id = 'wishlist_price_drop'
task_id = 'offer_price_vnimmakuri'
sql = '''
CREATE OR REPLACE TABLE noonbigmerch.product_noon.offer_price as 
with base as
(
--Rocket
select distinct date,country,sku,offer_code,"rocket" as ofc_tag,offer_price from noonbigrowth.assortment_rocket.psku_sku_mapped_ae
where date >= current_date() - 95


union all

select distinct date,country,sku,offer_code,"rocket" as ofc_tag,offer_price from noonbigrowth.assortment_rocket.psku_sku_mapped_sa
where date >= current_date() - 95

union all

-- Express & MP
select
distinct
date,country,sku,offer_code,
case when is_fbn=1 or is_express=1 then "express" else "mp" end as ofc_tag,offer_price
from noonbigrowth.assortment_noon.psku_sku_mapped_ae
where date >= current_date() - 95
and is_live=1

union all

select
distinct
date,country,sku,offer_code,
case when is_fbn=1 or is_express=1 then "express" else "mp" end as ofc_tag,offer_price
from noonbigrowth.assortment_noon.psku_sku_mapped_sa
where date >= current_date() - 95
and is_live=1


union all

select
distinct
date,country,sku,offer_code,
case when is_fbn=1 or is_express=1 then "express" else "mp" end as ofc_tag,offer_price
from noonbigrowth.assortment_noon.psku_sku_mapped_eg
where date >= current_date() - 95
and is_live=1

),

duplicates as
(
SELECT
date,country,sku,offer_code,count(*) as times from base
group by 1,2,3,4
having count(*)>1),

ofc_price as
(select * from base where concat(date,country,offer_code) not in (select distinct concat(date,country,offer_code) from duplicates))

select * from ofc_price
'''
failure_email_list = ['vnimmakuri@noon.com']
depends = ['wishlist_backend_vnimmakuri']


[[bqsql]]
dag_id = 'wishlist_price_drop'
task_id = 'wishlist_price_vnimmakuri'
sql = '''
create or replace table noonbigmktg.cache_noon.wishlist_price as 
with base as
(select
country,
uid,
event_type,
sku as sku_config,
ofc,
max(event_time) as max_event,
sum(1) as instances
from noonbigmerch.product_noon.wishlist_backend
where 1=1
and date(timestamp_add(event_time,interval 4 hour)) >= current_date() - 90
and length(uid) > 30
and sku not like "%-%"
group by 1,2,3,4,5

union all

select * from noonbigmerch.product_noon.price_drop_base_pdp
),

base1 as
(select
country,
uid,
event_type,
sku_config,
ofc,
date(max_event) as date,
row_number() over(partition by country,uid,sku_config order by max_event desc) as rnk
from base
where (case when event_type = "page_detail" then instances >= 2 else 1=1 end)
),

final as
(select
a.date,
a.country,
a.uid,
a.event_type,
a.sku_config as sku,
a.ofc,
b.offer_price as price,
b.ofc_tag
from base1 a
left join noonbigmerch.product_noon.offer_price b on a.date=b.date and lower(a.country)=lower(b.country) and a.ofc=b.offer_code
where a.rnk = 1
and a.event_type in ("add_to_cart","add_to_savedcart","page_detail")
),

price_today as
(select * from noonbigmerch.product_noon.offer_price
where date= current_date()
),


price_yst as
(select * from noonbigmerch.product_noon.offer_price
where date= current_date() - 1
),

buybox_offer as
(
select * from

(
(select
a.*,
b.ofc_tag
from
(select * from noonbigrowth.assortment_noon.buy_box_ae
where date= current_date()

union all

select * from noonbigrowth.assortment_noon.buy_box_sa
where date= current_date()

union all

select * from noonbigrowth.assortment_noon.buy_box_eg
where date= current_date()
) a
left join price_today b on a.offer_code=b.offer_code
)
)

where ofc_tag<> "rocket"
)
,

final1 as
(select
distinct
a.event_type as latest_event,
a.country,
a.date as wishlisted_date,
a.uid as customer_code,
a.sku as wishlisted_sku,
a.ofc as wishlisted_ofc,
a.price as wishlisted_price,
case
when b.offer_price is null and c.offer_price is null then d.offer_code
when b.offer_price<>c.offer_price and least(b.offer_price,c.offer_price)=b.offer_price then b.offer_code
when b.offer_price<>c.offer_price and least(b.offer_price,c.offer_price)=c.offer_price then c.offer_code
when b.offer_price=c.offer_price then c.offer_code
end as new_ofc,

coalesce(least(b.offer_price,c.offer_price),d.offer_price) as new_ofc_price,
case when coalesce(least(b.offer_price,c.offer_price),d.offer_price)<a.price then 1 else 0 end as price_decrease_tag,
case
when b.offer_price is null and c.offer_price is null then 1
when b.offer_price<>c.offer_price and least(b.offer_price,c.offer_price)=b.offer_price then 0
when b.offer_price<>c.offer_price and least(b.offer_price,c.offer_price)=c.offer_price then 1
when b.offer_price=c.offer_price then 1
end as same_ofc_tag
from final a
left join buybox_offer b on lower(a.country) = lower(b.country) and a.sku=b.sku
left join price_today c on lower(a.country)=lower(c.country) and a.ofc=c.offer_code
left join price_yst d on lower(a.country)=lower(d.country) and a.ofc=d.offer_code
where a.ofc_tag<> "rocket"
and (a.price is not null or a.price<>0)

),

final2 as
(select * from final1
where new_ofc is not null
),

exclusion as
(select
coalesce(lower(country) || customer_code|| wishlisted_sku,"") as comb,
count(*)
from final2
group by 1
having count(*)>1

union all

select
distinct
coalesce(lower(country) || customer_code || sku_config,"") as comb,
1 as count_
from `noonbigmktg.reporting_noon.sales_complete`
where order_pdate >= current_date() - 30

union all

select
distinct
coalesce(lower(country) || uid || sku,"") as comb,
1 as count_
from noonbigmerch.product_noon.price_drop_excl
),

scoring_base as
(select
uid,
country,
sku_config as sku,
sum(case when event_type="page_detail" then instances end) as pdp,
sum(case when event_type="add_to_cart" then instances end) as atc,
sum(case when event_type= "add_to_savedcart" then instances end) as wishlists
from base
group by 1,2,3
),

final3 as
(select
a.*,
b.pdp,
b.atc,
b.wishlists
from final2 a
left join scoring_base b on a.customer_code=b.uid and lower(a.country) = lower(b.country) and a.wishlisted_sku = b.sku
where coalesce(lower(a.country) || a.customer_code|| a.wishlisted_sku,"") not in (select distinct comb from exclusion)
and a.price_decrease_tag = 1
),

final4 as
(select
*,
date_diff(current_date(), wishlisted_date,day) as recency,
wishlisted_price - new_ofc_price AS price_drop,
(wishlisted_price - new_ofc_price) / nullif(wishlisted_price,0) AS price_drop_perc,
if(pdp>=2,(3 + (pdp - 2) * 2),2) + if(wishlists >=1 ,7,0) + if(atc>=1 ,(10 + (atc - 1) * 4),0)
as intent_score
from final3
-- where customer_code = "99b111be-6250-4ec3-a55c-de5b095f242e"
),

final5 as
(select
*,
1- (recency/nullif( (max(recency) over(partition by customer_code,country)),0)) as recency_norm,
price_drop/nullif(max(price_drop) over(partition by customer_code,country),0) as price_drop_norm,
price_drop_perc/nullif(max(price_drop_perc) over(partition by customer_code,country),0) as price_drop_perc_norm,
intent_score/nullif(max(intent_score) over(partition by customer_code,country),0) as intent_score_norm
from final4
),

final6 as
(select
* ,

case when left(customer_code,1) in ("1","2","3","4","5","6","7","8") then 0.2*intent_score_norm + 0.2*recency_norm + 0.3*price_drop_norm +0.3*price_drop_perc_norm
else 0.4*intent_score_norm + 0.2*recency_norm + 0.2*price_drop_norm +0.2*price_drop_perc_norm end
as final_score,

case when left(customer_code,1) in ("1","2","3","4","5","6","7","8") then
row_number() over(partition by customer_code,country order by (0.2*intent_score_norm + 0.2*recency_norm + 0.3*price_drop_norm +0.3*price_drop_perc_norm) desc)
else row_number() over(partition by customer_code,country order by (0.4*intent_score_norm + 0.2*recency_norm + 0.2*price_drop_norm +0.2*price_drop_perc_norm) desc)
end as rank
from final5
)

select
distinct
case 
  when latest_event = "add_to_savedcart" then "wishlist"
  when latest_event = "page_detail" then "pdp"
  when latest_event = "add_to_cart" then "atc"
else null end as source,
country,
wishlisted_date,
customer_code,
wishlisted_sku,
wishlisted_ofc,
wishlisted_price,
new_ofc,
new_ofc_price,
price_decrease_tag,
same_ofc_tag,
rank,
pdp,
atc,
wishlists
from final6

# with base as
# (select
# date(timestamp_add(event_time,interval 4 hour)) as date,
# event_type,
# uid,
# country,
# sku as sku_config,
# ofc,
# from noonbigmerch.product_noon.wishlist_backend
# where length(uid) > 30
# and sku not like "%-%"
# qualify row_number() over(partition by uid,country,sku order by event_time desc)=1
# ),

# final as
# (select
# a.*,
# b.sku,
# b.offer_price as price,
# b.ofc_tag
# from base a
# left join noonbigmerch.product_noon.offer_price  b on a.date=b.date and lower(a.country)=lower(b.country) and a.ofc=b.offer_code
# where event_type = "add_to_savedcart"),

# price_today as
# (select * from noonbigmerch.product_noon.offer_price 
# where date= current_date()
# ),


# price_yst as
# (select * from noonbigmerch.product_noon.offer_price 
# where date= current_date() - 1
# ),

# buybox_offer as
# (
# select * from

# (
# (select
# a.*,
# b.ofc_tag
# from
# (select * from noonbigrowth.assortment_noon.buy_box_ae
# where date= current_date()

# union all

# select * from noonbigrowth.assortment_noon.buy_box_sa
# where date= current_date()

# union all

# select * from noonbigrowth.assortment_noon.buy_box_eg
# where date= current_date()
# ) a
# left join price_today b on a.offer_code=b.offer_code
# )
# )

# where ofc_tag<> "rocket"
# )
# ,

# final1 as
# (select
# distinct
# a.country,
# a.date as wishlisted_date,
# a.uid as customer_code,
# a.sku as wishlisted_sku,
# a.ofc as wishlisted_ofc,
# a.price as wishlisted_price,
# case 
# when b.offer_price is null and c.offer_price is null then d.offer_code
# when b.offer_price<>c.offer_price and least(b.offer_price,c.offer_price)=b.offer_price then b.offer_code
# when b.offer_price<>c.offer_price and least(b.offer_price,c.offer_price)=c.offer_price then c.offer_code
# when b.offer_price=c.offer_price then c.offer_code
# end as new_ofc,

# coalesce(least(b.offer_price,c.offer_price),d.offer_price) as new_ofc_price,
# case when coalesce(least(b.offer_price,c.offer_price),d.offer_price)<a.price then 1 else 0 end as price_decrease_tag,
# case
# when b.offer_price is null and c.offer_price is null then 1 
# when b.offer_price<>c.offer_price and least(b.offer_price,c.offer_price)=b.offer_price then 0
# when b.offer_price<>c.offer_price and least(b.offer_price,c.offer_price)=c.offer_price then 1
# when b.offer_price=c.offer_price then 1
# end as same_ofc_tag
# from final a
# left join buybox_offer b on lower(a.country) = lower(b.country) and a.sku=b.sku
# left join price_today c on lower(a.country)=lower(c.country) and a.ofc=c.offer_code
# left join price_yst d on lower(a.country)=lower(d.country) and a.ofc=d.offer_code
# where a.ofc_tag<> "rocket"
# and (a.price is not null or a.price<>0)

# ),

# final2 as 
# (select * from final1
# where new_ofc is not null 
# ),

# duplicates as 
# (select lower(country) || customer_code|| wishlisted_sku as comb,
# count(*)
#  from final2
#  group by 1
#  having count(*)>1 

#  union all 

#  select 
# distinct 
# coalesce(lower(country) || customer_code || sku_config,"") as comb,
# 1 as count_
# from `noonbigmktg.reporting_noon.sales_complete`
# where order_pdate >= current_date() - 30
#  )

# select 
# *,
# row_number() over(partition by country,customer_code order by date_diff(current_date(),wishlisted_date,day)) as rank
# from final2 where coalesce(lower(country) || customer_code|| wishlisted_sku,"") not in (select distinct comb from duplicates)
# and price_decrease_tag = 1
'''
failure_email_list = ['vnimmakuri@noon.com']
depends = ['offer_price_vnimmakuri']


[[bqsql]]
dag_id = 'wishlist_price_drop'
task_id = 'price_drop_base_pdp_vnimmakuri'
sql = '''
create or replace table noonbigmerch.product_noon.price_drop_base_pdp as
with base as
(select
event_time,
right(locale,2) as country,
uid,
event_type,
coalesce(json_extract_scalar(event_misc,"$.sku"),json_extract_scalar(event_misc,"$.sku_config")) as sku,
coalesce(JSON_EXTRACT_SCALAR(event_misc,"$.offerCode"),JSON_EXTRACT_SCALAR(event_misc,"$.ofc")) as ofc,
from `noonprd-mp-analytics.noon_analytics_tool.raw_events`
where 1=1
and event_type in ("page_detail","add_to_cart","remove_from_cart")
and json_extract_scalar(event_misc,"$.mc") = "noon"
and event_date>=current_date() - 45
and property_code = "noon"
and length(uid) > 6
),

base1 as
(select
country,
uid,
event_type,
sku,
ofc,
max(event_time) as max_event,
sum(1) as instances_
from base
where sku is not null and ofc is not null
group by 1,2,3,4,5
)

select * from base1
  '''
failure_email_list = ['vnimmakuri@noon.com']



[[bqsql]]
dag_id = 'wishlist_price_drop'
task_id = 'price_drop_excl_vnimmakuri'
sql = '''
create or replace table noonbigmerch.product_noon.price_drop_excl as 
with base as
(SELECT
-- date(timestamp_add(event_time, interval 4 hour)) as date,
upper(RIGHT(locale, 2)) as country,
uid,
coalesce(JSON_EXTRACT_SCALAR(event_misc,"$.skuConfig"), JSON_EXTRACT_SCALAR(event_misc,"$.sku")) as sku,
count(case when event_type= "product_impression" then visitor_id end) as product_impressions,
coalesce(count(case when event_type= "page_detail" then visitor_id end),0)+coalesce(count(case when event_type= "add_to_cart" then visitor_id end),0) as pdp_atc,
FROM `noonprd-mp-analytics.noon_analytics_tool.raw_events`
WHERE 1=1
AND event_date >= current_date() - 8
AND date(timestamp_add(event_time, interval 4 hour)) >= current_date() - 7
AND property_code = "noon"
-- AND (RIGHT(locale, 2)) IN ("ae", "sa")
AND event_type in ('product_impression','page_detail', 'add_to_cart')
AND JSON_EXTRACT_SCALAR(event_misc, '$.wn') = 'wishlist_price_drop'
group by 1,2,3
)

select
distinct 
country,
uid,
sku
from base
where product_impressions >= 3
and coalesce(pdp_atc,0) = 0
  '''
failure_email_list = ['vnimmakuri@noon.com']
