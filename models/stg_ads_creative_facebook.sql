

SELECT * EXCEPT (complete_registration),
purchase as total_conversions,
likes+shares+comments+clicks+views as engagements
FROM {{ ref('src_ads_creative_facebook_all_data') }}

