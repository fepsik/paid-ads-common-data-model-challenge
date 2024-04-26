
SELECT * EXCEPT (imps, conv),
imps as impressions,
conv as total_conversions
FROM {{ ref('src_ads_bing_all_data') }}

