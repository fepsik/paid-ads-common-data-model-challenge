

SELECT * EXCEPT (conversions),
conversions as total_conversions,
FROM {{ ref('src_ads_tiktok_ads_all_data') }}

