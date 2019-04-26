
-- nps
WITH nps AS (
  SELECT
    YearOrder
    , MonthOrder
    , nps.retailerid
    , nps.shopOrderLinereference
    , npsType
    , npsScore
    , Weight
    , WeightPromotor
    , WeightDetractor
FROM
  `bolcom-pro-plato-vpm-615.team.nps_transactioneel_vw` nps 
  )
  
-- superjoin
, sj AS (
  SELECT
    Orderdate
    , ShopOrderLineReference
    , SellingParty
    , RetailerType
    , TransporterName
    , CodeTrackTrace 
    , PromisedDeliveryDate
    , FulfilmentMoment   
  FROM `bolcom-pro-plato-vpm-615.team.ppm_superjoin`
  --WHERE Orderdate >= '2019-02-01' AND OrderDate <=  '2019-03-01'
  )

, tpt AS (
  SELECT 
    CodeTrackTrace
    , MAX (CASE WHEN BolEventTypeDescription = 'Niet thuis' THEN 1 ELSE 0 END ) AS NietThuis
    , MAX (CASE WHEN BolEventTypeDescription = 'Bij de buren' THEN 1 ELSE 0 END) AS BijBuren
    , MAX (CASE WHEN BolEventTypeDescription = 'Wordt gebracht naar afhaallocatie/ kan worden afgehaald' 
      OR BolEventTypeDescription = 'Afgeleverd afhaalpunt' THEN 1 ELSE 0 END) AS Afhaallocatie   
  FROM `bolcom-pro-tpt-a62.tpt_views.tpt_transport_event_deduplicated_view_v2` 
  GROUP BY 
    CodeTrackTrace
)

SELECT DISTINCT nps.*, sj.* EXCEPT(shopOrderLinereference, CodeTrackTrace) , tpt.* 
FROM nps
LEFT JOIN sj ON nps.shopOrderLinereference = sj.ShopOrderLineReference
LEFT JOIN tpt ON tpt.CodeTrackTrace = sj.CodeTrackTrace
WHERE OrderDate >= '2019-01-01' AND Orderdate < '2019-04-01'
AND SellingParty = 'Plaza'
