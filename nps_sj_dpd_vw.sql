  
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
  FROM `bolcom-pro-plato-vpm-615.team.superjoin_orderdetails` sj
  --WHERE Orderdate >= '2019-02-01' AND OrderDate <=  '2019-03-01'
  )
  
-- dpd;
, dpd AS (
  SELECT
    pakketNummer
    , Content
    , EXISTS(
      SELECT *
      FROM UNNEST(scans) s
      WHERE s.ScanDescription = 'Pakket wordt bij de volgende Pickup parcelshop bezorgd:'
    ) AS DeliveredToParcelShop
    , EXISTS(
      SELECT *
      FROM UNNEST(scans) s
      WHERE s.ScanDescription = 'Afleverpoging was niet succesvol.'
    ) AS DeliverAttemptNotSuccesful
    , EXISTS(
      SELECT *
      FROM UNNEST(scans) s
      WHERE s.ScanDescription = 'We hebben je pakket niet volgens afspraak kunnen bezorgen. Onze excuses hiervoor.'
    ) AS ExcuseLateDelivery
    , (
      SELECT CASE WHEN s.ScanTypeName = 'SC_03_OUT_FOR_DELIVERY' THEN s.ScanDatetime ELSE '' END as Levermoment 
      FROM UNNEST(scans) s ORDER BY Levermoment DESC LIMIT 1
    ) AS DateOnderweg
    , (
      SELECT CASE WHEN s.ScanTypeName = 'SC_13_DELIVERED' AND s.ScanDescription  = 'Geleverd.' THEN s.ScanDatetime ELSE '' END as Levermoment 
      FROM UNNEST(scans) s ORDER BY Levermoment DESC LIMIT 1
    ) AS Levermoment
   
  FROM `bolcom-pro-plato-vpm-615.kkonings.nested_dpd_events_vw`
  ) 

SELECT * EXCEPT  (ShopOrderLineReference, pakketNummer)
FROM nps
LEFT JOIN sj ON nps.shopOrderLinereference = sj.ShopOrderLineReference
LEFT JOIN dpd ON pakketNummer = CodeTrackTrace
WHERE OrderDate >= '2019-01-01' AND OrderDate < '2019-04-01'
AND SellingParty = 'Plaza'