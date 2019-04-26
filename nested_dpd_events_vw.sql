with cte_alltransports as (
  select *
    , row_number() over(partition by CodeTrackTrace order by length(Response) desc) rn
  from `bolcom-pro-plato-vpm-615.pklinkenberg.ttscraper_processed_transports` t
  --where DateTransportCreated = '2019-03-01'
)
, cte_uniquetransports as (
  select * except(rn)
  from cte_alltransports
  where rn=1
)

SELECT 
   JSON_EXTRACT_SCALAR(Response, '$.parcellifecycleResponse.parcelLifeCycleData.shipmentInfo.parcelLabelNumber') AS PakketNummer
   , JSON_EXTRACT_SCALAR(Response, '$.parcellifecycleResponse.parcelLifeCycleData.shipmentInfo.serviceElements[0].content[1]') AS Content
   , ARRAY(
      SELECT STRUCT(
--         split_scans as s
        JSON_EXTRACT_SCALAR(split_scans, '$.date') AS ScanDatetime
        , JSON_EXTRACT_SCALAR(split_scans, '$.scanData.scanDepot.number') AS ScanDepotNumber
        , JSON_EXTRACT_SCALAR(split_scans, '$.scanData.location') AS ScanLocation
        , JSON_EXTRACT_SCALAR(split_scans, '$.scanData.scanType.name') AS ScanTypeName
        , JSON_EXTRACT_SCALAR(split_scans, '$.scanDescription.content[0]') AS ScanDescription
      )
    FROM (
      SELECT
        CONCAT('{"date"', REGEXP_REPLACE(split_scans, r'^\[{"date"|}\]$', ''), '}') AS split_scans
      FROM UNNEST(SPLIT(JSON_EXTRACT(Response, '$.parcellifecycleResponse.parcelLifeCycleData.scanInfo.scan'), '},{"date"')) AS split_scans
    )
   ) AS scans
   , Response
FROM cte_uniquetransports t