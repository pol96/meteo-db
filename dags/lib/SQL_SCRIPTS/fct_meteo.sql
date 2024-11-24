/*

    FCT_METEO, the final table containing all the reconciliated data from sources. 
    The pipeline looks for the primary key (observation_pk, a surrogate key based on city and timestamp); in case of concurrency it updates the update_timestamp technical field
    the purpose of this is to make idempotent the pipeline for multiple runs 

*/
create table if not exists FCT_METEO (
      OBSERVATION_PK text not null primary key
    , REGION text
    , PROVINCE text
    , PROVINCE_CODE text
    , CITY text not null 
    , OBSERVATION_DT timestamp not null 
    , SUMMARY text
    , PRECIP_INTENSITY numeric
    , PRECIP_ACCUMULATION numeric
    , PRECIP_TYPE  text
    , TEMPERATURE numeric
    , APPARENT_TEMPERATURE numeric
    , DEW_POINT numeric
    , PRESSURE numeric
    , WIND_SPEED numeric
    , WIND_GUST numeric
    , WINDB_EARING numeric
    , CLOUD_COVER numeric
    , SNOW_ACCUMULATION numeric
    , INSERT_TIMESTAMP timestamp
    , UPDATE_TIMESTAMP timestamp
);

WITH drv AS (
    SELECT 
        m.observation_pk,
        coalesce(m.region,c.region) region,
        coalesce(m.province,c.province) province,
        coalesce(m.province_code,c.province_code) sigla_provincia,
        coalesce(m.city, c.city) AS city,
        m.observation_dt,
        m.summary,
        m.precip_intensity,
        m.precip_accumulation,
        m.precip_type,
        m.temperature,
        m.apparent_temperature,
        m.dew_point,
        m.pressure,
        m.wind_speed,
        m.wind_gust,
        m.windb_earing,
        m.cloud_cover,
        m.snow_accumulation,
        m.insert_timestamp,
        m.update_timestamp
    FROM work_meteo m
    LEFT JOIN work_missing c 
        ON m.city = c.city
)

INSERT INTO FCT_METEO (
      OBSERVATION_PK
    , REGION
    , PROVINCE
    , PROVINCE_CODE
    , CITY
    , OBSERVATION_DT
    , SUMMARY
    , PRECIP_INTENSITY
    , PRECIP_ACCUMULATION
    , PRECIP_TYPE
    , TEMPERATURE
    , APPARENT_TEMPERATURE
    , DEW_POINT
    , PRESSURE
    , WIND_SPEED
    , WIND_GUST
    , WINDB_EARING
    , CLOUD_COVER
    , SNOW_ACCUMULATION
    , INSERT_TIMESTAMP
    , UPDATE_TIMESTAMP
)
SELECT 
    observation_pk,
    region,
    province,
    sigla_provincia,
    city,
    observation_dt,
    summary,
    precip_intensity,
    precip_accumulation,
    precip_type,
    temperature,
    apparent_temperature,
    dew_point,
    pressure,
    wind_speed,
    wind_gust,
    windb_earing,
    cloud_cover,
    snow_accumulation,
    insert_timestamp,
    update_timestamp
FROM drv
ON CONFLICT (observation_pk) 
DO UPDATE SET 
    UPDATE_TIMESTAMP = EXCLUDED.UPDATE_TIMESTAMP;