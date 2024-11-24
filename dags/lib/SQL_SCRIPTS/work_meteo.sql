/*

  postgis extension to be added. this will enable the GIS functions available on PostgreSQL.
  Tables to create:

    - WORK_METEO: for loading the cleaned data coming from the sources. 2 main purposes:
      - defines the structure of the final table
      - deduplicate the records base on the surrogate key (observation_pk)

    - WORK_MISSING: for inputing low-frequency missing cases for region and provinces. leveraging GIS functions it defines per each unreconciliated city which is its province and region  

*/
CREATE EXTENSION postgis;

create table if not exists WORK_METEO (
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

truncate table WORK_METEO;

WITH drv AS (
    SELECT 
        row_number() over (partition by c.denominazione_ita, m.time_converted) rnk,
        md5(cast(coalesce(trim(lower(c.denominazione_ita)),trim(replace(m.city,'-',' '))) as text) || cast(m.time_converted as text)) AS observation_pk,
        case 
          when c.sigla_provincia = 'AO' then 'Val d''Aosta' 
          when c.sigla_provincia = 'SU' then 'Sardegna'
          when c.sigla_provincia = 'SP' then 'Liguria'
          else p.region
        end region,
        case 
          when c.sigla_provincia = 'AO' then 'Val d''Aosta' 
          when c.sigla_provincia = 'SU' then 'Sardegna'
          when c.sigla_provincia = 'SP' then 'La Spezia'
        else p.province_name 
        end province,
        c.sigla_provincia,
        coalesce(trim(lower(c.denominazione_ita)), trim(replace(m.city, '-', ' '))) AS city,
        m.time_converted AS observation_datetime,
        m.summary,
        m."precipIntensity",
        m."precipAccumulation",
        m."precipType",
        m."temperature",
        m."apparentTemperature",
        m."dewPoint",
        m."pressure",
        m."windSpeed",
        m."windGust",
        m."windBearing",
        m."cloudCover",
        m."snowAccumulation",
        current_timestamp AS INSERT_TIMESTAMP,
        current_timestamp AS UPDATE_TIMESTAMP
    FROM raw_hourly_meteo m
    LEFT JOIN raw_city c 
        ON m.latitude = c.lat
        AND m.longitude = c.lon
    LEFT JOIN raw_province p
        ON p.sigla_provincia = c.sigla_provincia
)

INSERT INTO WORK_METEO (
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
    observation_datetime,
    summary,
    "precipIntensity",
    "precipAccumulation",
    "precipType",
    "temperature",
    "apparentTemperature",
    "dewPoint",
    "pressure",
    "windSpeed",
    "windGust",
    "windBearing",
    "cloudCover",
    "snowAccumulation",
    INSERT_TIMESTAMP,
    UPDATE_TIMESTAMP
FROM drv
WHERE rnk = 1
;

drop table if exists tmp;
create temporary table tmp as 
    select distinct ST_MakePoint(lon,lat) point, *
    from raw_city
    where trim(lower(denominazione_ita)) in (
                                select distinct city
                                from work_meteo
                                where region is null
                                )
;

create table if not exists WORK_MISSING (
        CITY text
      , PROVINCE_CODE text
      , PROVINCE text
      , REGION text
);

truncate table WORK_MISSING;
insert into WORK_MISSING
  select 
        trim(lower(c.denominazione_ita)) 
      , p.sigla_provincia
      , p.province_name
      , p.region
  from tmp c   
      inner join raw_province p
          on ST_within(c.point, p.province_boundaries)
  ;