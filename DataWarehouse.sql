-- =========================================================
-- NYC Energy Data Warehouse Schema
-- Star Schema: dim_date, dim_development, dim_meter,
--              dim_vendor, dim_rate, fact_energy_consumption
-- =========================================================

-- 1. Create / select database
CREATE DATABASE IF NOT EXISTS nyc_energy_dw;
USE nyc_energy_dw;

-- =========================================================
-- 2. (Optional) Staging table – assumed to be filled
--    from your previous ETL step (CSV/API -> MySQL)
--    If you already created this, keep your existing version.
-- =========================================================
/*
CREATE TABLE stg_electric_consumption (
    `:id`                VARCHAR(64),
    `:version`           VARCHAR(64),
    `:created_at`        DATETIME,
    `:updated_at`        DATETIME,
    development_name     VARCHAR(255),
    borough              VARCHAR(50),
    account_name         VARCHAR(255),
    location             VARCHAR(50),
    meter_amr            VARCHAR(20),
    tds                  VARCHAR(20),
    edp                  VARCHAR(20),
    rc_code              VARCHAR(20),
    funding_source       VARCHAR(50),
    amp                  VARCHAR(50),
    vendor_name          VARCHAR(255),
    umis_bill_id         VARCHAR(50),
    revenue_month        VARCHAR(7),       -- 'YYYY-MM'
    service_start_date   DATETIME,
    service_end_date     DATETIME,
    days                 VARCHAR(10),
    meter_number         VARCHAR(50),
    estimated            VARCHAR(5),
    current_charges      VARCHAR(50),
    rate_class           VARCHAR(50),
    bill_analyzed        VARCHAR(5),
    consumption_kwh      VARCHAR(50),
    kwh_charges          VARCHAR(50),
    consumption_kw       VARCHAR(50),
    kw_charges           VARCHAR(50),
    other_charges        VARCHAR(50),
    meter_scope          VARCHAR(255)
);
*/

-- =========================================================
-- 3. Dimension tables
-- =========================================================

-- 3.1 Date dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT PRIMARY KEY,      -- e.g. 20250522
    date_value      DATE NOT NULL,
    year            INT,
    quarter         TINYINT,
    month           TINYINT,
    month_name      VARCHAR(20),
    day             TINYINT,
    day_of_week     TINYINT
);

-- 3.2 Development dimension
CREATE TABLE IF NOT EXISTS dim_development (
    development_key INT AUTO_INCREMENT PRIMARY KEY,
    tds             INT,
    edp             INT,
    rc_code         VARCHAR(20),
    amp             VARCHAR(50),
    development_name VARCHAR(255),
    borough         VARCHAR(50),
    funding_source  VARCHAR(50)
);

-- 3.3 Meter dimension
CREATE TABLE IF NOT EXISTS dim_meter (
    meter_key       INT AUTO_INCREMENT PRIMARY KEY,
    meter_number    VARCHAR(50) UNIQUE,
    account_name    VARCHAR(255),
    meter_amr       VARCHAR(20),
    meter_scope     VARCHAR(255)
);

-- 3.4 Vendor dimension
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key      INT AUTO_INCREMENT PRIMARY KEY,
    vendor_name     VARCHAR(255) UNIQUE
);

-- 3.5 Rate / tariff dimension
CREATE TABLE IF NOT EXISTS dim_rate (
    rate_key        INT AUTO_INCREMENT PRIMARY KEY,
    rate_class      VARCHAR(50) UNIQUE
);

-- =========================================================
-- 4. Fact table
--    Grain: one utility bill for one meter for one billing period
-- =========================================================

CREATE TABLE IF NOT EXISTS fact_energy_consumption (
    fact_key                BIGINT AUTO_INCREMENT PRIMARY KEY,

    development_key         INT NOT NULL,
    meter_key               INT NOT NULL,
    vendor_key              INT NOT NULL,
    rate_key                INT NOT NULL,
    revenue_month_key       INT NOT NULL,
    service_start_date_key  INT NOT NULL,
    service_end_date_key    INT NOT NULL,

    umis_bill_id            VARCHAR(50),
    estimated               CHAR(1),
    bill_analyzed           CHAR(1),

    days                    INT,
    consumption_kwh         DECIMAL(14,3),
    kwh_charges             DECIMAL(12,2),
    consumption_kw          DECIMAL(12,3),
    kw_charges              DECIMAL(12,2),
    other_charges           DECIMAL(12,2),
    current_charges         DECIMAL(12,2),

    CONSTRAINT fk_fact_dev    FOREIGN KEY (development_key)
        REFERENCES dim_development(development_key),
    CONSTRAINT fk_fact_meter  FOREIGN KEY (meter_key)
        REFERENCES dim_meter(meter_key),
    CONSTRAINT fk_fact_vendor FOREIGN KEY (vendor_key)
        REFERENCES dim_vendor(vendor_key),
    CONSTRAINT fk_fact_rate   FOREIGN KEY (rate_key)
        REFERENCES dim_rate(rate_key),
    CONSTRAINT fk_fact_rev    FOREIGN KEY (revenue_month_key)
        REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_ssd    FOREIGN KEY (service_start_date_key)
        REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_sed    FOREIGN KEY (service_end_date_key)
        REFERENCES dim_date(date_key)
);

-- =========================================================
-- 5. Population scripts (DW load from staging)
--    Run AFTER stg_electric_consumption has data.
-- =========================================================

-- 5.1 Populate dim_date
INSERT IGNORE INTO dim_date (date_key, date_value, year, quarter, month, month_name, day, day_of_week)
SELECT
    DATE_FORMAT(d, '%Y%m%d') + 0 AS date_key,
    d AS date_value,
    YEAR(d) AS year,
    QUARTER(d) AS quarter,
    MONTH(d) AS month,
    DATE_FORMAT(d, '%M') AS month_name,
    DAY(d) AS day,
    DAYOFWEEK(d) AS day_of_week
FROM (
    SELECT DISTINCT DATE(service_start_date) AS d FROM stg_electric_consumption
    UNION
    SELECT DISTINCT DATE(service_end_date)   AS d FROM stg_electric_consumption
    UNION
    SELECT DISTINCT STR_TO_DATE(CONCAT(revenue_month, '-01'), '%Y-%m-%d') AS d
           FROM stg_electric_consumption
) AS x
WHERE d IS NOT NULL;

-- 5.2 Populate dim_development
INSERT INTO dim_development (tds, edp, rc_code, amp, development_name, borough, funding_source)
SELECT DISTINCT
    CAST(NULLIF(tds, '') AS SIGNED),
    CAST(NULLIF(edp, '') AS SIGNED),
    NULLIF(rc_code, ''),
    NULLIF(amp, ''),
    development_name,
    borough,
    funding_source
FROM stg_electric_consumption;

-- 5.3 Populate dim_meter
INSERT INTO dim_meter (meter_number, account_name, meter_amr, meter_scope)
SELECT DISTINCT
    meter_number,
    account_name,
    meter_amr,
    meter_scope
FROM stg_electric_consumption;

-- 5.4 Populate dim_vendor
INSERT INTO dim_vendor (vendor_name)
SELECT DISTINCT vendor_name
FROM stg_electric_consumption;

-- 5.5 Populate dim_rate
INSERT INTO dim_rate (rate_class)
SELECT DISTINCT rate_class
FROM stg_electric_consumption;

-- 5.6 Load fact table
INSERT INTO fact_energy_consumption (
    development_key,
    meter_key,
    vendor_key,
    rate_key,
    revenue_month_key,
    service_start_date_key,
    service_end_date_key,
    umis_bill_id,
    estimated,
    bill_analyzed,
    days,
    consumption_kwh,
    kwh_charges,
    consumption_kw,
    kw_charges,
    other_charges,
    current_charges
)
SELECT
    d.development_key,
    m.meter_key,
    v.vendor_key,
    r.rate_key,
    DATE_FORMAT(STR_TO_DATE(CONCAT(s.revenue_month, '-01'), '%Y-%m-%d'), '%Y%m%d') + 0 AS revenue_month_key,
    DATE_FORMAT(DATE(s.service_start_date), '%Y%m%d') + 0 AS service_start_date_key,
    DATE_FORMAT(DATE(s.service_end_date),   '%Y%m%d') + 0 AS service_end_date_key,

    s.umis_bill_id,
    s.estimated,
    s.bill_analyzed,
    CAST(NULLIF(s.days, '') AS SIGNED),
    CAST(NULLIF(s.consumption_kwh, '') AS DECIMAL(14,3)),
    CAST(NULLIF(s.kwh_charges, '')     AS DECIMAL(12,2)),
    CAST(NULLIF(s.consumption_kw, '')  AS DECIMAL(12,3)),
    CAST(NULLIF(s.kw_charges, '')      AS DECIMAL(12,2)),
    CAST(NULLIF(s.other_charges, '')   AS DECIMAL(12,2)),
    CAST(NULLIF(s.current_charges, '') AS DECIMAL(12,2))
FROM stg_electric_consumption s
JOIN dim_development d
  ON d.development_name = s.development_name
 AND d.borough          = s.borough
JOIN dim_meter m
  ON m.meter_number     = s.meter_number
JOIN dim_vendor v
  ON v.vendor_name      = s.vendor_name
JOIN dim_rate r
  ON r.rate_class       = s.rate_class;
