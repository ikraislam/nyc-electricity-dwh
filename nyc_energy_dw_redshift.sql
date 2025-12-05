-- =========================================================
-- NYC Energy Data Warehouse - Redshift DDL
-- Star schema: fact_energy_consumption + dimensions
-- =========================================================

-- 1. Create schema
CREATE SCHEMA IF NOT EXISTS nyc_energy_dw;
SET search_path TO nyc_energy_dw;

-- 2. Staging table (loaded from S3 via COPY)
--    Types are text-first; we will cast when loading dims/fact.
CREATE TABLE IF NOT EXISTS stg_electric_consumption (
    id                  VARCHAR(64),
    version             VARCHAR(64),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    development_name    VARCHAR(255),
    borough             VARCHAR(50),
    account_name        VARCHAR(255),
    location            VARCHAR(50),
    meter_amr           VARCHAR(20),
    tds                 VARCHAR(20),
    edp                 VARCHAR(20),
    rc_code             VARCHAR(20),
    funding_source      VARCHAR(50),
    amp                 VARCHAR(50),
    vendor_name         VARCHAR(255),
    umis_bill_id        VARCHAR(50),
    revenue_month       VARCHAR(7),        -- 'YYYY-MM'
    service_start_date  TIMESTAMP,
    service_end_date    TIMESTAMP,
    days                VARCHAR(10),
    meter_number        VARCHAR(50),
    estimated           VARCHAR(5),
    current_charges     VARCHAR(50),
    rate_class          VARCHAR(50),
    bill_analyzed       VARCHAR(5),
    consumption_kwh     VARCHAR(50),
    kwh_charges         VARCHAR(50),
    consumption_kw      VARCHAR(50),
    kw_charges          VARCHAR(50),
    other_charges       VARCHAR(50),
    meter_scope         VARCHAR(255)
);

-- =========================================================
-- Dimension tables
-- =========================================================

-- dim_date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER      NOT NULL PRIMARY KEY,  -- e.g. 20250522
    date_value      DATE         NOT NULL,
    year            INTEGER,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(20),
    day             SMALLINT,
    day_of_week     SMALLINT
)
DISTSTYLE ALL
SORTKEY(date_key);

-- dim_development
CREATE TABLE IF NOT EXISTS dim_development (
    development_key INTEGER IDENTITY(1,1) PRIMARY KEY,
    tds             INTEGER,
    edp             INTEGER,
    rc_code         VARCHAR(20),
    amp             VARCHAR(50),
    development_name VARCHAR(255),
    borough         VARCHAR(50),
    funding_source  VARCHAR(50)
)
DISTSTYLE ALL;

-- dim_meter
CREATE TABLE IF NOT EXISTS dim_meter (
    meter_key       INTEGER IDENTITY(1,1) PRIMARY KEY,
    meter_number    VARCHAR(50) UNIQUE,
    account_name    VARCHAR(255),
    meter_amr       VARCHAR(20),
    meter_scope     VARCHAR(255)
)
DISTSTYLE ALL;

-- dim_vendor
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key      INTEGER IDENTITY(1,1) PRIMARY KEY,
    vendor_name     VARCHAR(255) UNIQUE
)
DISTSTYLE ALL;

-- dim_rate
CREATE TABLE IF NOT EXISTS dim_rate (
    rate_key        INTEGER IDENTITY(1,1) PRIMARY KEY,
    rate_class      VARCHAR(50) UNIQUE
)
DISTSTYLE ALL;

-- =========================================================
-- Fact table
-- =========================================================

CREATE TABLE IF NOT EXISTS fact_energy_consumption (
    fact_key                BIGINT IDENTITY(1,1) PRIMARY KEY,

    development_key         INTEGER NOT NULL,
    meter_key               INTEGER NOT NULL,
    vendor_key              INTEGER NOT NULL,
    rate_key                INTEGER NOT NULL,
    revenue_month_key       INTEGER NOT NULL,
    service_start_date_key  INTEGER NOT NULL,
    service_end_date_key    INTEGER NOT NULL,

    umis_bill_id            VARCHAR(50),
    estimated               CHAR(1),
    bill_analyzed           CHAR(1),

    days                    INTEGER,
    consumption_kwh         DECIMAL(14,3),
    kwh_charges             DECIMAL(12,2),
    consumption_kw          DECIMAL(12,3),
    kw_charges              DECIMAL(12,2),
    other_charges           DECIMAL(12,2),
    current_charges         DECIMAL(12,2),

    CONSTRAINT fk_fact_dev    FOREIGN KEY (development_key)        REFERENCES dim_development(development_key),
    CONSTRAINT fk_fact_meter  FOREIGN KEY (meter_key)              REFERENCES dim_meter(meter_key),
    CONSTRAINT fk_fact_vendor FOREIGN KEY (vendor_key)             REFERENCES dim_vendor(vendor_key),
    CONSTRAINT fk_fact_rate   FOREIGN KEY (rate_key)               REFERENCES dim_rate(rate_key),
    CONSTRAINT fk_fact_rev    FOREIGN KEY (revenue_month_key)      REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_ssd    FOREIGN KEY (service_start_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_sed    FOREIGN KEY (service_end_date_key)   REFERENCES dim_date(date_key)
)
DISTSTYLE KEY
DISTKEY(development_key)
SORTKEY(revenue_month_key, development_key);
