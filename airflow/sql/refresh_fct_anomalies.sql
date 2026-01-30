DROP TABLE IF EXISTS fin_risk.fct_anomalies;

CREATE TABLE fin_risk.fct_anomalies
WITH (
  format = 'PARQUET',
  external_location = 's3://fin-risk-observatory-joanne/analytics/fct_anomalies/'
) AS
WITH base AS (
  SELECT
    symbol                                         AS symbol,
    CAST(date AS date)                             AS trade_date,
    close                                          AS close_price,
    volume                                         AS volume,
    lag(close) OVER (
      PARTITION BY symbol
      ORDER BY CAST(date AS date)
    )                                              AS prev_close,
    avg(volume) OVER (
      PARTITION BY symbol
      ORDER BY CAST(date AS date)
      ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    )                                              AS avg_volume_20
  FROM fin_risk.prices_daily_csv
),
calc AS (
  SELECT
    symbol                                         AS symbol,
    trade_date                                     AS trade_date,
    close_price                                    AS close_price,
    volume                                         AS volume,
    (close_price / prev_close) - 1                 AS daily_return,
    CASE
      WHEN avg_volume_20 IS NULL OR avg_volume_20 = 0 THEN NULL
      ELSE volume / avg_volume_20
    END                                            AS volume_multiple
  FROM base
  WHERE prev_close IS NOT NULL
)
SELECT
  symbol                                           AS symbol,
  trade_date                                       AS trade_date,
  close_price                                      AS close_price,
  volume                                           AS volume,
  daily_return                                     AS daily_return,
  volume_multiple                                  AS volume_multiple,
  CASE WHEN abs(daily_return) > 0.05 THEN 1 ELSE 0 END AS flag_big_move,
  CASE WHEN volume_multiple IS NOT NULL AND volume_multiple > 3 THEN 1 ELSE 0 END AS flag_vol_spike
FROM calc
WHERE abs(daily_return) > 0.05
   OR (volume_multiple IS NOT NULL AND volume_multiple > 3);

