DROP TABLE IF EXISTS fin_risk.fct_daily_returns;

CREATE TABLE fin_risk.fct_daily_returns
WITH (
  format = 'PARQUET',
  external_location = 's3://fin-risk-observatory-joanne/analytics/fct_daily_returns/'
) AS
SELECT
  symbol                                   AS symbol,
  CAST(date AS date)                       AS trade_date,
  close                                    AS close_price,
  (close / prev_close) - 1                 AS daily_return
FROM (
  SELECT
    symbol,
    date,
    close,
    lag(close) OVER (
      PARTITION BY symbol
      ORDER BY CAST(date AS date)
    ) AS prev_close
  FROM fin_risk.prices_daily_csv
)
WHERE prev_close IS NOT NULL;

