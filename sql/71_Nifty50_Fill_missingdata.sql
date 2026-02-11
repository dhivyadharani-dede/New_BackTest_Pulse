-- DROP and CREATE staging table for Nifty50
DROP TABLE IF EXISTS public.stg_nifty50;

CREATE TABLE public.stg_nifty50 (
    date        date,
    time        time,
    open        numeric,
    high        numeric,
    low         numeric,
    close       numeric,
    volume      numeric,
	 oi      text,
	option_nm  text
);
/*
$folder = "D:\downloads\nifty_historic_data(OHLC)\Nifty_spot"

Get-ChildItem "$folder\*.csv" | ForEach-Object {
    Write-Host "Loading $($_.Name)..."
    psql -h localhost -U postgres -d Backtest_Pulse `
      -c "\copy public.stg_nifty50(date, time, open, high, low, close, volume,oi,option_nm)
          FROM '$($_.FullName)' CSV HEADER"
}
*/

-- total rows
SELECT count(*) FROM public.stg_nifty50;

-- Remove duplicates
-- DELETE FROM public.stg_nifty50 a
-- USING public.stg_nifty50 b
-- WHERE a.ctid > b.ctid
--   AND a.date   = b.date
--   --AND a.symbol = b.symbol
--   AND a.time   = b.time;

CREATE UNIQUE INDEX IF NOT EXISTS ux_nifty50_unique
ON public."Nifty50" (
    date,
    time
);

INSERT INTO public."Nifty50" (
    date, time, open, high, low, close, volume, oi, option_nm
)
SELECT DISTINCT ON (date, time)
    date,
    time,
    open,
    high,
    low,
    close,
    volume,
    oi,
    option_nm
FROM public.stg_nifty50
WHERE date > '2020-12-31'
ORDER BY date, time, volume DESC
ON CONFLICT (date, time)
DO NOTHING;

