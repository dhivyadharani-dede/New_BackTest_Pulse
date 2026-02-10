DROP TABLE IF EXISTS public.stg_nifty_options;

CREATE TABLE public.stg_nifty_options (
    date        date,
    expiry      text,      -- keep as TEXT (do NOT use date yet)
    strike      numeric,
    option_type text,
    time        time,
    open        numeric,
    high        numeric,
    low         numeric,
    close       numeric
);



/*$folder = "D:\downloads\nifty_historic_data(OHLC)\newwwwww\cumulative_data_monthly"

Get-ChildItem "$folder\*.csv" | ForEach-Object {
    Write-Host "Loading $($_.Name)..."
    psql -h localhost -U postgres -d Backtest_Pulse `
      -c "\copy public.stg_nifty_options(date, expiry, strike, option_type, time, open, high, low, close)
          FROM '$($_.FullName)' CSV HEADER"
}
*/

-- total rows
SELECT count(*) FROM public.stg_nifty_options LIMIT 20;

DELETE FROM public.stg_nifty_options a
USING public.stg_nifty_options b
WHERE a.ctid > b.ctid
  AND a.date        = b.date
  AND a.expiry      = b.expiry
  AND a.strike      = b.strike
  AND a.option_type = b.option_type
  AND a.time        = b.time;

CREATE UNIQUE INDEX IF NOT EXISTS ux_nifty_options_unique
ON public."Nifty_options" (
    date,
    expiry,
    strike,
    option_type,
    time
);


INSERT INTO public."Nifty_options" (
    date,
    expiry,
    strike,
    option_type,
    time,
    open,
    high,
    low,
    close
)
SELECT
    s.date,
    s.expiry::date,          -- convert safely (see note below)
    s.strike,
    UPPER(s.option_type),
    s.time,
    s.open,
    s.high,
    s.low,
    s.close
FROM public.stg_nifty_options s
WHERE s.expiry ~ '^\d{4}-\d{2}-\d{2}$'   -- only valid date strings
ON CONFLICT (date, expiry, strike, option_type, time)
DO NOTHING;
