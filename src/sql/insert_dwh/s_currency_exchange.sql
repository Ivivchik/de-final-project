MERGE INTO IVIVCHIKYANDEXRU__DWH.s_currency_exchange trg
USING (
    SELECT DISTINCT HASH(currency_code || currency_code_with) AS h_currency_exchange_pk,
                    currency_with_dev,
                    date_update,
                    NOW() AS load_dt,
                    's3' AS load_src
               FROM IVIVCHIKYANDEXRU__STAGING.currencies_history) src
   ON trg.h_currency_exchange_pk = src.h_currency_exchange_pk
  AND trg.date_update = src.date_update
 WHEN MATCHED
         THEN UPDATE SET currency_with_dev = src.currency_with_dev, load_dt = src.load_dt
 WHEN NOT MATCHED 
         THEN INSERT (h_currency_exchange_pk, currency_with_dev, date_update, load_dt, load_src)
              VALUES (src.h_currency_exchange_pk, src.currency_with_dev, src.date_update, src.load_dt, src.load_src)