INSERT INTO IVIVCHIKYANDEXRU__DWH.l_currency_exchange(h_currency_exchange_pk,
                                                      h_currency_code,
                                                      h_currency_code_with,
                                                      load_dt,
                                                      load_src)

    SELECT DISTINCT HASH(currency_code || currency_code_with),
                    HASH(currency_code),
                    HASH(currency_code_with),
                    NOW(),
                    's3' 
               FROM IVIVCHIKYANDEXRU__STAGING.currencies_history
              WHERE HASH(currency_code || currency_code_with) NOT IN (
                    SELECT h_currency_exchange_pk
                      FROM IVIVCHIKYANDEXRU__DWH.l_currency_exchange);
