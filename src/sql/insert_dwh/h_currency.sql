INSERT INTO IVIVCHIKYANDEXRU__DWH.h_currency(h_currency_code_pk,
												currency_code,
                                                load_dt,
                                               load_src)

    SELECT DISTINCT HASH(currency_code), 
    				currency_code,
                    NOW() load_dt,
                    's3' load_src
               FROM IVIVCHIKYANDEXRU__STAGING.currencies_history
              WHERE HASH(currency_code) NOT IN (SELECT h_currency_code_pk 
                                                  FROM IVIVCHIKYANDEXRU__DWH.h_currency); 