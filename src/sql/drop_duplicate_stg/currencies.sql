CREATE TABLE IVIVCHIKYANDEXRU__STAGING.currencies_history_new LIKE IVIVCHIKYANDEXRU__STAGING.currencies_history INCLUDING PROJECTIONS;

INSERT INTO IVIVCHIKYANDEXRU__STAGING.currencies_history_new(currency_code,
                                                             currency_code_with,
                                                             date_update,
                                                             currency_with_dev)
    SELECT DISTINCT currency_code,
                    currency_code_with,
                    date_update,
                    currency_with_dev
               FROM IVIVCHIKYANDEXRU__STAGING.currencies_history;


DROP TABLE IVIVCHIKYANDEXRU__STAGING.currencies_history;

ALTER TABLE IVIVCHIKYANDEXRU__STAGING.currencies_history_new RENAME TO currencies_history;