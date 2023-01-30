CREATE TABLE IVIVCHIKYANDEXRU__STAGING.transactions_new LIKE IVIVCHIKYANDEXRU__STAGING.transactions INCLUDING PROJECTIONS;

INSERT INTO IVIVCHIKYANDEXRU__STAGING.transactions_new(operation_id,
                                                       account_number_from,
                                                       account_number_to,
                                                       currency_code,
                                                       country,
                                                       "status",
                                                       transaction_type,
                                                       amount,
                                                       transaction_dt)
    SELECT DISTINCT operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    country,
                    "status",
                    transaction_type,
                    amount,
                    transaction_dt
               FROM IVIVCHIKYANDEXRU__STAGING.transactions;


DROP TABLE IVIVCHIKYANDEXRU__STAGING.transactions;

ALTER TABLE IVIVCHIKYANDEXRU__STAGING.transactions_new RENAME TO transactions;