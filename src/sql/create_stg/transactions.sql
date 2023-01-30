CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__STAGING.transactions(
    operation_id             UUID           NOT NULL,
    account_number_from      INT            NOT NULL,
    account_number_to        INT            NOT NULL,
    currency_code            INT            NOT NULL,
    country                  VARCHAR(100)   NOT NULL,
    "status"                 VARCHAR(15)    NOT NULL,
    transaction_type         VARCHAR(30)    NOT NULL,
    amount                   INT            NOT NULL,
    transaction_dt           TIMESTAMP      NOT NULL
)
ORDER BY operation_id
SEGMENTED BY HASH(operation_id) ALL NODES
PARTITION BY transaction_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(transaction_dt::DATE, 1, 2)