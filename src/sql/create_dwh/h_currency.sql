CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.h_currency(
    h_currency_code_pk       INT          	,
    currency_code            INT            NOT NULL,
    load_dt                  DATETIME       NOT NULL,
    load_src                 VARCHAR(10)    NOT NULL,
    PRIMARY KEY (h_currency_code_pk) ENABLED,
    CONSTRAINT currency_code_unique UNIQUE (currency_code) ENABLED
)
ORDER BY h_currency_code_pk
SEGMENTED BY HASH(h_currency_code_pk) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 1, 2);