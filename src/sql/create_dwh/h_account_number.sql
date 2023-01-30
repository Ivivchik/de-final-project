CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.h_account_number(
    h_account_number_pk      INT            ,
    account_number           INT            NOT NULL,
    load_dt                  DATETIME       NOT NULL,
    load_src                 VARCHAR(10)    NOT NULL,
    PRIMARY KEY (h_account_number_pk) ENABLED,
    CONSTRAINT account_number_unique UNIQUE (account_number) ENABLED
)
ORDER BY h_account_number_pk
SEGMENTED BY HASH(h_account_number_pk) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 1, 2);
