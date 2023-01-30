CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.s_transaction_country(
    hk_transaction_country_pk    INT           ,
    h_transaction_pk             INT           NOT NULL REFERENCES IVIVCHIKYANDEXRU__DWH.h_transaction (h_transaction_pk),
    country                      VARCHAR(100)   NOT NULL,
    load_dt                      DATETIME       NOT NULL,
    load_src                     VARCHAR(10)    NOT NULL,
    PRIMARY KEY (hk_transaction_country_pk) ENABLED,
    CONSTRAINT h_transaction_pk_unique UNIQUE (h_transaction_pk) ENABLED
)
ORDER BY hk_transaction_country_pk
SEGMENTED BY HASH(hk_transaction_country_pk) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 1, 2);