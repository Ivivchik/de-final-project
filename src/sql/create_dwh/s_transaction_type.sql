CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.s_transaction_type(
    hk_transaction_type_pk      INT            ,
    h_transaction_pk            INT            NOT NULL REFERENCES IVIVCHIKYANDEXRU__DWH.h_transaction (h_transaction_pk),
    "type"                      VARCHAR(30)     NOT NULL,
    load_dt                     DATETIME        NOT NULL,
    load_src                    VARCHAR(10)     NOT NULL,
    PRIMARY KEY (hk_transaction_type_pk) ENABLED,
    CONSTRAINT h_transaction_pk_unique UNIQUE (h_transaction_pk) ENABLED
)
ORDER BY hk_transaction_type_pk
SEGMENTED BY HASH(hk_transaction_type_pk) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 1, 2);