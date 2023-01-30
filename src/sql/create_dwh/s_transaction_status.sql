CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.s_transaction_status(
    hk_transaction_status_pk    INT             ,
    h_transaction_pk            INT             NOT NULL REFERENCES IVIVCHIKYANDEXRU__DWH.h_transaction (h_transaction_pk),
    "status"                    VARCHAR(15)     NOT NULL,
    transaction_dt              DATETIME        NOT NULL,
    load_dt                     DATETIME        NOT NULL,
    load_src                    VARCHAR(10)     NOT NULL,
    PRIMARY KEY (hk_transaction_status_pk) ENABLED
)
ORDER BY hk_transaction_status_pk
SEGMENTED BY HASH(hk_transaction_status_pk) ALL NODES
PARTITION BY transaction_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(transaction_dt::DATE, 1, 2);