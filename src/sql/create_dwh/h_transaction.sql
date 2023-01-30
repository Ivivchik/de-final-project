CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.h_transaction(
    h_transaction_pk   INT           ,
    transaction_id     UUID          NOT NULL,
    load_dt            DATETIME      NOT NULL,
    load_src           VARCHAR(10)   NOT NULL,
    PRIMARY KEY (h_transaction_pk) ENABLED,
    CONSTRAINT transaction_id_unique UNIQUE (transaction_id) ENABLED
)
ORDER BY h_transaction_pk
SEGMENTED BY HASH(h_transaction_pk) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 1, 2);