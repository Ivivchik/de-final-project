CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.l_transaction_account(
    h_transaction_account_pk   INT          ,
    h_transaction_pk           INT          NOT NULL REFERENCES IVIVCHIKYANDEXRU__DWH.h_transaction (h_transaction_pk),
    h_account_number_FROM      INT          NOT NULL REFERENCES IVIVCHIKYANDEXRU__DWH.h_account_number (h_account_number_pk),
    h_account_number_to        INT          NOT NULL REFERENCES IVIVCHIKYANDEXRU__DWH.h_account_number (h_account_number_pk),
    load_dt                    DATETIME      NOT NULL,
    load_src                   VARCHAR(10)   NOT NULL,
    PRIMARY KEY (h_transaction_account_pk) ENABLED,
    CONSTRAINT h_transaction_pk_unique UNIQUE (h_transaction_pk) ENABLED
)
ORDER BY h_transaction_account_pk
SEGMENTED BY HASH(h_transaction_account_pk) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 1, 2);