CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.l_currency_exchange(
    h_currency_exchange_pk   INT          ,
    h_currency_code      	 INT          NOT NULL REFERENCES IVIVCHIKYANDEXRU__DWH.h_currency (h_currency_code_pk),
    h_currency_code_with     INT          NOT NULL REFERENCES IVIVCHIKYANDEXRU__DWH.h_currency (h_currency_code_pk),
    load_dt                  DATETIME     NOT NULL,
    load_src                 VARCHAR(10)  NOT NULL,
    PRIMARY KEY (h_currency_exchange_pk) ENABLED
)
ORDER BY h_currency_exchange_pk
SEGMENTED BY HASH(h_currency_exchange_pk) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 1, 2);