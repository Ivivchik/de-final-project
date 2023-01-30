CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.s_currency_exchange(
    h_currency_exchange_pk       INT            REFERENCES IVIVCHIKYANDEXRU__DWH.l_currency_exchange (h_currency_exchange_pk),
    currency_with_dev            NUMERIC(19,2)  NOT NULL CHECK (currency_with_dev > 0),
    date_update					 DATETIME       NOT NULL,
    load_dt                      DATETIME       NOT NULL,
    load_src                     VARCHAR(10)    NOT NULL
)
ORDER BY h_currency_exchange_pk
SEGMENTED BY HASH(h_currency_exchange_pk) ALL NODES
PARTITION BY date_update::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 1, 2);