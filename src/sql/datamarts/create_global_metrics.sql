CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.global_metrics(
    date_update                         DATETIME           NOT NULL,
    report_date                         DATETIME           NOT NULL,
    currency_FROM                       INT                NOT NULL,
    amount_total                        NUMERIC(19,2)      NOT NULL,
    cnt_transactions                    INT                NOT NULL,
    avg_transactions_per_account        NUMERIC(19,2)      NOT NULL,
    cnt_accounts_make_transactions      INT                NOT NULL
)
ORDER BY date_update, currency_FROM
SEGMENTED BY HASH(date_update, currency_FROM) ALL NODES
PARTITION BY date_update::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 1, 2);