CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__STAGING.currencies_history(
    currency_code 		INT				NOT NULL,
    currency_code_with	INT 			NOT NULL,
    date_update 		TIMESTAMP 		NOT NULL,
    currency_with_dev 	NUMERIC(19,2) 	NOT NULL
)
ORDER BY currency_code, currency_code_with
SEGMENTED BY HASH(currency_code, currency_code_with) ALL NODES
PARTITION BY date_update::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 1, 2);