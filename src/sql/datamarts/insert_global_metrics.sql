INSERT INTO IVIVCHIKYANDEXRU__DWH.global_metrics(date_update,
												 report_date,
												 currency_from,
												 amount_total,
												 cnt_transactions,
												 avg_transactions_per_account,
												 cnt_accounts_make_transactions)

WITH currency_exchange AS(

	SELECT sce.currency_with_dev,
		   hc.currency_code,
		   hc.h_currency_code_pk
	  FROM IVIVCHIKYANDEXRU__DWH.l_currency_exchange lce 
      JOIN IVIVCHIKYANDEXRU__DWH.h_currency hc
	  	ON hc.h_currency_code_pk = lce.h_currency_code
      JOIN IVIVCHIKYANDEXRU__DWH.h_currency hc1
	    ON hc1.h_currency_code_pk  = lce.h_currency_code_with   
      JOIN IVIVCHIKYANDEXRU__DWH.s_currency_exchange sce
	    ON sce.h_currency_exchange_pk  = lce.h_currency_exchange_pk
	 WHERE sce.date_update::DATE = '{{ ds }}'
	   AND hc1.currency_code = 420),

transaction_info AS (
	SELECT ht.h_transaction_pk,
		   CASE
		   	WHEN sta.amount < 0 THEN sta.amount * -1
			ELSE sta.amount
		   END AS amount
	  FROM IVIVCHIKYANDEXRU__DWH.h_transaction ht 
	  JOIN IVIVCHIKYANDEXRU__DWH.s_transaction_amount sta
	    ON sta.h_transaction_pk = ht.h_transaction_pk 
	  JOIN IVIVCHIKYANDEXRU__DWH.s_transaction_status sts
	    ON sts.h_transaction_pk = ht.h_transaction_pk 
	  JOIN IVIVCHIKYANDEXRU__DWH.s_transaction_type stt
	    ON stt.h_transaction_pk = ht.h_transaction_pk 
	 WHERE sts.status = 'done'
	   AND sts.transaction_dt::DATE = '{{ ds }}'
	   AND stt."type" != 'authorisation'
),

metrics AS (
	SELECT ti.h_transaction_pk,
		   ti.amount,
		   han.h_account_number_pk,
		   COALESCE(ce.currency_code, 420) AS currency_from,
		   CASE
		   		WHEN ce.currency_code IS NULL THEN ti.amount 
				ELSE ti.amount * ce.currency_with_dev
		   END AS amount_in_dollar
      FROM transaction_info ti
      JOIN IVIVCHIKYANDEXRU__DWH.l_transaction_account lta
	    ON lta.h_transaction_pk = ti.h_transaction_pk
      JOIN IVIVCHIKYANDEXRU__DWH.h_account_number han
	    ON han.h_account_number_pk = lta.h_account_number_from
      JOIN IVIVCHIKYANDEXRU__DWH.l_transaction_currency ltc
	    ON ltc.h_transaction_pk = ti.h_transaction_pk
 LEFT JOIN currency_exchange ce
        ON ce.h_currency_code_pk = ltc.h_currency_code_pk
)

  SELECT '{{ ds }}',
		 NOW(),
  		 currency_from,
         SUM(amount_in_dollar) AS amount_total,
  	     COUNT(h_transaction_pk) cnt_transactions, 
  	     COUNT(h_transaction_pk) / COUNT(DISTINCT h_account_number_pk) avg_transactions_per_account,
		 COUNT(DISTINCT h_account_number_pk) cnt_accounts_make_transactions
    FROM metrics
GROUP BY currency_from