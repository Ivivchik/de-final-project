INSERT INTO IVIVCHIKYANDEXRU__DWH.s_transaction_status(hk_transaction_status_pk,
                                                       h_transaction_pk, 
                                                       status,
                                                       transaction_dt,
                                                       load_dt,
                                                       load_src)
    WITH grouped_transaction AS (
    SELECT tt.*
      FROM (SELECT COUNT(DISTINCT account_number_to) cnt,
                   COUNT(DISTINCT currency_code) cnt2,
                   operation_id
  	          FROM IVIVCHIKYANDEXRU__STAGING.transactions
             WHERE account_number_from > 0 
  	      GROUP BY operation_id) t
  	  JOIN IVIVCHIKYANDEXRU__STAGING.transactions tt ON tt.operation_id = t.operation_id
  	 WHERE cnt = 1 
       AND cnt2 = 1
       AND tt.account_number_from > 0

    )
	  SELECT DISTINCT HASH(operation_id || status),
                      HASH(operation_id),
                      status,
                      transaction_dt,
                      NOW(),
                      's3' 
                 FROM grouped_transaction t
                WHERE HASH(operation_id || status) NOT IN (
                      SELECT hk_transaction_status_pk
                        FROM IVIVCHIKYANDEXRU__DWH.s_transaction_status)
