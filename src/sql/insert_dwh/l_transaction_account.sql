 INSERT INTO IVIVCHIKYANDEXRU__DWH.l_transaction_account(h_transaction_account_pk,
                                                         h_transaction_pk,
                                                         h_account_number_from,
                                                         h_account_number_to,
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
 
    SELECT DISTINCT HASH(operation_id || account_number_from || account_number_to),
                    HASH(operation_id),
                    HASH(account_number_from),
                    HASH(account_number_to),
                    NOW(),
                    's3'
               FROM grouped_transaction t 
              WHERE HASH(operation_id || account_number_from || account_number_to) NOT IN (
                    SELECT h_transaction_account_pk 
                      FROM IVIVCHIKYANDEXRU__DWH.l_transaction_account)