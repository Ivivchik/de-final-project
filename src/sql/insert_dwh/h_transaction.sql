INSERT INTO IVIVCHIKYANDEXRU__DWH.h_transaction(h_transaction_pk,
												transaction_id, 
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
    SELECT DISTINCT HASH(operation_id), 
    				operation_id,
                    NOW() load_dt,
                    's3' load_src
               FROM grouped_transaction
              WHERE HASH(operation_id) NOT IN (SELECT h_transaction_pk 
                                                 FROM IVIVCHIKYANDEXRU__DWH.h_transaction);
