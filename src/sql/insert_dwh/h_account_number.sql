INSERT INTO IVIVCHIKYANDEXRU__DWH.h_account_number(h_account_number_pk,
                                                account_number,
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
    
    ),
    accounts AS (
    
        SELECT DISTINCT account_number_FROM account_number
          FROM grouped_transaction
    
         UNION
    
        SELECT DISTINCT account_number_to account_number 
          FROM grouped_transaction
    )
                                                
                                                
    SELECT DISTINCT HASH(account_number),
                    account_number,
                    NOW() load_dt,
                    's3' load_src
               FROM accounts
              WHERE HASH(account_number) NOT IN (SELECT h_account_number_pk
                                                   FROM IVIVCHIKYANDEXRU__DWH.h_account_number);
