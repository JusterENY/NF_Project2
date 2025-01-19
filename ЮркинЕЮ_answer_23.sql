SELECT * FROM rd.account_balance order by account_rk,effective_date;

/* 1) Подготовить запрос, который определит корректное значение поля account_in_sum. 
Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются, то корректным выбирается значение account_out_sum предыдущего дня. */

SELECT 
	account_rk, 
	effective_date, 
	CASE 
		WHEN account_in_sum != COALESCE(LAG(account_out_sum) OVER (ORDER BY account_rk,effective_date) ,0)
		THEN COALESCE(LAG(account_out_sum) OVER (ORDER BY account_rk,effective_date) ,0)
		ELSE account_in_sum
	END AS new_account_in_sum, 
	account_in_sum, 
	account_out_sum
FROM rd.account_balance;

/* 2) Подготовить такой же запрос, только проблема теперь в том, что account_in_sum одного дня правильная, а account_out_sum предыдущего дня некорректна. 
Это означает, что если эти значения отличаются, то корректным значением для account_out_sum предыдущего дня выбирается значение account_in_sum текущего дня. */

SELECT 
	account_rk, 
	effective_date, 
	account_in_sum, 
	account_out_sum,
	CASE 
		WHEN account_out_sum != COALESCE(LEAD(account_in_sum) OVER (ORDER BY account_rk,effective_date) ,0)
		THEN COALESCE(LEAD(account_in_sum) OVER (ORDER BY account_rk,effective_date) ,0)
		ELSE account_out_sum
	END AS new_account_out_sum	
FROM rd.account_balance;

 /* 3) Подготовить запрос, который поправит данные в таблице rd.account_balance используя уже имеющийся запрос из п.1 */

WITH new_in AS (
SELECT 
	account_rk, 
	effective_date, 
	CASE 
		WHEN account_in_sum != COALESCE(LAG(account_out_sum) OVER (ORDER BY account_rk,effective_date) ,0)
		THEN COALESCE(LAG(account_out_sum) OVER (ORDER BY account_rk,effective_date) ,0)
		ELSE account_in_sum
	END AS new_account_in_sum, 
	account_in_sum, 
	account_out_sum
FROM rd.account_balance
)
UPDATE rd.account_balance rab
SET account_in_sum = new_in.new_account_in_sum
FROM new_in 
WHERE new_in.account_rk = rab.account_rk
	AND new_in.effective_date = rab.effective_date
	AND new_in.new_account_in_sum != rab.account_in_sum;

/* Написать процедуру по аналогии с задание 2.2 для перезагрузки данных в витрину */

SELECT * FROM dm.dict_currency;

INSERT INTO dm.dict_currency
VALUES ('500', 'KZT', '1900-01-01', '2999-12-31');

select *
from rd.account_balance ab
natural join rd.account ra;

CREATE OR REPLACE PROCEDURE dm.load_account_balance_turnover() 
LANGUAGE plpgsql 
AS $$
BEGIN 
TRUNCATE dm.account_balance_turnover;

INSERT INTO dm.account_balance_turnover (
    account_rk,
    currency_name,
    department_rk,
    effective_date,
    account_in_sum,
    account_out_sum
)
SELECT a.account_rk,
	   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
	   a.department_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd
WHERE ab.effective_date IS NOT NULL;

END;
$$;

CALL dm.load_account_balance_turnover();

SELECT * FROM dm.account_balance_turnover;