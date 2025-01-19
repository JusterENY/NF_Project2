/*
Имеется витрина dm.loan_holiday_info, которая содержит информацию по кредитным каникулам, 
сделке и продукте, который был предоставлен клиенту в рамках сделки. 
После проверки качества данных выявилась проблема отсутствия некоторого количества записей 
в источниках витрины для некоторых периодов эффективности строк.

Источниками для данной витрины являются 3 таблицы:
- rd.deal – информация по сделкам клиентов
- rd.loan_holiday – информация о кредитных каникулах: дата начала, конца, тип кредитных каникул
- rd.product – информация о продуктах банка, которые он предоставляет

При формировании витрины был подготовлен прототип витрины, по которому она собирается, loan_holiday_info_prototype.sql.

Также, источник, из которого были загружены данные сущности на слой rd, предоставил актуальные выгрузки имеющихся у них данных в формате csv.

Необходимо проанализировать витрину, актуальное состояние таблиц-источников витрины 
и определить, по каким датам эффективности (effective_from_date или effective_to_date) отсутствуют строки 
и определить, какой способ загрузки новых данных подойдет: полная перегрузка таблицы или загрузка части данных.

Для загрузки данных на слой RD необходимо воспользоваться аналогичным методом загрузки данных из CSV файла в БД, 
которым Вы пользовались в первом задании. 
После того, как данные будут успешно загружены, необходимо написать процедуру, 
которая будет повторять шаги из прототипа для выполнения перегрузки данных в витрину.
*/
SELECT * FROM dm.loan_holiday_info;
SELECT * FROM rd.deal_info;
SELECT * FROM stage.deal_info;
SELECT * FROM rd.product;
SELECT * FROM stage.product_info;

with cte as (
SELECT 'rd.deal_info' as tb, * FROM rd.deal_info
union all
SELECT 'stage.deal_info' as tb, 
	cast(deal_rk as bigint),
    cast(deal_num as text),
    cast(deal_name as text),
    cast(deal_sum as numeric),
    cast(client_rk as bigint),
    cast(account_rk as bigint),
    cast(agreement_rk as bigint),
    cast(deal_start_date as date),
    cast(department_rk as bigint),
    cast(product_rk as bigint),
    cast(deal_type_cd as text),
    cast(effective_from_date as date),
    cast(effective_to_date as date)
	FROM stage.deal_info
)
select COUNT(1), effective_from_date, effective_to_date, tb
from cte
GROUP BY effective_from_date, effective_to_date, tb
ORDER BY effective_from_date, effective_to_date, tb;

with cte as (
SELECT 'rd.product' as tb, * FROM rd.product
UNION ALL
SELECT 'stage.product_info' as tb,  
    cast(product_rk as bigint),
    cast(product_name as text),
    cast(effective_from_date as date),
    cast(effective_to_date as date)
FROM stage.product_info
)
select COUNT(1), effective_from_date, effective_to_date, tb
from cte c
GROUP BY effective_from_date, effective_to_date, tb
ORDER BY effective_from_date, effective_to_date, tb;

with cte as (
SELECT 'rd.product' as tb, * FROM rd.product
UNION ALL
SELECT 'stage.product_info' as tb,  
    cast(product_rk as bigint),
    cast(product_name as text),
    cast(effective_from_date as date),
    cast(effective_to_date as date)
FROM stage.product_info
)
, rown as (
select * , ROW_NUMBER () over (partition by product_rk order by c.tb) as rn
from cte c
FULL join cte cc USING(product_rk,product_name,effective_from_date, effective_to_date)
WHERE c.tb != cc.tb
)
select *
from rown
where rn > 2
;

select * from rd.product where product_rk in (1308366,1668282,1956251,1979096) order by product_rk;
select * from stage.product_info where product_rk in (1308366,1668282,1956251,1979096) order by product_rk;

-- delete from rd.deal_info where effective_from_date = '2023-03-15'

INSERT INTO rd.deal_info 
SELECT 
	cast(deal_rk as bigint),
    cast(deal_num as text),
	cast(deal_name as text),
    cast(deal_sum as numeric),
    cast(client_rk as bigint),
    cast(account_rk as bigint),
    cast(agreement_rk as bigint),
    cast(deal_start_date as date),
    cast(department_rk as bigint),
    cast(product_rk as bigint),
    cast(deal_type_cd as text),
    cast(effective_from_date as date),
    cast(effective_to_date as date)
FROM stage.deal_info sti
WHERE deal_rk IS NOT NULL
		AND client_rk IS NOT NULL
   		AND account_rk IS NOT NULL
   		AND agreement_rk IS NOT NULL
   		AND effective_from_date IS NOT NULL
   		AND effective_to_date IS NOT NULL
		AND not exists (select * from rd.deal_info rdi where rdi.effective_from_date = cast(sti.effective_from_date as date));

TRUNCATE TABLE rd.product;

INSERT INTO rd.product
SELECT DISTINCT  
    cast(product_rk as bigint),
    cast(product_name as text),
    cast(effective_from_date as date),
    cast(effective_to_date as date)
FROM stage.product_info
WHERE product_rk IS NOT NULL
    AND effective_from_date IS NOT NULL
    AND effective_to_date IS NOT NULL;

CREATE OR REPLACE PROCEDURE dm.load_loan_holiday_info() 
LANGUAGE plpgsql 
AS $$
BEGIN 
TRUNCATE dm.loan_holiday_info;

WITH deal as (
select  deal_rk
	   ,deal_num --Номер сделки
	   ,deal_name --Наименование сделки
	   ,deal_sum --Сумма сделки
	   ,client_rk --Ссылка на клиента
	   ,account_rk /* Примечание!!! 
	   В скрипте прототипа витрины loan_holiday_info_prototype.sql этот столбец пропущен, 
	   но т.к. он в оригинальной витрине есть и заполнен, то и мы заполняем */
	   ,agreement_rk --Ссылка на договор
	   ,deal_start_date --Дата начала действия сделки
	   ,department_rk --Ссылка на отделение
	   ,product_rk -- Ссылка на продукт
	   ,deal_type_cd
	   ,effective_from_date
	   ,effective_to_date
from RD.deal_info
), loan_holiday as (
select  deal_rk
	   ,loan_holiday_type_cd  --Ссылка на тип кредитных каникул
	   ,loan_holiday_start_date     --Дата начала кредитных каникул
	   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
	   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
	   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
	   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
	   ,effective_from_date
	   ,effective_to_date
from RD.loan_holiday
), product as (
select product_rk
	  ,product_name
	  ,effective_from_date
	  ,effective_to_date
from RD.product
), holiday_info as (
select   d.deal_rk
        ,lh.effective_from_date
        ,lh.effective_to_date
        ,d.deal_num as deal_number --Номер сделки
	    ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
        ,d.deal_name --Наименование сделки
        ,d.deal_sum --Сумма сделки
		,d.account_rk /* Примечание!!! 
	   В скрипте прототипа витрины loan_holiday_info_prototype.sql этот столбец пропущен, 
	   но т.к. он в оригинальной витрине есть и заполнен, то и мы заполняем */
        ,d.client_rk --Ссылка на контрагента
        ,d.agreement_rk --Ссылка на договор
        ,d.deal_start_date --Дата начала действия сделки
        ,d.department_rk --Ссылка на ГО/филиал
        ,d.product_rk -- Ссылка на продукт
        ,p.product_name -- Наименование продукта
        ,d.deal_type_cd -- Наименование типа сделки
from deal d
left join loan_holiday lh on 1=1
                             and d.deal_rk = lh.deal_rk
                             and d.effective_from_date = lh.effective_from_date
left join product p on p.product_rk = d.product_rk
					   and p.effective_from_date = d.effective_from_date
					   and p.product_name = d.deal_name /* Примечание!!! 
					   добавил вот это условие чтобы дубли продуктов по product_rk но с разными наименованиями грузились соответственно */					   
)
INSERT INTO dm.loan_holiday_info (
    deal_rk,
    effective_from_date,
    effective_to_date,
    agreement_rk,
    account_rk, /* Примечание!!! 
	   В скрипте прототипа витрины loan_holiday_info_prototype.sql этот столбец пропущен, 
	   но т.к. он в оригинальной витрине есть и заполнен, то и мы заполняем */
    client_rk,
    department_rk,
    product_rk,
    product_name,
    deal_type_cd,
    deal_start_date,
    deal_name,
    deal_number,
    deal_sum,
    loan_holiday_type_cd,
    loan_holiday_start_date,
    loan_holiday_finish_date,
    loan_holiday_fact_finish_date,
    loan_holiday_finish_flg,
    loan_holiday_last_possible_date
)
SELECT deal_rk
		,effective_from_date
		,effective_to_date
		,agreement_rk
		,account_rk /* Примечание!!! 
	   В скрипте прототипа витрины loan_holiday_info_prototype.sql этот столбец пропущен, 
	   но т.к. он в оригинальной витрине есть и заполнен, то и мы заполняем */
		,client_rk
		,department_rk
		,product_rk
		,product_name
		,deal_type_cd
		,deal_start_date
		,deal_name
		,deal_number
		,deal_sum
		,loan_holiday_type_cd
		,loan_holiday_start_date
		,loan_holiday_finish_date
		,loan_holiday_fact_finish_date
		,loan_holiday_finish_flg
		,loan_holiday_last_possible_date
FROM holiday_info;

END;
$$;

CALL dm.load_loan_holiday_info();

SELECT * FROM dm.loan_holiday_info;
SELECT distinct effective_from_date FROM dm.loan_holiday_info;