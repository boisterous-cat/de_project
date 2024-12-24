-- 1. Загрузка в приемник инкремента.
--Он выбирает все записи из таблицы public.aadv_stg_blacklist и вставляет их в таблицу public.aadv_dwh_fact_passport_blacklist.
--Этот запрос не проверяет, существуют ли уже данные с такими же значениями passport_num и entry_dt в целевой таблице. Это может привести к дублированию записей, если в stg есть такие же значения, которые уже присутствуют в tgt

insert into public.aadv_dwh_fact_passport_blacklist( entry_dt, passport_num )
select
    stg.entry_dt,
    stg.passport_num
--from public.aadv_stg_blacklist stg;

--Он также выбирает записи из public.aadv_stg_blacklist, но перед вставкой выполняет левое соединение с таблицей public.aadv_dwh_fact_passport_blacklist.
--Условие and tgt.passport_num is null в блоке where гарантирует, что вставляемые записи будут только те, для которых нет соответствующих записей в целевой таблице (т.е. те, которые уже не существуют).
--Таким образом, этот запрос предотвращает дублирование записей в таблице public.aadv_dwh_fact_passport_blacklist.
from public.aadv_stg_blacklist stg
    left join public.aadv_dwh_fact_passport_blacklist tgt on stg.passport_num = tgt.passport_num
        and stg.entry_dt = stg.entry_dt
where true
    and tgt.passport_num is null;

-- 2. Обновление метаданных.
update public.aadv_meta_dwh
set max_update_dt = coalesce((select max(update_dt::date) from public.aadv_stg_blacklist),
                             (select max(max_update_dt) from public.aadv_meta_dwh where schema_name = 'public' and table_name = 'aadv_dwh_fact_passport_blacklist'))
where true
    and schema_name = 'public'
    and table_name = 'aadv_dwh_fact_passport_blacklist';

-- 3. Фиксация транзакции.
commit;