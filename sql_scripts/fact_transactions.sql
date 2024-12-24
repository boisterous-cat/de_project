-- 1. Загрузка в приемник инкремента.
insert into public.aadv_dwh_fact_transactions( trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal )
select
    stg.trans_id,
    stg.trans_date,
    stg.card_num,
    stg.oper_type,
    stg.amt,
    stg.oper_result,
    stg.terminal
--from public.aadv_stg_transactions stg;
-- Условие where tgt.trans_id is null гарантирует, что будут вставлены только те записи из stg, которые не имеют соответствия в tgt (т.е. только те, которых еще нет в целевой таблице).
from public.aadv_stg_transactions stg
    left join public.aadv_dwh_fact_transactions tgt on stg.trans_id = tgt.trans_id
        and stg.trans_date = stg.trans_date
where true
    and tgt.trans_id is null;

-- 2. Обновление метаданных.
update public.aadv_meta_dwh
set max_update_dt = coalesce((select max(trans_date::date) from public.aadv_stg_transactions),
                             (select max(max_update_dt) from public.aadv_meta_dwh where schema_name = 'public' and table_name = 'aadv_dwh_fact_transactions'))
where true
    and schema_name = 'public'
    and table_name = 'aadv_dwh_fact_transactions';

-- 3. Фиксация транзакции.
commit;