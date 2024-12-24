-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).
insert into public.aadv_dwh_dim_terminals_hist( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg  )
select 
    stg.terminal_id,
    stg.terminal_type,
    stg.terminal_city,
    stg.terminal_address,
    stg.update_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
	False
from public.aadv_stg_terminals stg
left join public.aadv_dwh_dim_terminals_hist tgt
on stg.terminal_id = tgt.terminal_id
where tgt.terminal_id is null;

-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).
--выбирает записи, которые были изменены (из aadv_stg_accounts) по сравнению с историческими данными из (aadv_dwh_dim_terminals_hist)

CREATE TEMP TABLE temp_updated_terminals AS
WITH updated_terminals AS (
    SELECT
        stg.terminal_id,
        stg.terminal_type,
        stg.terminal_city,
        stg.terminal_address,
        stg.update_dt
    FROM
        public.aadv_stg_terminals stg
    INNER JOIN
        public.aadv_dwh_dim_terminals_hist tgt
    ON
        stg.terminal_id = tgt.terminal_id
    WHERE
        tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
        AND
        --The IS DISTINCT FROM operator evaluates to true if the two values are different, including when one is null and the other is not
        (stg.terminal_type IS DISTINCT FROM tgt.terminal_type
           OR stg.terminal_city IS DISTINCT FROM tgt.terminal_city
           OR stg.terminal_address IS DISTINCT FROM tgt.terminal_address
   )
)
SELECT * FROM updated_terminals;

--обновленные записи будут помечены как "неактивные" с помощью изменения даты окончания их действия
UPDATE public.aadv_dwh_dim_terminals_hist
SET effective_to = temp_updated_terminals.update_dt - interval '1 second'
FROM temp_updated_terminals
WHERE public.aadv_dwh_dim_terminals_hist.terminal_id = temp_updated_terminals.terminal_id
    AND public.aadv_dwh_dim_terminals_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

-- Insert new records after the update
INSERT INTO public.aadv_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
SELECT
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    update_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
    False
FROM temp_updated_terminals;

DROP TABLE temp_updated_terminals;

-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).
CREATE TEMP TABLE temp_deleted_terminals AS
WITH deleted_terminals AS (
    SELECT
        tgt.terminal_id,
        tgt.terminal_type,
        tgt.terminal_city,
        tgt.terminal_address,
        (SELECT max(update_dt) FROM public.aadv_stg_terminals) AS deleted_dt
    FROM
        public.aadv_stg_terminals_del stg
    --будут выбраны все записи из tgt, даже если для них нет соответствующих записей в stg
    RIGHT JOIN
        public.aadv_dwh_dim_terminals_hist tgt ON stg.terminal_id = tgt.terminal_id
    WHERE
        stg.terminal_id IS NULL --аккаунты, которые были удалены
        AND tgt.deleted_flg != TRUE
        AND tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
)
SELECT * FROM deleted_terminals;

-- аккаунт больше не действителен с момента deleted_dt
UPDATE public.aadv_dwh_dim_terminals_hist
SET effective_to = dt.deleted_dt - interval '1 second'
FROM temp_deleted_terminals dt
WHERE
    public.aadv_dwh_dim_terminals_hist.terminal_id = dt.terminal_id
    --которые все еще имеют effective_to равным 2999-12-31
    AND public.aadv_dwh_dim_terminals_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

-- Insert new records for deleted accounts
INSERT INTO public.aadv_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
SELECT
    dt.terminal_id,
    dt.terminal_type,
    dt.terminal_city,
    dt.terminal_address,
    dt.deleted_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
    TRUE
FROM temp_deleted_terminals dt;

DROP TABLE temp_deleted_terminals;


-- 4. Обновление метаданных.
update public.aadv_meta_dwh
set max_update_dt = coalesce((select max(effective_from) from public.aadv_dwh_dim_terminals_hist),
                             (select max(max_update_dt) from public.aadv_meta_dwh where schema_name = 'public' and table_name = 'aadv_dwh_dim_terminals_hist'))
where schema_name = 'public'
    and table_name = 'aadv_dwh_dim_terminals_hist';

-- 5. Фиксация транзакции.
commit;