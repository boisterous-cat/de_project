-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).
insert into public.aadv_dwh_dim_accounts_hist( account, valid_to, client, effective_from, effective_to, deleted_flg )
select 
    stg.account,
    stg.valid_to,
    stg.client,
    stg.update_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
	False
from public.aadv_stg_accounts stg
left join public.aadv_dwh_dim_accounts_hist tgt
on stg.account = tgt.account
where tgt.account is null;

-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).
--выбирает записи, которые были изменены (из aadv_stg_accounts) по сравнению с историческими данными из (aadv_dwh_dim_accounts_hist)

CREATE TEMP TABLE temp_updated_accounts AS
WITH updated_accounts AS (
    SELECT
        stg.account,
        stg.valid_to,
        stg.client,
        stg.update_dt
    FROM
        public.aadv_stg_accounts stg
    INNER JOIN
        public.aadv_dwh_dim_accounts_hist tgt
    ON
        stg.account = tgt.account
    WHERE
        (stg.valid_to != tgt.valid_to
            OR (stg.valid_to IS NULL AND tgt.valid_to IS NOT NULL)
            OR (stg.valid_to IS NOT NULL AND tgt.valid_to IS NULL)
            OR stg.client != tgt.client
            OR (stg.client IS NULL AND tgt.client IS NOT NULL)
            OR (stg.client IS NOT NULL AND tgt.client IS NULL))
        AND tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
)
SELECT * FROM updated_accounts;

--обновленные записи будут помечены как "неактивные" с помощью изменения даты окончания их действия
UPDATE public.aadv_dwh_dim_accounts_hist
SET effective_to = temp_updated_accounts.update_dt - interval '1 second'
FROM temp_updated_accounts
WHERE public.aadv_dwh_dim_accounts_hist.account = temp_updated_accounts.account
    AND public.aadv_dwh_dim_accounts_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

-- Insert new records after the update
INSERT INTO public.aadv_dwh_dim_accounts_hist (account, valid_to, client, effective_from, effective_to, deleted_flg)
SELECT
    account,
    valid_to,
    client,
    update_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
    False
FROM temp_updated_accounts;

DROP TABLE temp_updated_accounts;

-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).
CREATE TEMP TABLE temp_deleted_accounts AS
WITH deleted_accounts AS (
    SELECT
        tgt.account,
        tgt.valid_to,
        tgt.client,
        (SELECT max(update_dt) FROM public.aadv_stg_accounts) AS deleted_dt
    FROM
        public.aadv_stg_accounts_del stg
    --будут выбраны все записи из tgt, даже если для них нет соответствующих записей в stg
    RIGHT JOIN
        public.aadv_dwh_dim_accounts_hist tgt ON stg.account = tgt.account
    WHERE
        stg.account IS NULL --аккаунты, которые были удалены
        AND tgt.deleted_flg != TRUE
        AND tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
)
SELECT * FROM deleted_accounts;

-- аккаунт больше не действителен с момента deleted_dt
UPDATE public.aadv_dwh_dim_accounts_hist
SET effective_to = da.deleted_dt - interval '1 second'
FROM temp_deleted_accounts da
WHERE
    public.aadv_dwh_dim_accounts_hist.account = da.account
    --которые все еще имеют effective_to равным 2999-12-31
    AND public.aadv_dwh_dim_accounts_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

-- Insert new records for deleted accounts
INSERT INTO public.aadv_dwh_dim_accounts_hist (account, valid_to, client, effective_from, effective_to, deleted_flg)
SELECT
    da.account,
    da.valid_to,
    da.client,
    da.deleted_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
    TRUE
FROM temp_deleted_accounts da;

DROP TABLE temp_deleted_accounts;


-- 4. Обновление метаданных.
update public.aadv_meta_dwh
set max_update_dt = coalesce((select max(effective_from) from public.aadv_dwh_dim_accounts_hist),
                             (select max(max_update_dt) from public.aadv_meta_dwh where schema_name = 'public' and table_name = 'aadv_dwh_dim_accounts_hist'))
where schema_name = 'public'
    and table_name = 'aadv_dwh_dim_accounts_hist';

-- 5. Фиксация транзакции.
commit;