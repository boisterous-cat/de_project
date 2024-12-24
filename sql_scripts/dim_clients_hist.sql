-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).
insert into public.aadv_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
select 
    stg.client_id,
    stg.last_name,
    stg.first_name,
    stg.patronymic,
    stg.date_of_birth,
    stg.passport_num,
    stg.passport_valid_to,
    stg.phone,
    stg.update_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
	False
from public.aadv_stg_clients stg
left join public.aadv_dwh_dim_clients_hist tgt
on stg.client_id = tgt.client_id
where tgt.client_id is null;

-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).

CREATE TEMP TABLE temp_updated_clients AS
WITH updated_clients AS (
    SELECT
        stg.client_id,
        stg.last_name,
        stg.first_name,
        stg.patronymic,
        stg.date_of_birth,
        stg.passport_num,
        stg.passport_valid_to,
        stg.phone,
        stg.update_dt
    FROM
        public.aadv_stg_clients stg
    INNER JOIN
        public.aadv_dwh_dim_clients_hist tgt
    ON
        stg.client_id = tgt.client_id
    WHERE
         tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
        AND
        (COALESCE(stg.last_name, '') != COALESCE(tgt.last_name, '') OR
        COALESCE(stg.first_name, '') != COALESCE(tgt.first_name, '') OR
        COALESCE(stg.patronymic, '') != COALESCE(tgt.patronymic, '') OR
        COALESCE(stg.date_of_birth, '1899-01-01')::date != COALESCE(tgt.date_of_birth, '1899-01-01')::date OR
        COALESCE(stg.passport_num, '') != COALESCE(tgt.passport_num, '') OR
        COALESCE(stg.passport_valid_to, '1899-01-01')::date != COALESCE(tgt.passport_valid_to, '1899-01-01')::date OR
        COALESCE(stg.phone, '') != COALESCE(tgt.phone, ''))
)
SELECT * FROM updated_clients;

--обновленные записи будут помечены как "неактивные" с помощью изменения даты окончания их действия
UPDATE public.aadv_dwh_dim_clients_hist
SET effective_to = temp_updated_clients.update_dt - interval '1 second'
FROM temp_updated_clients
WHERE public.aadv_dwh_dim_clients_hist.client_id = temp_updated_clients.client_id
    AND public.aadv_dwh_dim_clients_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

-- Insert new records after the update
INSERT INTO public.aadv_dwh_dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
SELECT
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    update_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
    False
FROM temp_updated_clients;

DROP TABLE temp_updated_clients;

-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).
CREATE TEMP TABLE temp_deleted_clients AS
WITH deleted_clients AS (
    SELECT
        tgt.client_id,
        tgt.last_name,
        tgt.first_name,
        tgt.patronymic,
        tgt.date_of_birth,
        tgt.passport_num,
        tgt.passport_valid_to,
        tgt.phone,
        (SELECT max(update_dt) FROM public.aadv_stg_clients) AS deleted_dt
    FROM
        public.aadv_stg_clients_del stg
    --будут выбраны все записи из tgt, даже если для них нет соответствующих записей в stg
    RIGHT JOIN
        public.aadv_dwh_dim_clients_hist tgt ON stg.client_id = tgt.client_id
    WHERE
        stg.client_id IS NULL --аккаунты, которые были удалены
        AND tgt.deleted_flg != TRUE
        AND tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
)
SELECT * FROM deleted_clients;

-- аккаунт больше не действителен с момента deleted_dt
UPDATE public.aadv_dwh_dim_clients_hist
SET effective_to = dc.deleted_dt - interval '1 second'
FROM temp_deleted_clients dc
WHERE
    public.aadv_dwh_dim_clients_hist.client_id = dc.client_id
    --которые все еще имеют effective_to равным 2999-12-31
    AND public.aadv_dwh_dim_clients_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

-- Insert new records for deleted accounts
INSERT INTO public.aadv_dwh_dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
SELECT
    dc.client_id,
    dc.last_name,
    dc.first_name,
    dc.patronymic,
    dc.date_of_birth,
    dc.passport_num,
    dc.passport_valid_to,
    dc.phone,
    dc.deleted_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
    TRUE
FROM temp_deleted_clients dc;

DROP TABLE temp_deleted_clients;


-- 4. Обновление метаданных.
update public.aadv_meta_dwh
set max_update_dt = coalesce((select max(effective_from) from public.aadv_dwh_dim_clients_hist),
                             (select max(max_update_dt) from public.aadv_meta_dwh where schema_name = 'public' and table_name = 'aadv_dwh_dim_clients_hist'))
where schema_name = 'public'
    and table_name = 'aadv_dwh_dim_clients_hist';

-- 5. Фиксация транзакции.
commit;