-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).
insert into public.aadv_dwh_dim_cards_hist( card_num, account, effective_from, effective_to, deleted_flg )
select 
    stg.card_num,
    stg.account,
    stg.update_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
	False
from public.aadv_stg_cards stg
left join public.aadv_dwh_dim_cards_hist tgt
on stg.card_num = tgt.card_num
where tgt.card_num is null;

-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).
--выбирает записи, которые были изменены (из aadv_stg_accounts) по сравнению с историческими данными из (aadv_dwh_dim_accounts_hist)

CREATE TEMP TABLE temp_updated_cards AS
WITH updated_cards AS (
    SELECT
        stg.card_num,
        stg.account,
        stg.update_dt
    FROM
        public.aadv_stg_cards stg
    INNER JOIN
        public.aadv_dwh_dim_cards_hist tgt
    ON
        stg.account = tgt.account
    WHERE
        tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
        AND
        (stg.account != tgt.account
            or (stg.account is null and tgt.account is not null)
            or (stg.account is not null and tgt.account is null)
        )
)
SELECT * FROM updated_cards;

--обновленные записи будут помечены как "неактивные" с помощью изменения даты окончания их действия
UPDATE public.aadv_dwh_dim_cards_hist
SET effective_to = temp_updated_cards.update_dt - interval '1 second'
FROM temp_updated_cards
WHERE public.aadv_dwh_dim_cards_hist.card_num = temp_updated_cards.card_num
    AND public.aadv_dwh_dim_cards_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

-- Insert new records after the update
INSERT INTO public.aadv_dwh_dim_cards_hist (card_num, account, effective_from, effective_to, deleted_flg)
SELECT
    card_num,
    account,
    update_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
    False
FROM temp_updated_cards;

DROP TABLE temp_updated_cards;

-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).
CREATE TEMP TABLE temp_deleted_cards AS
WITH deleted_cards AS (
    SELECT
        tgt.card_num,
        tgt.account,
        (SELECT max(update_dt) FROM public.aadv_stg_cards) AS deleted_dt
    FROM
        public.aadv_stg_cards_del stg
    --будут выбраны все записи из tgt, даже если для них нет соответствующих записей в stg
    RIGHT JOIN
        public.aadv_dwh_dim_cards_hist tgt ON stg.card_num = tgt.card_num
    WHERE
        stg.card_num IS NULL --аккаунты, которые были удалены
        AND tgt.deleted_flg != TRUE
        AND tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
)
SELECT * FROM deleted_cards;

-- аккаунт больше не действителен с момента deleted_dt
UPDATE public.aadv_dwh_dim_cards_hist
SET effective_to = dc.deleted_dt - interval '1 second'
FROM temp_deleted_cards dc
WHERE
    public.aadv_dwh_dim_cards_hist.card_num = dc.card_num
    --которые все еще имеют effective_to равным 2999-12-31
    AND public.aadv_dwh_dim_cards_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

-- Insert new records for deleted accounts
INSERT INTO public.aadv_dwh_dim_cards_hist (card_num, account, effective_from, effective_to, deleted_flg)
SELECT
    dc.card_num,
    dc.account,
    dc.deleted_dt,
    to_date('2999-12-31', 'YYYY-MM-DD')::timestamp,
    TRUE
FROM temp_deleted_cards dc;

DROP TABLE temp_deleted_cards;


-- 4. Обновление метаданных.
update public.aadv_meta_dwh
set max_update_dt = coalesce((select max(effective_from) from public.aadv_dwh_dim_cards_hist),
                             (select max(max_update_dt) from public.aadv_meta_dwh where schema_name = 'public' and table_name = 'aadv_dwh_dim_cards_hist'))
where schema_name = 'public'
    and table_name = 'aadv_dwh_dim_cards_hist';

-- 5. Фиксация транзакции.
commit;