drop table if exists aadv_tmp_data_mart;

create temporary table aadv_tmp_data_mart as (
    with max_update as (
select
	max_update_dt::date - interval '1 second' as max_dt
from
	public.aadv_meta_dwh
where
	schema_name = 'public'
	and table_name = 'aadv_dwh_fact_transactions'
    ),
-- транзакции + счета
trns_crds as (
select
	fact_trans.trans_date,
	fact_trans.trans_id,
	fact_trans.card_num,
	fact_trans.oper_type,
	fact_trans.oper_result,
	dim_cards.account,
	fact_trans.terminal,
	fact_trans.amt
from
	public.aadv_dwh_fact_transactions fact_trans
left join public.aadv_dwh_dim_cards_hist dim_cards
            on
	fact_trans.card_num = dim_cards.card_num
	-- учитывая период действия записи  по дате транзакции
	and fact_trans.trans_date between dim_cards.effective_from and dim_cards.effective_to
	-- фильтруем транзакции, которые произошли после изменений в таблице meta
	-- берем свежие транзакции, чтобы старые не тянулис
where
	fact_trans.trans_date >= (
	select
		max_dt
	from
		max_update)
    ),
-- + город терминала
trns_crds_trml as (
select
	tc.*,
	dim_terms.terminal_city
from
	trns_crds tc
left join public.aadv_dwh_dim_terminals_hist dim_terms
            on
	tc.terminal = dim_terms.terminal_id
	and tc.trans_date between dim_terms.effective_from and dim_terms.effective_to
    ),
-- + действие счета
trns_crds_trml_acnt as (
select
	tc.*,
	dim_accounts.client,
	dim_accounts.valid_to
from
	trns_crds_trml tc
left join public.aadv_dwh_dim_accounts_hist dim_accounts
            on
	tc.account = dim_accounts.account
	and tc.trans_date between dim_accounts.effective_from and dim_accounts.effective_to
    )
-- + данные по клиенту
select
		tc.*,
		concat(dim_clients.last_name,
	' ',
	dim_clients.first_name,
	' ',
	dim_clients.patronymic) as fio,
		dim_clients.passport_num,
		dim_clients.passport_valid_to,
		dim_clients.phone
from
	trns_crds_trml_acnt tc
left join public.aadv_dwh_dim_clients_hist dim_clients on
	tc.client = dim_clients.client_id
	and tc.trans_date between dim_clients.effective_from and dim_clients.effective_to
);
-- 2. определяем типы мошеннических операций
drop table if exists aadv_tmp_report;
-- просроченный паспорт
create temporary table aadv_tmp_report as (
select
	dm.trans_date as event_dt,
	dm.passport_num as passport,
	dm.fio,
	dm.phone,
	'outdated passport' as event_type,
	dm.trans_date::date as report_dt
from
	aadv_tmp_data_mart dm
where
	dm.trans_date > dm.passport_valid_to
union all
select
	dm.trans_date as event_dt,
	dm.passport_num as passport,
	dm.fio,
	dm.phone,
	'blacklist passport' as event_type,
	dm.trans_date::date as report_dt
from
	aadv_tmp_data_mart dm
inner join
    public.aadv_dwh_fact_passport_blacklist fact_black
on
	dm.passport_num = fact_black.passport_num
where
	dm.trans_date > fact_black.entry_dt
union all
--недействующий договор
select
	dm.trans_date as event_dt,
	dm.passport_num as passport,
	dm.fio,
	dm.phone,
	'outdated contract' as event_type,
	dm.trans_date::date as report_dt
from
	aadv_tmp_data_mart dm
where
	dm.trans_date > dm.valid_to
union all
--совершение операций в разных городах за короткое время
select
	trans_date as event_dt,
	passport_num as passport,
	fio,
	phone,
	'different cities' as event_type,
	trans_date::date as report_dt
from
	(
	select
		tmp.*,
		lag(trans_date) over (partition by card_num
	order by
		trans_date) as prev_trans_date,
		lag(terminal_city) over (partition by card_num
	order by
		trans_date) as prev_terminal_city
	from
		aadv_tmp_data_mart tmp
) s
where
	terminal_city != prev_terminal_city
	and trans_date <= prev_trans_date + interval '1 hour'
union all
-- 	Попытка подбора суммы
select
	dates[1] as event_dt,
	passport_num,
	fio,
	phone,
	'suspicious ammounts' as event_type,
	dates[1]::date as report_dt
from
	(
	select
		card_num,
		array_agg(amt
	order by
		trans_date desc) as amounts,
		array_agg(oper_result
	order by
		trans_date desc) as results,
		array_agg(trans_date
	order by
		trans_date desc) as dates,
		max(passport_num) as passport_num,
		-- use max to aggregate passport_num
		max(fio) as fio,
		-- use max to aggregate fio
		max(phone) as phone
		-- use max to aggregate phone
	from
		(
		select
			trans_date,
			passport_num,
			fio,
			phone,
			card_num,
			amt,
			oper_result,
			row_number() over (partition by card_num
		order by
			trans_date desc) as rn
		from
			aadv_tmp_data_mart
		where
			oper_type = 'withdraw'
    ) as recent_transactions
	where
		rn <= 4
	group by
		card_num
) as aggregated_transactions
where
	array_length(amounts,
	1) = 4
		and dates[1] <= dates[4] + interval '20 minutes'
		and results[1] = 'success'
		and results[2] = 'reject'
		and results[3] = 'reject'
		and amounts[1] < amounts[2]
		and amounts[2] < amounts[3]
		and amounts[3] < amounts[4]
    );

insert
	into
	public.aadv_rep_fraud( event_dt,
	passport,
	fio,
	phone,
	event_type,
	report_dt)
select
	event_dt,
	passport,
	fio,
	phone,
	event_type,
	report_dt
from
	aadv_tmp_report
where
	event_dt::date >= (
	select
		max_update_dt::date
	from
		public.aadv_meta_dwh
	where
		schema_name = 'public'
		and table_name = 'aadv_dwh_fact_transactions'
		);
--обновляем метаданные
update
	public.aadv_meta_dwh
set
	max_update_dt = coalesce((
	select
		max(report_dt)
	from
		aadv_tmp_report),
	(
	select
		max(max_update_dt)
	from
		public.aadv_meta_dwh
	where
		schema_name = 'public'
		and table_name = 'aadv_rep_fraud'))
where
	schema_name = 'public'
	and table_name = 'aadv_rep_fraud';

drop table if exists aadv_tmp_data_mart;

drop table if exists aadv_tmp_report;
-- фиксация транзакции.
commit;