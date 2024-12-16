-- STG-таблицы
-- Вместо truncate используем drop
--drop table if exists public.aadv_stg_transactions;
create table public.aadv_stg_transactions (
	trans_id varchar(20),
	trans_date timestamp(0),
	card_num varchar(20),
	oper_type varchar(20),
	amt decimal,
	oper_result varchar(20),
	terminal varchar(10)
);

--drop table if exists public.aadv_stg_terminals;
create table public.aadv_stg_terminals (
	terminal_id varchar(10),
	terminal_type varchar(20),
	terminal_city varchar(20),
	terminal_address varchar(100),
	update_dt timestamp(0)
);

--drop table if exists public.aadv_stg_blacklist;
create table public.aadv_stg_blacklist (
	passport_num varchar(20),
	entry_dt timestamp(0),
	update_dt timestamp(0)
);

--drop table if exists public.aadv_stg_clients;
create table public.aadv_stg_clients (
	client_id varchar(20),
	last_name varchar(50),
	first_name varchar(50),
	patronymic varchar(50),
	date_of_birth date,
	passport_num varchar(20),
	passport_valid_to date,
	phone varchar(16),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

--drop table if exists public.aadv_stg_accounts;
create table public.aadv_stg_accounts (
	account varchar(20),
	valid_to date,
	client varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

--drop table if exists public.aadv_stg_cards;
create table public.aadv_stg_cards (
	card_num varchar(20),
	account varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

create table public.aadv_stg_terminals_del (
	terminal_id varchar(10)
);

create table public.aadv_stg_clients_del (
	client_id varchar(20)
);

create table public.aadv_stg_accounts_del (
	account varchar(20)
);

create table public.aadv_stg_cards_del (
	card_num varchar(20)
);

-- DIM-таблицы
create table public.aadv_dwh_dim_clients_hist (
	client_id varchar(20),
	last_name varchar(50),
	first_name varchar(50),
	patronymic varchar(50),
	date_of_birth date,
	passport_num varchar(20),
	passport_valid_to date,
	phone varchar(16),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg boolean
);

create table public.aadv_dwh_dim_accounts_hist (
	account varchar(20),
	valid_to date,
	client varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg boolean
);

create table public.aadv_dwh_dim_cards_hist (
	card_num varchar(20),
	account varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg boolean
);

create table public.aadv_dwh_dim_terminals_hist (
	terminal_id varchar(10),
	terminal_type varchar(20),
	terminal_city varchar(20),
	terminal_address varchar(100),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg boolean
);

-- FACT-таблицы
create table public.aadv_dwh_fact_transactions (
	trans_id varchar(20),
	trans_date timestamp(0),
	card_num varchar(20),
	oper_type varchar(20),
	amt decimal,
	oper_result varchar(20),
	terminal varchar(10)
);

create table public.aadv_dwh_fact_passport_blacklist (
	passport_num varchar(20),
	entry_dt timestamp(0)
);

-- Таблица для отчетов
create table public.aadv_rep_fraud (
	event_dt timestamp(0),
	passport varchar(20),
	fio varchar (100),
	phone varchar(16),
	event_type varchar(20),
	report_dt date,
);

-- Таблица для метаданных
create table public.aadv_meta_dwh (
	schema_name varchar(30)
    table_name varchar(50)
    max_update_dt timestamp(0)
);
