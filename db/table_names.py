# Имена таблиц
# STG-таблицы

initials = 'aadv_'

stg_transactions = initials+'stg_transactions'
stg_terminals = initials+'stg_terminals'
stg_blacklist = initials + 'stg_blacklist'
stg_clients = initials + 'stg_clients'
stg_accounts = initials + 'stg_accounts'
stg_cards = initials + 'stg_cards'

stg_terminals_del = initials + 'stg_terminals_del'
stg_clients_del = initials + 'stg_clients_del'
stg_accounts_del = initials + 'stg_accounts_del'
stg_cards_del = initials + 'stg_cards_del'

# DIM-таблицы
dwh_dim_clients_hist = initials + 'dwh_dim_clients_hist'
dwh_dim_accounts_hist = initials + 'dwh_dim_accounts_hist'
dwh_dim_cards_hist = initials + 'dwh_dim_cards_hist'
dwh_dim_terminals_hist = initials + 'dwh_dim_terminals_hist'

# FACT-таблицы
dwh_fact_transactions = initials + 'dwh_fact_transactions'
dwh_fact_passport_blacklist = initials + 'dwh_fact_passport_blacklist'

# Таблица для отчетов
rep_fraud = initials + 'rep_fraud'

# Таблица для метаданных
meta_dwh = initials + 'meta_dwh'

