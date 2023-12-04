CREATE TABLE IF NOT EXISTS deaise.grir_stg_transactions (
	trans_id varchar(30),
	trans_date varchar(20), 
	amt varchar(20),
	card_num varchar(20),
	oper_type varchar(20),	
	oper_result varchar(20),
	terminal varchar(20)
);
	
CREATE TABLE IF NOT EXISTS deaise.grir_stg_terminals (
	terminal_id varchar(20),
	terminal_type varchar(20),
	terminal_city varchar(30),
	terminal_address varchar(80),
    update_dt timestamp(0)	
);

CREATE TABLE IF NOT EXISTS deaise.grir_stg_passport_blacklist (
	entry_dt varchar(20),
	passport_num varchar(20)
);

CREATE TABLE IF NOT EXISTS deaise.grir_stg_cards (
	card_num varchar(20),
	account_num varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

CREATE TABLE IF NOT EXISTS deaise.grir_stg_accounts (
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

CREATE TABLE IF NOT EXISTS deaise.grir_stg_clients (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

CREATE TABLE IF NOT EXISTS deaise.grir_stg_del_terminals (
	terminal_id varchar(20)
);

CREATE TABLE IF NOT EXISTS deaise.grir_stg_del_cards (
	card_num varchar(20)
);

CREATE TABLE IF NOT EXISTS deaise.grir_stg_del_accounts (
	account_num varchar(20)
);

CREATE TABLE IF NOT EXISTS deaise.grir_stg_del_clients (
	client_id varchar(10)
);

CREATE TABLE IF NOT EXISTS deaise.grir_dwh_fact_transactions (  /* тип данных изменен на timestamp, так как из источника приходят данные формата YYYY-MM-DD HH-MI-SS */
	trans_id varchar(50),
	trans_date timestamp(0),
	card_num varchar(20),
	oper_type varchar(20),
	amt decimal(7, 2),
	oper_result varchar(20),
	terminal varchar(20)
);

CREATE TABLE IF NOT EXISTS deaise.grir_dwh_fact_passport_blacklist (
	passport_num varchar(15),
	entry_dt date
);
