#!/usr/bin/python3

import psycopg2
import pandas as pd
import os
import datetime 
## Создание подключения к PostgreSQL
print('Создание подключений')
conn_src = psycopg2.connect(database = "bank",
                        host =     "de-edu-db.chronosavant.ru",
                        user =     "bank_etl",
                        password = "bank_etl_password",
                        port =     "5432")
                        
conn_dwh = psycopg2.connect(database = "edu",
                        host =     "de-edu-db.chronosavant.ru",
                        user =     "deaise",
                        password = "meriadocbrandybuck",
                        port =     "5432")

## Отключение автокоммита
conn_src.autocommit = False
conn_dwh.autocommit = False

## Создание курсора
cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()

#####################################################################################################################################################################

## Очистка Stage
print('0. Очистка stage')
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_transactions " )
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_terminals " )
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_passport_blacklist " )
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_cards " )
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_accounts " )
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_clients " )
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_del_terminals" )
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_del_cards " )
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_del_accounts " )
cursor_dwh.execute( "DELETE FROM deaise.grir_stg_del_clients " )

#####################################################################################################################################################################

## Загрузка данных в Stage
print('1. Загрузка в stage')

files = [] #список для всех выгруженных файлов за день, 

## Чтение из файлов и заполнение таблиц данными

## grir_stg_transactions
print('1.1. stg_transactions')
file1 = 'transactions_01032021.txt'        #ввод переменной для удобства

df = pd.read_csv(file1, sep=";", header=0)

cursor_dwh.executemany( """ INSERT INTO deaise.grir_stg_transactions (
                                trans_id,
                                trans_date,
                                amt,
                                card_num,
                                oper_type,
                                oper_result,
                                terminal)
                            VALUES( %s, %s, %s, %s, %s, %s, %s )
                        """, df.values.tolist() )

##добавление в список
files.append(file1)

#1.2.terminals
print('1.2.1. terminals - вставка метаданных')

cursor_dwh.execute( """ INSERT INTO deaise.grir_meta_data (
                                schema_name,
                                table_name,
                                max_update_dt)
                        SELECT 
                                'deaise',
                                'grir_stg_terminals',
                                coalesce((select max(update_dt) from deaise.grir_stg_terminals), 
                                                cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp))
                        WHERE   not exists (select 1 from deaise.grir_meta_data where schema_name = 'deaise'
                                and table_name = 'grir_stg_terminals' )
                    """)

## grir_stg_terminals
print('1.2.2. вставка в stg_terminals')

file2 = 'terminals_01032021.xlsx'

dt = file2[14:18] + '-' + file2[12:14] + '-' + file2[10:12]   #переменная для извлечения даты из названия файла

df = pd.read_excel(file2, sheet_name='terminals', header=0, index_col=None)
cursor_dwh.executemany( f""" INSERT INTO deaise.grir_stg_terminals (
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                update_dt)
                            VALUES( trim(%s), trim(%s), trim(%s), trim(%s), cast(to_date('{dt}', 'YYYY-MM-DD') as timestamp) )
                        """, df.values.tolist() )

##добавление в список
files.append(file2)

## grir_stg_passport_blacklist
print('1.3. stg_passport_blacklist')

file3 = 'passport_blacklist_01032021.xlsx'

df = pd.read_excel(file3, sheet_name='blacklist', header=0, index_col=None)
cursor_dwh.executemany( """ INSERT INTO deaise.grir_stg_passport_blacklist (
                                entry_dt,
                                passport_num)
                            VALUES( %s, %s )
                        """, df.values.tolist() )

##добавление в список
files.append(file3)

## Выполнение SQL кода в БД с возвратом результата

## Вставка метаданных
##(cards)
print('1.4.1. cards - вставка метаданных')

cursor_dwh.execute( """ INSERT INTO deaise.grir_meta_data (
                                schema_name,
                                table_name,
                                max_update_dt)
                        SELECT 
                                'deaise',
                                'grir_stg_cards',
                                coalesce((select max(update_dt) from deaise.grir_stg_cards), 
                                                cast(to_date('1900-01-01', 'YYYY-MM-DD') as timestamp )  - interval '1 day' )
                        WHERE   not exists (select 1 from deaise.grir_meta_data where schema_name = 'deaise'
                                and table_name = 'grir_stg_cards' )
                    """)
                    
## stg_cards
print('1.4.2. вставка в grir_stg_cards')

cursor_dwh.execute( """ SELECT max_update_dt
                        FROM deaise.grir_meta_data
                        WHERE schema_name = 'deaise' and table_name = 'grir_stg_cards';
                    """);

update_date = cursor_dwh.fetchone()[0]

cursor_src.execute( f""" SELECT
                            card_num,
                            account,
                            create_dt,
                            update_dt
                        FROM info.cards
                        WHERE coalesce(update_dt, create_dt) >                                                         
                                    coalesce( to_date('{update_date}', 'YYYY-MM-DD'), to_timestamp('1900-01-01', 'YYYY-MM-DD') )
                     """ )

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( """ INSERT INTO deaise.grir_stg_cards(
                                card_num,
                                account_num,
                                create_dt,
                                update_dt )
                            VALUES( %s, %s, %s, %s )
                         """, df.values.tolist() )

##(1.5.1. accounts)
print('1.5.1. accounts - вставка метаданных')

cursor_dwh.execute( """ INSERT INTO deaise.grir_meta_data (
                                schema_name,
                                table_name,
                                max_update_dt)
                        SELECT 
                                'deaise',
                                'grir_stg_accounts',
                                coalesce((select max(update_dt) from deaise.grir_stg_accounts), 
                                                cast( to_date('1900-01-01', 'YYYY-MM-DD') as timestamp )  - interval '1 day' )
                        WHERE   not exists (select 1 from deaise.grir_meta_data where schema_name = 'deaise'
                                and table_name = 'grir_stg_accounts' ) 
                    """)

## stg_accounts
print('1.5.2. вставка в grir_stg_accounts')

cursor_dwh.execute( """ SELECT max_update_dt
                        FROM deaise.grir_meta_data
                        WHERE schema_name = 'deaise' and table_name = 'grir_stg_accounts';
                    """);

update_date = cursor_dwh.fetchone()[0]

cursor_src.execute( f""" SELECT
                            account,
                            valid_to,
                            client,
                            create_dt,
                            update_dt
                        FROM info.accounts 
                        WHERE coalesce(update_dt, create_dt) >                                                         
                                    coalesce( to_date('{update_date}', 'YYYY-MM-DD'), to_timestamp('1900-01-01', 'YYYY-MM-DD') )
                     """ )

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( """ INSERT INTO deaise.grir_stg_accounts(
                                account_num,
                                valid_to,
                                client,
                                create_dt,
                                update_dt )
                           VALUES( %s, %s, %s, %s, %s )
                         """, df.values.tolist() )

 
##(1.6. clients)
print('1.6.1. clients - вставка метаданных')

cursor_dwh.execute( """ INSERT INTO deaise.grir_meta_data (
                                schema_name,
                                table_name,
                                max_update_dt)
                        SELECT 
                                'deaise',
                                'grir_stg_clients',
                                coalesce((select max(update_dt) from deaise.grir_stg_clients), 
                                                to_date('1900-01-01', 'YYYY-MM-DD')  - interval '1 day' )
                        WHERE   not exists (select 1 from deaise.grir_meta_data where schema_name = 'deaise'
                                and table_name = 'grir_stg_clients' )
                    """)

 
## stg_clients
print('1.6.2. вставка в grir_stg_clients')

cursor_dwh.execute( """ SELECT max_update_dt
                        FROM deaise.grir_meta_data
                        WHERE schema_name = 'deaise' and table_name = 'grir_stg_clients';
                    """);

update_date = cursor_dwh.fetchone()[0]

cursor_src.execute( f""" SELECT
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt
                        FROM info.clients
                        WHERE coalesce(update_dt, create_dt) >                                                         
                                    coalesce( to_date('{update_date}', 'YYYY-MM-DD'), to_date('1900-01-01', 'YYYY-MM-DD') )
                     """ )

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( """ INSERT INTO deaise.grir_stg_clients(
                                client_id,
                                last_name,
                                first_name,
                                patronymic,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone,
                                create_dt,
                                update_dt )
                           VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
                         """, df.values.tolist() )
                         

########################################################################################################################################################################

## Захват в стейджинг ключей из источника полным срезом для вычисления удалений
print('2. Наполнение таблиц для удалений ')

##grir_stg_del_terminals
print('2.1. stg_del_terminals')
df = pd.read_excel(file2, sheet_name='terminals', header=0, usecols="A", index_col=None)

cursor_dwh.executemany( """ INSERT INTO deaise.grir_stg_del_terminals (
                                terminal_id)
                            VALUES( %s )
                        """, df.values.tolist() )

##grir_stg_del_cards
print('2.2. stg_del_cards')

cursor_src.execute( """ SELECT
                            card_num
                        FROM info.cards 
                     """ )

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( """ INSERT INTO deaise.grir_stg_del_cards(
                                card_num )
                            VALUES( %s )
                         """, df.values.tolist() )

##grir_stg_del_accounts
print('2.2. stg_del_accounts')

cursor_src.execute( """ SELECT
                            account
                        FROM info.accounts 
                     """ )

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( """ INSERT INTO deaise.grir_stg_del_accounts(
                                account_num )
                            VALUES( %s )
                         """, df.values.tolist() )

##grir_stg_del_clients
print('2.2. stg_del_clients')

cursor_src.execute( """ SELECT
                            client_id
                        FROM info.clients 
                     """ )

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( """ INSERT INTO deaise.grir_stg_del_clients(
                                client_id )
                            VALUES( %s )
                         """, df.values.tolist() )

####################################################################################################################################################################

## Загрузка данных в Детальный слой DDS
print('3. Загрузка в DDS')


## 3.1. Загрузка в приемник "вставок" на источнике
print('3.1. grir_dwh_dim_terminals_hist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_terminals_hist (
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            stg.terminal_id,
                            stg.terminal_type,
                            stg.terminal_city,
                            stg.terminal_address,
                            stg.update_dt effective_from,
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'N' deleted_flg
                        FROM deaise.grir_stg_terminals stg LEFT JOIN
                             deaise.grir_dwh_dim_terminals_hist tgt
                          ON 1=1
                             and stg.terminal_id = tgt.terminal_id
                        WHERE tgt.terminal_id is null 
                    """ )


## 3.2. Загрузка в приемник "вставок" на источнике
print('3.2. grir_dwh_dim_cards_hist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_cards_hist (
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            stg.card_num,
                            stg.account_num,
                            create_dt,
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'N' deleted_flg
                        FROM deaise.grir_stg_cards stg LEFT JOIN
                             deaise.grir_dwh_dim_cards_hist tgt
                          ON 1=1
                             and stg.card_num = tgt.card_num
                        WHERE tgt.card_num is null 
                    """ )

## 3.3. Загрузка в приемник "вставок" на источнике
print('3.3. grir_dwh_dim_accounts_hist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_accounts_hist (
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            stg.account_num,
                            stg.valid_to,
                            stg.client,
                            create_dt,
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'N' deleted_flg
                        FROM deaise.grir_stg_accounts stg LEFT JOIN
                             deaise.grir_dwh_dim_accounts_hist tgt
                          ON 1=1
                             and stg.account_num = tgt.account_num
                        WHERE tgt.account_num is null 
                    """ )


## 3.4. Загрузка в приемник "вставок" на источнике
print('3.4. grir_dwh_dim_clients_hist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_clients_hist (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone,
                            create_dt,
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'N' deleted_flg
                        FROM deaise.grir_stg_clients stg LEFT JOIN
                             deaise.grir_dwh_dim_clients_hist tgt
                          ON 1=1
                             and stg.client_id = tgt.client_id
                        WHERE tgt.client_id is null 
                    """ )

## 3.5. Загрузка в приемник "вставок" на источнике
print('3.5. grir_dwh_fact_transactions вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_fact_transactions (
                            trans_id,
                            trans_date,
                            card_num,
                            oper_type,
                            amt,
                            oper_result,
                            terminal
                        )
                        SELECT
                            stg.trans_id,
                            cast(stg.trans_date as timestamp(0)),
                            stg.card_num,
                            stg.oper_type,
                            cast(replace(stg.amt, ',', '.') as decimal),
                            stg.oper_result,
                            stg.terminal
                        FROM deaise.grir_stg_transactions stg LEFT JOIN
                             deaise.grir_dwh_fact_transactions tgt
                          ON 1=1
                             and stg.trans_id = tgt.trans_id
                        WHERE tgt.trans_id is null 
                    """ )

## 3.6. Загрузка в приемник "вставок" на источнике
print('3.6. grir_dwh_fact_passport_blacklist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_fact_passport_blacklist (
                            passport_num,
                            entry_dt
                        )
                        SELECT
                            stg.passport_num,
                            cast(stg.entry_dt as date)
                        FROM deaise.grir_stg_passport_blacklist stg LEFT JOIN
                             deaise.grir_dwh_fact_passport_blacklist tgt
                          ON 1=1
                             and stg.passport_num = tgt.passport_num
                        WHERE tgt.passport_num is null 
                    """ )

###############################################################################################################################
## Обновление данных в Детальном слое
print('4. Обновление')

## 4.1.1. Обновление в приемнике "обновлений" на источнике
print('4.1.2. grir_dwh_dim_terminals_hist вставка') 

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_terminals_hist (
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            stg.terminal_id,
                            stg.terminal_type,
                            stg.terminal_city,
                            stg.terminal_address,
                            stg.update_dt effective_from,
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'N' deleted_flg
                        FROM deaise.grir_stg_terminals stg INNER JOIN
                             deaise.grir_dwh_dim_terminals_hist tgt
                          ON trim(stg.terminal_id) = trim(tgt.terminal_id)
                             and tgt.effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
                        WHERE (1=0
                            or stg.terminal_city <> tgt.terminal_city
                            or (stg.terminal_city is null and tgt.terminal_city is not null)
                            or (stg.terminal_city is not null and tgt.terminal_city is null)
                            or stg.terminal_address <> tgt.terminal_address
                            or (stg.terminal_address is null and tgt.terminal_address is not null)
                            or (stg.terminal_address is not null and tgt.terminal_address is null)  
                            or stg.terminal_type <> tgt.terminal_type
                            or (stg.terminal_type is null and tgt.terminal_type is not null)
                            or (stg.terminal_type is not null and tgt.terminal_type is null) ) 
                            or tgt.deleted_flg = 'Y' 
                    """ )

## 4.1.2. Обновление в приемнике "обновлений" на источнике
print('4.1.2. grir_dwh_dim_terminals_hist обновление') 

cursor_dwh.execute( """ UPDATE deaise.grir_dwh_dim_terminals_hist tgt
                        SET effective_to = tmp.update_dt - interval '1 day'
                        FROM (
                            SELECT
                                tgt.terminal_id,
                                stg.terminal_type,
                                stg.terminal_city,
                                stg.terminal_address,
                                stg.update_dt
                            FROM deaise.grir_stg_terminals stg INNER JOIN 
                                 deaise.grir_dwh_dim_terminals_hist tgt
                              ON stg.terminal_id = tgt.terminal_id
                                 and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                            WHERE ( 1=0
                                   or stg.terminal_type <> tgt.terminal_type 
                                   or (stg.terminal_type is null and tgt.terminal_type is not null) 
                                   or (stg.terminal_type is not null and tgt.terminal_type is null)
                                   or stg.terminal_city <> tgt.terminal_city 
                                   or (stg.terminal_city is null and tgt.terminal_city is not null) 
                                   or (stg.terminal_city is not null and tgt.terminal_city is null)
                                   or stg.terminal_address <> tgt.terminal_address 
                                   or (stg.terminal_address is null and tgt.terminal_address is not null)
                                   or (stg.terminal_address is not null and tgt.terminal_address is null))
                                   or tgt.deleted_flg = 'Y' ) tmp
                        WHERE tgt.terminal_id = tmp.terminal_id
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                              and (tmp.terminal_type <> tgt.terminal_type 
                              or (tmp.terminal_type is null and tgt.terminal_type is not null) 
                              or (tmp.terminal_type is not null and tgt.terminal_type is null)
                              or tmp.terminal_city <> tgt.terminal_city 
                              or (tmp.terminal_city is null and tgt.terminal_city is not null) 
                              or (tmp.terminal_city is not null and tgt.terminal_city is null)
                              or tmp.terminal_address <> tgt.terminal_address 
                              or (tmp.terminal_address is null and tgt.terminal_address is not null) 
                              or (tmp.terminal_address is not null and tgt.terminal_address is null)
                              or tgt.deleted_flg = 'Y')
                    """ )

## 4.2.1. Обновление в приемнике "обновлений" на источнике
print('4.2.1. grir_dwh_dim_cards_hist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_cards_hist (
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            stg.card_num,
                            stg.account_num,
                            stg.update_dt,
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'N' deleted_flg
                        FROM deaise.grir_stg_cards stg INNER JOIN
                             deaise.grir_dwh_dim_cards_hist tgt
                          ON stg.card_num = tgt.card_num
                             and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                        WHERE (1=0
                              or stg.account_num <> tgt.account_num
                              or (stg.account_num is null and tgt.account_num is not null)
                              or (stg.account_num is not null and tgt.account_num is null) )
                              or tgt.deleted_flg = 'Y'
                     """ )

## 4.2.2. Обновление в приемнике "обновлений" на источнике
print('4.2.2. grir_dwh_dim_cards_hist обновление')

cursor_dwh.execute( """ UPDATE deaise.grir_dwh_dim_cards_hist tgt
                        SET effective_to = tmp.update_dt - interval '1 day'
                        FROM (
                                SELECT
                                    stg.card_num,
                                    stg.account_num,
                                    stg.update_dt
                                FROM deaise.grir_stg_cards stg INNER JOIN
                                     deaise.grir_dwh_dim_cards_hist tgt
                                  ON stg.card_num = tgt.card_num
                                     and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                                WHERE (1=0
                                     or stg.account_num <> tgt.account_num
                                     or (stg.account_num is null and tgt.account_num is not null)
                                     or (stg.account_num is not null and tgt.account_num is null) )
                                     or tgt.deleted_flg = 'Y' ) tmp
                        WHERE  tgt.card_num = tmp.card_num
                               and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                               and (tmp.account_num <> tgt.account_num
                                or (tmp.account_num is null and tgt.account_num is not null)
                                or (tmp.account_num is not null and tgt.account_num is null) )
                                or tgt.deleted_flg = 'Y'
                     """ )

## 4.3.1. Обновление в приемнике "обновлений" на источнике
print('4.3.1. grir_dwh_dim_accounts_hist вставка' )

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_accounts_hist (
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            stg.account_num,
                            stg.valid_to,
                            stg.client,
                            stg.update_dt,
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'N' deleted_flg
                        FROM deaise.grir_stg_accounts stg INNER JOIN
                             deaise.grir_dwh_dim_accounts_hist tgt
                          ON stg.account_num = tgt.account_num
                             and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                        WHERE (1=0
                              or stg.valid_to <> tgt.valid_to
                              or (stg.valid_to is null and tgt.valid_to is not null)
                              or (stg.valid_to is not null and tgt.valid_to is null)
                              or stg.client <> tgt.client
                              or (stg.client is null and tgt.client is not null)
                              or (stg.client is not null and tgt.client is null) )
                              or tgt.deleted_flg = 'Y'
                    """ )

## 4.3.2. Обновление в приемнике "обновлений" на источнике
print('4.3.2. grir_dwh_dim_accounts_hist обновление' )

cursor_dwh.execute( """ UPDATE deaise.grir_dwh_dim_accounts_hist tgt
                        SET effective_to = tmp.update_dt - interval '1 day'
                        FROM (
                                SELECT
                                    stg.account_num,
                                    stg.valid_to,
                                    stg.client,
                                    stg.update_dt
                                FROM deaise.grir_stg_accounts stg INNER JOIN
                                     deaise.grir_dwh_dim_accounts_hist tgt
                                  ON stg.account_num = tgt.account_num
                                     and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                                WHERE (1=0
                                     or stg.valid_to <> tgt.valid_to
                                     or (stg.valid_to is null and tgt.valid_to is not null)
                                     or (stg.valid_to is not null and tgt.valid_to is null) 
                                     or stg.client <> tgt.client
                                     or (stg.client is null and tgt.client is not null)
                                     or (stg.client is not null and tgt.client is null) )
                                     or tgt.deleted_flg = 'Y' ) tmp
                        WHERE  tgt.account_num = tmp.account_num
                               and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                               and (tmp.valid_to <> tgt.valid_to
                               or (tmp.valid_to is null and tgt.valid_to is not null)
                               or (tmp.valid_to is not null and tgt.valid_to is null)
                               or tmp.client <> tgt.client
                               or (tmp.client is null and tgt.client is not null)
                               or (tmp.client is not null and tgt.client is null)
                               or tgt.deleted_flg = 'Y') 
                     """ )

## 4.4.1. Обновление в приемнике "обновлений" на источнике
print('4.4.1. grir_dwh_dim_clients_hist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_clients_hist (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone,
                            stg.update_dt,
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'N' deleted_flg
                        FROM deaise.grir_stg_clients stg INNER JOIN
                             deaise.grir_dwh_dim_clients_hist tgt
                          ON  stg.client_id = tgt.client_id
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                        WHERE (1=0
                              or stg.last_name <> tgt.last_name
                              or (stg.last_name is null and tgt.last_name is not null)
                              or (stg.last_name is not null and tgt.last_name is null)
                              or stg.first_name <> tgt.first_name
                              or (stg.first_name is null and tgt.first_name is not null)
                              or (stg.first_name is not null and tgt.first_name is null)
                              or stg.patronymic <> tgt.patronymic
                              or (stg.patronymic is null and tgt.patronymic is not null)
                              or (stg.patronymic is not null and tgt.patronymic is null)
                              or stg.passport_num <> tgt.passport_num
                              or (stg.passport_num is null and tgt.passport_num is not null)
                              or (stg.passport_num is not null and tgt.passport_num is null)
                              or stg.passport_valid_to <> tgt.passport_valid_to
                              or (stg.passport_valid_to is null and tgt.passport_valid_to is not null)
                              or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
                              or stg.phone <> tgt.phone
                              or (stg.phone is null and tgt.phone is not null)
                              or (stg.phone is not null and tgt.phone is null) )
                              or tgt.deleted_flg = 'Y' 
                    """ )

## 4.4.2. Обновление в приемнике "обновлений" на источнике
print('4.4.2. grir_dwh_dim_clients_hist обновление')

cursor_dwh.execute( """ UPDATE deaise.grir_dwh_dim_clients_hist tgt
                        SET effective_to = tmp.update_dt - interval '1 day'
                        FROM (
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
                                FROM deaise.grir_stg_clients stg INNER JOIN
                                     deaise.grir_dwh_dim_clients_hist tgt
                                  ON stg.client_id = tgt.client_id
                                     and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                                WHERE (1=0
                                      or stg.last_name <> tgt.last_name
                                      or (stg.last_name is null and tgt.last_name is not null)
                                      or (stg.last_name is not null and tgt.last_name is null) 
                                      or stg.first_name <> tgt.first_name
                                      or (stg.first_name is null and tgt.first_name is not null)
                                      or (stg.first_name is not null and tgt.first_name is null) 
                                      or stg.patronymic <> tgt.patronymic
                                      or (stg.patronymic is null and tgt.patronymic is not null)
                                      or (stg.patronymic is not null and tgt.patronymic is null) 
                                      or stg.passport_num <> tgt.passport_num
                                      or (stg.passport_num is null and tgt.passport_num is not null)
                                      or (stg.passport_num is not null and tgt.passport_num is null) 
                                      or stg.passport_valid_to <> tgt.passport_valid_to
                                      or (stg.passport_valid_to is null and tgt.passport_valid_to is not null)
                                      or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
                                      or stg.phone <> tgt.phone
                                      or (stg.phone is null and tgt.phone is not null)
                                      or (stg.phone is not null and tgt.phone is null) )
                                      or tgt.deleted_flg = 'Y'  ) tmp
                        WHERE  tgt.client_id = tmp.client_id
                               and tgt.effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
                               and (tmp.last_name <> tgt.last_name
                               or (tmp.last_name is null and tgt.last_name is not null)
                               or (tmp.last_name is not null and tgt.last_name is null)
                               or tmp.first_name <> tgt.first_name
                               or (tmp.first_name is null and tgt.first_name is not null)
                               or (tmp.first_name is not null and tgt.first_name is null)
                               or tmp.patronymic <> tgt.patronymic
                               or (tmp.patronymic is null and tgt.patronymic is not null)
                               or (tmp.patronymic is not null and tgt.patronymic is null)
                               or tmp.passport_num <> tgt.passport_num
                               or (tmp.passport_num is null and tgt.passport_num is not null)
                               or (tmp.passport_num is not null and tgt.passport_num is null)
                               or tmp.passport_valid_to <> tgt.passport_valid_to
                               or (tmp.passport_valid_to is null and tgt.passport_valid_to is not null)
                               or (tmp.passport_valid_to is not null and tgt.passport_valid_to is null)
                               or tmp.phone <> tgt.phone
                               or (tmp.phone is null and tgt.phone is not null)
                               or (tmp.phone is not null and tgt.phone is null)
                               or tgt.deleted_flg = 'Y') 
                    """ )


###############################################################################################################################
## Обработка удаления данных в Детальном слое
print('5. Удаление данных')

## 5.1.1.Удаление данных (вставка)
print('5.1.1. grir_dwh_dim_terminals_hist вставка') 

cursor_dwh.execute( f""" INSERT INTO deaise.grir_dwh_dim_terminals_hist (
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            tgt.terminal_id,
                            tgt.terminal_type,
                            tgt.terminal_city,
                            tgt.terminal_address,
                            cast(to_date('{dt}', 'YYYY-MM-DD') as timestamp) effective_from,
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'Y' deleted_flg
                        FROM deaise.grir_dwh_dim_terminals_hist tgt LEFT JOIN
                             deaise.grir_stg_del_terminals stgd
                          ON stgd.terminal_id = tgt.terminal_id
                        WHERE stgd.terminal_id is null
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                              and tgt.deleted_flg = 'N' 
                    """ )

## 5.1.2.Удаление данных (обновление)
print('5.1.2. grir_dwh_dim_terminals_hist вставка') 

cursor_dwh.execute( f""" UPDATE deaise.grir_dwh_dim_terminals_hist tgt
                        SET effective_to = cast(to_date('{dt}', 'YYYY-MM-DD') as timestamp) - interval '1 day'
                        WHERE tgt.terminal_id in (
                                                    SELECT
                                                            tgt.terminal_id
                                                    FROM deaise.grir_dwh_dim_terminals_hist tgt LEFT JOIN
                                                         deaise.grir_stg_del_terminals stgd
                                                      ON stgd.terminal_id = tgt.terminal_id
                                                    WHERE stgd.terminal_id is null
                                                          and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                                                          and deleted_flg = 'N' )
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                              and deleted_flg = 'N'
                    """ )


## 5.2.1.Удаление данных (вставка)
print('5.2.1. grir_dwh_dim_cards_hist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_cards_hist (
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            tgt.card_num,
                            tgt.account_num,
                            now(),
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'Y' deleted_flg
                        FROM deaise.grir_dwh_dim_cards_hist tgt LEFT JOIN
                             deaise.grir_stg_del_cards stgd
                          ON stgd.card_num = tgt.card_num                             
                        WHERE stgd.card_num is null
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                              and tgt.deleted_flg = 'N'
                     """ )
                     
## 5.2.2.Удаление данных (обновление)
print('5.2.2. grir_dwh_dim_cards_hist обновление') 

cursor_dwh.execute( """ UPDATE deaise.grir_dwh_dim_cards_hist tgt
                        SET effective_to = now() - interval '1 day'
                        WHERE tgt.card_num in (
                                                    SELECT
                                                            tgt.card_num
                                                    FROM deaise.grir_dwh_dim_cards_hist tgt LEFT JOIN
                                                         deaise.grir_stg_del_cards stgd
                                                      ON stgd.card_num = tgt.card_num
                                                    WHERE stgd.card_num is null
                                                          and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                                                          and deleted_flg = 'N' )
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                              and deleted_flg = 'N'
                    """ )

## 5.3.1.Удаление данных (вставка)
print('5.3.1. grir_dwh_dim_accounts_hist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_accounts_hist (
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            tgt.account_num,
                            tgt.valid_to,
                            tgt.client,
                            now(),
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'Y' deleted_flg
                        FROM deaise.grir_dwh_dim_accounts_hist tgt LEFT JOIN
                             deaise.grir_stg_del_accounts stgd
                          ON stgd.account_num = tgt.account_num                             
                        WHERE stgd.account_num is null
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                              and tgt.deleted_flg = 'N'
                     """ )
                     
## 5.3.2.Удаление данных (обновление)
print('5.3.2. grir_dwh_dim_accounts_hist обновление') 

cursor_dwh.execute( """ UPDATE deaise.grir_dwh_dim_accounts_hist tgt
                        SET effective_to = now() - interval '1 day'
                        WHERE tgt.account_num in (
                                                    SELECT
                                                            tgt.account_num
                                                    FROM deaise.grir_dwh_dim_accounts_hist tgt LEFT JOIN
                                                         deaise.grir_stg_del_accounts stgd
                                                      ON stgd.account_num = tgt.account_num
                                                    WHERE stgd.account_num is null
                                                          and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                                                          and deleted_flg = 'N' )
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                              and deleted_flg = 'N'
                    """ )


## 5.4.1.Удаление данных (вставка)
print('5.4.1. grir_dwh_dim_clients_hist вставка')

cursor_dwh.execute( """ INSERT INTO deaise.grir_dwh_dim_clients_hist (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        SELECT
                            tgt.client_id,
                            tgt.last_name,
                            tgt.first_name,
                            tgt.patronymic,
                            date_of_birth,
                            tgt.passport_num,
                            tgt.passport_valid_to,
                            tgt.phone,
                            now(),
                            cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp) effective_to,
                            'Y' deleted_flg
                        FROM deaise.grir_dwh_dim_clients_hist tgt LEFT JOIN
                             deaise.grir_stg_del_clients stgd
                          ON stgd.client_id = tgt.client_id                             
                        WHERE stgd.client_id is null
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                              and tgt.deleted_flg = 'N'
                     """ )
                     
## 5.4.2.Удаление данных (обновление)
print('5.4.2. grir_dwh_dim_clients_hist обновление') 

cursor_dwh.execute( """ UPDATE deaise.grir_dwh_dim_clients_hist tgt
                        SET effective_to = now() - interval '1 day'
                        WHERE tgt.client_id in (
                                                    SELECT
                                                            tgt.client_id
                                                    FROM deaise.grir_dwh_dim_clients_hist tgt LEFT JOIN
                                                         deaise.grir_stg_del_clients stgd
                                                      ON stgd.client_id = tgt.client_id
                                                    WHERE stgd.client_id is null
                                                          and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                                                          and deleted_flg = 'N' )
                              and tgt.effective_to = cast(to_date('9999-12-31', 'YYYY-MM-DD') as timestamp)
                              and deleted_flg = 'N'
                    """ )

###############################################################################################################################################
## 6.Обновление метаданных.

# ##(6.1. accounts)

print('6.1.2. accounts - обновление метаданных')
cursor_dwh.execute( """ UPDATE deaise.grir_meta_data
                        SET max_update_dt = coalesce((SELECT coalesce(max(update_dt), max(create_dt)) 
                                                      FROM deaise.grir_stg_accounts), max_update_dt)
                        WHERE schema_name = 'deaise'
                              and table_name = 'grir_stg_accounts'
                   """)

##(6.2. cards)

print('6.2.2. cards - обновление метаданных')
cursor_dwh.execute( """ UPDATE deaise.grir_meta_data
                        SET max_update_dt = coalesce((SELECT coalesce(max(update_dt), max(create_dt)) 
                                                      FROM deaise.grir_stg_cards), max_update_dt)
                        WHERE schema_name = 'deaise'
                              and table_name = 'grir_stg_cards'
                   """)

##(6.3. clients)

print('6.3.2. clients - обновление метаданных')
cursor_dwh.execute( """ UPDATE deaise.grir_meta_data
                        SET max_update_dt = coalesce((SELECT coalesce(max(update_dt), max(create_dt)) 
                                                      FROM deaise.grir_stg_clients), max_update_dt)
                        WHERE schema_name = 'deaise'
                              and table_name = 'grir_stg_clients'
                   """)                  

##(6.4. terminals)
# print('6.4.1. terminals - вставка метаданных')
                    
print('6.4.2 terminals - обновление метаданных')                    
cursor_dwh.execute( f""" UPDATE deaise.grir_meta_data
                        SET max_update_dt = coalesce((SELECT coalesce(max(update_dt), max(to_date('{dt}', 'YYYY-MM-DD'))) 
                                                      FROM deaise.grir_stg_terminals), max_update_dt)
                        WHERE schema_name = 'deaise'
                              and table_name = 'grir_stg_terminals'
                   """)

conn_dwh.commit()
######################################################################################################################################################

## 7.Построение витрины данных
print('7.Построение витрины данных')

cursor_dwh.execute( """ WITH tab AS (
                        SELECT 
                                tr.trans_id,
                                tr.trans_date,
                                coalesce (lag(tr.trans_date) over(partition by cl.client_id  order by trans_date ), null) last_trans_date,
                                tr.oper_type,
                                tr.amt,
                                tr.oper_result,
                                tm.terminal_city,
                                coalesce (lag(tm.terminal_city) over(partition by cl.client_id  order by trans_date ), null) last_city, 
                                ac.valid_to,
                                cl.passport_num,
                                cl.passport_valid_to,
                                cl.last_name,
                                cl.first_name,
                                cl.patronymic,
                                cl.phone
                        FROM grir_dwh_fact_transactions tr LEFT JOIN
                            grir_dwh_dim_cards_hist c 
                        ON trim(tr.card_num) = trim(c.card_num)  LEFT JOIN 
                            grir_dwh_dim_accounts_hist ac
                        ON trim(ac.account_num) = trim(c.account_num) LEFT JOIN
                            grir_dwh_dim_clients_hist cl
                        ON trim(ac.client) = trim(cl.client_id) LEFT JOIN 
                            grir_dwh_dim_terminals_hist tm
                        ON tr.terminal = tm.terminal_id                          
                        ),                   
                        tb AS (
                            SELECT 
                                    tr.trans_id,
                                    tr.trans_date,
                                    coalesce (lag(tr.trans_date) over(partition by cl.client_id,c.card_num, tr.oper_type  order by trans_date ), null) last_trans_date,
                                    coalesce (lag(tr.trans_date, 2) over(partition by cl.client_id, c.card_num, tr.oper_type  order by trans_date ), null) last_last_trans_date,
                                    coalesce (lag(tr.trans_date, 3) over(partition by cl.client_id,c.card_num, tr.oper_type  order by trans_date ), null) last_last_last_trans_date,
                                    tr.oper_type,
                                    coalesce (lag(tr.oper_type) over(partition by cl.client_id,c.card_num, tr.oper_type  order by trans_date ), null) last_oper_type,
                                    coalesce (lag(tr.oper_type, 2) over(partition by cl.client_id,c.card_num, tr.oper_type  order by trans_date ), null) last_last_oper_type,
                                    coalesce (lag(tr.oper_type, 3) over(partition by cl.client_id,c.card_num, tr.oper_type  order by trans_date ), null) last_last_last_oper_type,
                                    tr.amt,
                                    coalesce (lag(tr.amt) over(partition by cl.client_id,c.card_num, tr.oper_type order by trans_date ), null) last_amt,
                                    coalesce (lag(tr.amt, 2) over(partition by cl.client_id,c.card_num, tr.oper_type order by trans_date ), null) last_last_amt,
                                    coalesce (lag(tr.amt, 3) over(partition by cl.client_id,c.card_num, tr.oper_type order by trans_date ), null) last_last_last_amt,
                                    tr.oper_result,
                                    coalesce (lag(tr.oper_result) over(partition by cl.client_id,c.card_num, tr.oper_type order by trans_date ), null) last_oper_result,
                                    coalesce (lag(tr.oper_result, 2) over(partition by cl.client_id,c.card_num, tr.oper_type order by trans_date ), null) last_last_oper_result,
                                    coalesce (lag(tr.oper_result, 3) over(partition by cl.client_id, c.card_num,tr.oper_type order by trans_date ), null) last_last_last_oper_result,
                                    cl.passport_num,
                                    cl.last_name,
                                    cl.first_name,
                                    cl.patronymic,
                                    cl.phone,
                                    cl.passport_valid_to,
                                    ac.valid_to
                            FROM grir_dwh_fact_transactions tr LEFT JOIN 
                                 grir_dwh_dim_cards_hist c 
                            ON trim(tr.card_num) = trim(c.card_num)  LEFT JOIN 
                                grir_dwh_dim_accounts_hist ac
                            ON trim(ac.account_num) = trim(c.account_num) LEFT JOIN
                                grir_dwh_dim_clients_hist cl
                            ON trim(ac.client) = trim(cl.client_id) LEFT JOIN 
                                grir_dwh_dim_terminals_hist tm
                            ON tr.terminal = tm.terminal_id
                        ) 
                        INSERT INTO deaise.grir_rep_fraud (
                            event_dt,
                            passport,
                            fio,
                            phone,
                            event_type,
                            report_dt )
                        SELECT 
                            stg.event_dt,
                            stg.passport_num passport,
                            stg.fio,
                            stg.phone,
                            stg.event_type,
                            stg.report_dt
                        FROM (
                            SELECT 
                                trans_date event_dt,
                                passport_num,
                                last_name || ' ' || first_name || ' ' || patronymic fio,
                                phone,
                                CASE 
                                WHEN passport_valid_to < trans_date or passport_num in (SELECT passport_num FROM deaise.grir_dwh_fact_passport_blacklist) THEN '1'
                                WHEN valid_to < trans_date THEN '2'
                                WHEN trans_id in ( SELECT 
                                                trans_id 
                                           FROM tab 
                                           WHERE (last_city is not null) 
                                                 and (terminal_city <> last_city)
                                                 and (trans_date - last_trans_date < interval '1 hour') ) THEN '3'
                                WHEN trans_id in ( SELECT 
                                                trans_id 
                                           FROM tb 
                                           WHERE (last_last_last_amt > last_last_amt and last_last_amt > last_amt and last_amt > amt )
                                                and   (last_last_last_oper_result = 'REJECT' and last_last_oper_result = 'REJECT' and last_oper_result = 'REJECT' and oper_result = 'SUCCESS')
                                                and   (trans_date - last_last_last_trans_date <= interval '20 minutes')
                                                and   (oper_type = 'PAYMENT' or oper_type = 'WITHDRAW') ) THEN '4'
                                                END event_type,
                                current_date report_dt
                            FROM tab ) stg LEFT JOIN 
                                deaise.grir_rep_fraud tgt
                             ON 1=1
                             and stg.passport_num = tgt.passport
                             and stg.report_dt = tgt.report_dt
                             and stg.event_dt = tgt.event_dt
                            WHERE tgt.passport is null 
                                  and stg.event_type in ('1', '2', '3', '4')
                """)

######################################################################################################################################################################

## 8.Перенос файлов в архив и переименование 
print('8.Перенос файлов в архив и переименование')
for file in files:
    os.rename(file, "archive/" +  file + ".backup")
    
######################################################################################################################################################

## 9.Сохранение изменений
print('9.Сохранение изменений')
conn_dwh.commit()

## 10.Закрываем соединение
print('10.Закрытие подключений')
cursor_src.close()
cursor_dwh.close()
conn_src.close()
conn_dwh.close()
