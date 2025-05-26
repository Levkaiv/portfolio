import pandas as pd
import asyncio
import aiohttp
from datetime import date
import requests
import sys

import os
from dotenv import load_dotenv
import paramiko
from clickhouse_driver import Client
import psycopg2

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from data_input import worksheet1
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.float_format', lambda x: '{:,.2f}'.format(x))


###############################################################################################################


load_dotenv()

username = os.getenv("SSH_NAME")
key_filename = os.getenv("KEY_FILENAME")
hostname = os.getenv("SSH_HOST")
database_ch = os.getenv("DATABASE_NAME_CH")
database_pg = os.getenv("DATABASE_NAME_PG")
user = os.getenv("DATABASE_USER")
password_ch = os.getenv("DATABASE_PASSWORD_CH")
password_pg = os.getenv("DATABASE_PASSWORD_PG")

local_hostname = os.getenv("DB_HOST")
local_port = os.getenv("DB_PORT")
local_user = os.getenv("DB_USER")
local_password = os.getenv("DB_PASS")
local_database = os.getenv("DB_NAME")


# SSH туннель
def connect_to_ssh(hostname, username, key_filename):

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(hostname, username=username, key_filename=key_filename)
        print(f"Успешное подключение к БД как {username}")
        return ssh

    except Exception as e:
        print(f"Ошибка подключения: {e}")
        sys.exit(1)
        return None

# PostgreSQL
def execute_pg_query(ssh_client, database, password, query):

    try:
       
        conn = psycopg2.connect(
        host=hostname,
        port=5432,
        user=user,
        password=password,
        database=database
    )


        cursor = conn.cursor()
        cursor.execute(query)
        ads_2024 = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        return ads_2024

    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None
    
# ClickHouse
def execute_ch_query_wb(ssh_client, database, password, query):
    try:

        client = Client(host=hostname,
                user=user,
                password=password,
                port=9000,
                database=database)

        remains = client.execute(query)
        remains = pd.DataFrame(remains, columns=['date','SKU','subject','organization_id',\
                        'warehouse_name','quantity','in_way_from_client','in_way_to_client','quantity_full','ostatki'])
        return remains

    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None
    
# ClickHouse 2 (остатки Озона)
def execute_ch_query_oz(ssh_client, database, password, query):
    try:

        client = Client(host=hostname,
                user=user,
                password=password,
                port=9000,
                database=database)

        remains = client.execute(query)
        remains = pd.DataFrame(remains, columns=['Дата','organization_id','Артикул','remains'])
        return remains

    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None



def extract_data(date_from, date_from_q, date_to, date_to_q, date_from_str, date_to_str, diff_date):


    # Запросы
    query_ads = f'''
    select 
        date,   
        st.external_id,
        case 
            when adid.organization_id = 12
                then 'Организация 1'
            when adid.organization_id = 13
                then 'Организация 2'
            when adid.organization_id = 14
                then 'Организация 3'
            when adid.organization_id = 16
                then 'Организация 4'
            when adid.organization_id = 49
                then 'Организация 5'
            else '0'
        end as organization_id,
        st.article as nm_id,
        sum(st.stat_sum) as internal_ads_sum,
        sum(st.clicks) as clicks_sum,
        sum(st.views) as views_sum
    from an_wb_advert_article_stats st
    left join an_wb_advert_ids as adid on st.external_id=adid.external_id 
    left join an_wb_advert_info as inf on st.external_id=inf.external_id
    where 1=1
        and date >= '{date_from_q}'
    group by date, 
            adid.organization_id,
            st.external_id,
            st.article
    order by date desc
    '''

    query_remains = f'''
    --Остатки:

    WITH orders as (SELECT sum(quantity) q,
            sum(quantity_full) qf,
            sum(in_way_from_client) wfc,
            sum(in_way_to_client) wtc,
            toDate(created_at) AS date,
            nm_id,
            barcode,
            warehouse_name,
            subject, 
            case 
                    when organization_id=12 then 'Организация 1'
                    when organization_id=13 then 'Организация 2'
                    when organization_id=14 then 'Организация 3'
                    when organization_id=16 then 'Организация 4'
                    when organization_id=49 then 'Организация 5'
                    else null end as organization_id
        FROM mp_product_quantity o
        INNER JOIN
        (SELECT max(created_at) AS date,
                nm_id
        FROM mp_product_quantity
        WHERE toDate(created_at) >= '{date_from_q}'
        GROUP BY toDate(created_at),
                    nm_id) m ON o.nm_id = m.nm_id
        AND toHour(o.created_at) = toHour(m.date)
        AND toDate(o.created_at) = toDate(m.date)
        WHERE date_diff('minute', o.created_at, m.date) < 5
            and warehouse_name != 'Санкт-Петербург Шушары'
        GROUP BY date, nm_id,
                    barcode,
                    warehouse_name,
                    subject,
                    organization_id
        ORDER BY nm_id, date DESC)

    select date,
            nm_id as SKU
            subject,
            organization_id,
            warehouse_name,
            sum(q) as quantity,
            sum(wfc) as in_way_from_client,
            sum(wtc) as in_way_to_client,
            sum(qf) as quantity_full,
            sum(q + wfc + wtc*0.7) as ostatki
    from orders
    group by date,
            SKU,
            subject,
            organization_id,
            warehouse_name
    order by date asc;
    '''

    query_supplier_arts = '''
    select 
        mc."name" as supplier_name,
        sma.marketplace_article as SKU,
        makm."name" as marletplace
    from st_market_article sma
    full outer join mc_supplier_product msp on sma.product_id=msp.product_id
    left join mp_counterparty mc on msp.counterparty_id=mc.id
    left join mp_api_key_marketplace makm on sma.marketplace_id=makm.id
    where msp.supplier_article is  not null
    group by
        mc."name", 
        sma.marketplace_article, 
        makm."name"
    '''

    query_oz_remains = f'''
    select 
            DATE(created_at) as "Дата",
            organization_id,
            item_code as "Артикул",
            sum(promised_amount + free_to_sell_amount + reserved_amount) as remains
        from mp_ozon_product_remains
        where toDate(created_at) >= '{date_from_q}'
        group by  
                organization_id,
                item_code,
                DATE(created_at)

    '''


    # Подключение к SSH-серверу
    ssh_client = connect_to_ssh(hostname, username, key_filename)

    if ssh_client: 

        ads_2024 = execute_pg_query(ssh_client, database_pg, password_pg, query_ads)
        remains_2024 = execute_ch_query_wb(ssh_client, database_ch, password_ch, query_remains)
        # api_keys = execute_pg_query(ssh_client, database_pg, password_pg, query_api_key)
        supplier_arts = execute_pg_query(ssh_client, database_pg, password_pg, query_supplier_arts)
        supplier_arts = supplier_arts.rename(columns={'sku':'SKU'})
        supplier_arts['SKU'] = supplier_arts['SKU'].astype(str).str.strip()

        oz_remains = execute_ch_query_oz(ssh_client, database_ch, password_ch, query_oz_remains)


        # Закрытие соединения
        ssh_client.close()
        print('Соединение закрыто')

    local_url = f'postgresql://{local_user}:{local_password}@{local_hostname}:{local_port}/{local_database}'

    engine = create_engine(
        url=local_url
    )

    Session = sessionmaker(engine)
    session_fac = Session()


    with session_fac as session:
        query = f'''
        select 
    organization_id,
	sku,
	sum(sales_spp) as sales_spp,
	sum(return_spp) as return_spp,
	sum(sales_spp) as sales,
	sum(return_spp) as return,
	sum((sales_spp-return_spp)) as revenue_spp,
	sum(sales_op) as sales_op,
	sum(return_op) as return_op,
	sum((sales_op-return_op)) as revenue_op,
	sum(sales_count) as sales_count,
	sum(return_count) as return_count,
	sum((sales_count-return_count)) as sales_with_return,
	sum(logistics_full) as logistics_full,
	sum(log_to_wb) as log_to_wb,
	sum(log_to_seller) as log_to_seller,
	sum(log_to_wb_count) as log_to_wb_count,
	sum(log_to_seller_count) as log_to_seller_count,
	sum(log_to_client_sales) as log_to_client_sales,
	sum(log_to_client_cancel) as log_to_client_cancel,
	sum(log_from_client_return) as log_from_client_return,
	sum(log_from_client_cancel) as log_from_client_cancel,
	sum((logistics_full-log_to_wb-log_to_seller)) as log_sales,
	sum(((sales_op-return_op)-(sales_spp-return_spp))) as spp,
	sum((log_to_client_sales+log_to_client_cancel)) as orders,
	sum(penalty) as penalty,
	sum(wb_compensations) as wb_compensations,
	sum(comission) as comission
from detail_fin_for_abc
where date_sales >= '{date_from}' 
	and date_sales <= '{date_to}'
	--and sku != 0
group by
	organization_id,
	sku
        '''

        result = session.execute(text(query))

        df = pd.DataFrame(result)


        detal_fin1_for_ABC = df.rename(columns={
            'sku':'SKU',
            'organization_id':'ИП',
            'spp':'Скидка ВБ',
            'comission':'Реальная комиссия',
            'logistic_full':'Логистика полная',
            'log_to_seller':'Логистика до продавца',
            'log_to_wb':'Логистика до склада МП',
            'log_sales':'Логистика продаж',
            'penalty':'Штрафы ВБ'})
        
        detal_fin1_for_ABC['sales_count'] = detal_fin1_for_ABC['sales_count'].astype(float)
        detal_fin1_for_ABC['return_count'] = detal_fin1_for_ABC['return_count'].astype(float)
        detal_fin1_for_ABC['sales_with_return'] = detal_fin1_for_ABC['sales_with_return'].astype(float)

        query_api_keys = '''
        select marketplace, organization_id as organization, client_id, api_key 
        from api_keys ak 
        '''
        result = session.execute(text(query_api_keys))
        api_keys = pd.DataFrame(result)
    



    print('############---Выгрузка справочника артикулов WB---##################')

    df_ip1 = []
    df_ip2 = []
    for api in range(len(api_keys[api_keys['marketplace']=='Wildberries']['organization'])):
        api_key = api_keys[api_keys['marketplace']=='Wildberries']['api_key'].values[api]
        organization = api_keys[api_keys['marketplace']=='Wildberries']['organization'].values[api]
        print(organization)
        # Первичный запрос на получение списка товаров (100 шт)
        url = 'https://content-api.wildberries.ru/content/v2/get/cards/list'
        headers = {
                'Authоrization':api_key,
                'Content-Type':'application/json'
            }
        params = {
            'locale':'ru'
            }
        payload = {
                    "settings": {
                    "cursor": {
                        "limit": 100
                    },
                    "filter": {
                        "withPhoto": -1
                    }
                    }
                }
        print('Запрос 1')
        res = requests.post(url=url, headers=headers, params=params, json=payload)
        print(res)
        data = res.json()
        sku = []
        arts = []
        subject = []
        brand = []
        title = []
        meneger = []
        created_at = []
        updated_at = []
        for n in range(len(data['cards'])):
            nmID = data['cards'][n]['nmID']
            sku.append(nmID)
            vendorCode = data['cards'][n]['vendorCode']
            arts.append(vendorCode)
            subjectName = data['cards'][n]['subjectName']
            subject.append(subjectName)
            brand_ = data['cаrds'][n]['brand']
            brand.append(brand_)
            title_ = data['cards'][n]['title']
            title.append(title_)
            if 'tags' in data['cards'][n] and len(data['cards'][n]['tags']) > 0:
                name = data['cards'][n]['tags'][0]['name']
                meneger.append(name)
            else:
                meneger.append("нет менеджера")
            createdAt = data['cards'][n]['createdAt']
            created_at.append(createdAt)
            updatedAt = data['cards'][n]['updatedAt']
            updated_at.append(updatedAt)
        df1 = pd.DataFrame({
            'organization':organization,
            'sku':sku,
            'arts':arts,
            "subject":subject,
            "brand":brand,
            "title":title,
            "meneger":meneger,
            "created_at":created_at,
            "updated_at":updated_at
        })
        # Повторные запросы на получение товаров
        total = data['cursor']['total']
        limit = 100
        updatedAt = data['cursor']['updatedAt']
        nmID = data['cursor']['nmID']
        df2_f = []
        print('Запрос 2')
        N = 1

        while total >= limit or limit == 0:
            payload_tl = {
                        "settings": {
                        "cursor": {
                            "limit": limit,
                            'updatedAt':updatedAt,
                            'nmID':nmID
                        },
                        "filter": {
                            "withPhoto": -1
                        }
                        }
                    }
            print(f'Запрос 2-{N}   ("limit":{limit}, "updatedAt":{updatedAt}, "nmID":{nmID})')
            res_tl = requests.post(url=url, headers=headers, params=params, json=payload_tl)
            print(res_tl)
            print(res_tl.json()['cursor'])
            data_tl = res_tl.json()
            sku = []
            arts = []
            subject = []
            brand = []
            title = []
            meneger = []
            created_at = []
            updated_at = []
            for n in range(len(data_tl['cards'])):
                nmID = data_tl['cards'][n]['nmID']
                sku.append(nmID)
                vendorCode = data_tl['cards'][n]['vendorCode']
                arts.append(vendorCode)
                subjectName = data_tl['cards'][n]['subjectName']
                subject.append(subjectName)
                brand_ = data_tl['cards'][n]['brand']
                brand.append(brand_)
                title_ = data_tl['cards'][n]['title']
                title.append(title_)
                # Проверка на наличие ключа "tags"
                if 'tags' in data_tl['cards'][n] and len(data_tl['cards'][n]['tags']) > 0:
                    name = data_tl['cards'][n]['tags'][0]['name']
                    meneger.append(name)
                else:
                    meneger.append("нет менеджера")
                createdAt = data_tl['cards'][n]['createdAt']
                created_at.append(createdAt)
                updatedAt = data_tl['cards'][n]['updatedAt']
                updated_at.append(updatedAt)
            df_tl = pd.DataFrame({
                'organization':organization,
                'sku':sku,
                'arts':arts,
                "subject":subject,
                "brand":brand,
                "title":title,
                "meneger":meneger,
                "created_at":created_at,
                "updated_at":updated_at
            })
            df2_f.append(df_tl)
            total = data_tl['cursor']['total']
            if total != 0:
                updatedAt = data_tl['cursor']['updatedAt']
            else:
                pass
            nmID = data_tl['cursor']['nmID']
            N += 1

        if df2_f is None or len(df2_f) == 0:
            df2 = pd.DataFrame(df2_f)
        else:
            df2 = pd.concat(df2_f)
        df_ip1.append(df1)
        df_ip2.append(df2)
    df_1 = pd.concat(df_ip1)
    df_2 = pd.concat(df_ip2)
    wb_sku = pd.concat([df_1, df_2])
    wb_sku = wb_sku[['organization', 'sku', 'arts', 'title', 'subject', 'brand', 'meneger']]







    print('############---Выгрузка поартикульной платой приёмкой---##################')
    paid_acceptance = []
    if diff_date < 31:
        for api in range(len(api_keys[api_keys['marketplace']=='Wildberries']['organization'])):
            api_key = api_keys[api_keys['marketplace']=='Wildberries']['api_key'].values[api]
            organization = api_keys[api_keys['marketplace']=='Wildberries']['organization'].values[api] 
            print(organization)  
            url = 'https://seller-analytics-api.wildberries.ru/api/v1/analytics/acceptance-report'  
            headers = {
                    'Authorization':api_key,
                    'Content-Type':'application/json'
            }       
            params = {
                    'dateFrom':date_from_str,
                    'dateTo':date_to_str
            }       
            res = requests.get(url=url, headers=headers, params=params)
            print(res)
            data = res.json()
            df = pd.DataFrame(data['report'])
            df['organization'] = organization
            paid_acceptance.append(df)

        paid_acceptance = pd.concat(paid_acceptance)
        if paid_acceptance.shape[0] != 0:
            paid_acceptance = paid_acceptance[['nmID','organization','total']]
            paid_acceptance = paid_acceptance.rename(columns={'nmID':'SKU','total':'paid_acceptance_sum','organization':'ИП'})
            paid_acceptance = paid_acceptance.groupby(['SKU','ИП'])[['paid_acceptance_sum']].sum().reset_index()
        else:
            pass
    else:
        print('Много дней в отчёте для выгрузки платной приёмки')



    print('############---Выгрузка характеристик товаров с Ozona---##################')

    ###############################

    async def fetch_product_list(session, client_id, api_key, last_id):
        url = 'https://api-seller.ozon.ru/v3/product/list'
        headers = {
            'Contеnt-Type': 'application/json',
            'Client-Id': client_id,
            'Аpi-Key': api_key
        }
        params = {
            "filter": {"visibility": "ALL"},
            "last_id": last_id,
            "limit": 100
        }
        async with session.post(url, headers=headers, json=params) as response:
            return await response.json()


    async def fetch_product_info(session, client_id, api_key, product_id):
        url = 'https://api-seller.ozon.ru/v3/product/info/list'
        headers = {
            'Content-Type': 'application/json',
            'Client-Id': client_id,
            'Api-Key': api_key
        }
        params = {
            "offer_id": "",
            "product_id": product_id,
            "sku": 0
        }
        async with session.post(url, headers=headers, json=params) as response:
            return await response.json()


    async def process_organization(organization_data, session):
        client_id = organization_data['client_id']
        api_key = organization_data['api_key']
        organization_name = organization_data['organization']
        product_id = []
        arts = []
        last_id = ''

        while True:
            data = await fetch_product_list(session, client_id, api_key, last_id)
            last_id = data['result']['last_id']

            for item in data['result']['items']:
                product_id.append(item['product_id'])
                arts.append(item['offer_id'])

            await asyncio.sleep(2)  # Уважение к лимитам API

            if last_id == '':
                break

        tasks = [fetch_product_info(session, client_id, api_key, pid) for pid in product_id]
        product_info = await asyncio.gather(*tasks)

        name = []
        sku = []
        for info in product_info:
            try:
                name.append(info['result']['name'])
                sku.append(info['result']['sku'])
            except (KeyError, TypeError):  # Обработка отсутствия ключей или некорректных типов данных
                name.append(None)  # или "" - в зависимости от вашей логики обработки
                sku.append(None) 

        #Обработка потенциальных ошибок
        if len(name) != len(product_id) or len(sku) != len(product_id):
            print(f"Warning: Data mismatch for {organization_name}. Check API responses.")

        df_0 = pd.DataFrame({'product_id': product_id, 'atrs': arts, 'name': name, 'SKU': sku})
        df_0['organization'] = organization_name
        return df_0



    async def main(marketplace, api_keys):
        async with aiohttp.ClientSession() as session:
            organization_data = api_keys[api_keys['marketplace'] == marketplace]
            tasks = [process_organization(row, session) for _, row in organization_data.iterrows()]
            dfs = await asyncio.gather(*tasks)
            return pd.concat(dfs)

    # Ваш код вызова функции:
    marketplace = "Ozon" 
    oz_sku = asyncio.run(main(marketplace, api_keys))

    return ads_2024, remains_2024, detal_fin1_for_ABC, supplier_arts, wb_sku, paid_acceptance, oz_sku, oz_remains


def ET_wb(ads_2024, remains_2024, detal_fin1_for_ABC, wb_sku, paid_acceptance, date_from, date_to, diff_date):


    # Отчёты ВБ (3)
    otchets_wb = pd.read_excel('otshets_wb.xlsx',engine='openpyxl')
    otchets_wb = otchets_wb.fillna(0)
    otchets_wb['Дата начала'] = pd.to_datetime(otchets_wb['Дата начала'])
    otchets_wb['Дата конца'] = pd.to_datetime(otchets_wb['Дата конца'])
    otchets_wb_v = otchets_wb[(otchets_wb['Дата начала']>=date_from)&(otchets_wb['Дата конца']<=date_to)]
    otchets_wb_v = otchets_wb_v[['ИП','Транзит','Реклама ВБ','Стоимость платной приемки','Стоимость хранения','Продвижение','Медиа']]


    # Хранение поартикульно по дням (4,5)


    # Остатки по дням (5)
    remains = remains_2024
    remains['date'] = pd.to_datetime(remains['date'])
    remains['ostatki'] = remains['quantity'] + remains['in_way_from_client']
    remains_full = remains.groupby(['date','SKU','organization_id'])[['ostatki']].sum().reset_index()

    sku_remains = remains_full['SKU'].unique()
    dates = pd.date_range(start='2024-01-01', end=date.today())
    full_index = pd.MultiIndex.from_product([sku_remains, dates], names=['SKU', 'date'])
    full_df = pd.DataFrame(index=full_index).reset_index()
    remains_full = pd.merge(full_df, remains_full, on=['SKU', 'date'], how='left')
    remains_full['ostatki'] = remains_full['ostatki'].interpolate()
    
    remains_full = remains_full[(remains_full['date']>=date_from)] ### !!!!!!!!!!
    remains_full = remains_full[['date','SKU','organization_id','ostatki']]
    remains_v = remains_full[(remains_full['date']==date_to)]
    remains_v = remains_v.groupby(['SKU'])[['ostatki']].sum().reset_index()
    remains_avg = remains_full.groupby(['date','SKU'])[['ostatki']].sum().reset_index()
    remains_avg = remains_avg.groupby(['SKU'])[['ostatki']].mean().reset_index()
    remains_avg = remains_avg.rename(columns={'ostatki':'avg_remains'})



    # Платная приёмка, хранение и транзит по дням
    otchets_wb_dt_sum = otchets_wb.groupby(['Дата начала','ИП'])[['Транзит','Стоимость платной приемки','Стоимость хранения']].sum().reset_index()
    otchets_wb_dt_sum['tranzit_day'] = otchets_wb_dt_sum['Транзит']/7
    otchets_wb_dt_sum['paid_acceptance_day'] = otchets_wb_dt_sum['Стоимость платной приемки']/7
    otchets_wb_dt_sum['storage_day'] = otchets_wb_dt_sum['Стоимость хранения']/7

    remains_full['date_otchets'] = remains_full['date'].apply(lambda x: otchets_wb['Дата начала'][otchets_wb['Дата начала'] <= x].max())

    remains_full['dol_remains'] = remains_full.groupby(['date'])[['ostatki']].transform(lambda x: x/x.sum())
    remains_full['dol_remains_org'] = remains_full.groupby(['date','organization_id'])[['ostatki']].transform(lambda x: x/x.sum())
    remains_paid_acceptance = remains_full.merge(otchets_wb_dt_sum, how='left', left_on=['date_otchets','organization_id'], right_on=['Дата начала','ИП'])
    remains_paid_acceptance['tranzit_sum'] = remains_paid_acceptance['dol_remains_org']*remains_paid_acceptance['tranzit_day']
    remains_paid_acceptance['paid_acceptance_sum'] = remains_paid_acceptance['dol_remains_org']*remains_paid_acceptance['paid_acceptance_day']
    remains_paid_acceptance['storage_sum'] = remains_paid_acceptance['dol_remains_org']*remains_paid_acceptance['storage_day']
    paid_acceptance_tranzit = remains_paid_acceptance[(remains_paid_acceptance['date']>=date_from)&(remains_paid_acceptance['date']<=date_to)]
    if diff_date > 31:
        paid_acceptance_tranzit = paid_acceptance_tranzit.groupby(['SKU'])[['tranzit_sum','storage_sum','paid_acceptance_sum']].sum().reset_index()
    else:
        paid_acceptance_tranzit = paid_acceptance_tranzit.groupby(['SKU'])[['tranzit_sum','storage_sum']].sum().reset_index()






    # Выделение подкатегорий для товаров с помощью ещё одного справочника для товаров WB
    wb_charakteristic_sku = wb_sku
    wb_charakteristic_sku = wb_charakteristic_sku.rename(columns={'sku':'SKU','meneger':'Менеджер','arts':'Артикул продавца','organization':'ИП'})
    wb_charakteristic_sku['ah'] = 1
    wb_charakteristic_sku = wb_charakteristic_sku.groupby(['ИП','SKU','Артикул продавца','title','subject','Менеджер'])[['ah']].count().reset_index()
    def sub_category(row):
        if isinstance(row['title'], str):
            if ('куртка' in row['title'].lower()
                and 'кожан' in row['title'].lower()
                and 'куртк' in row['subject'].lower()):
                return 'Куртки кожаные'
            elif ('куртка' in row['title'].lower()
                and 'зимн' in row['title'].lower()
                and 'стеган' not in row['title'].lower()):
                return 'Куртки зимние'
            elif ('куртка' in row['title'].lower()
                and 'демисез' in row['title'].lower()):
                return 'Куртки демисезон'
            elif ('куртка' in row['title'].lower()
                and 'стеган' in row['title'].lower()):
                return 'Куртки стеганные'
            elif ('пальто' in row['title'].lower()
                and 'пиджак' in row['title'].lower()):
                return 'Пальто-пиджак'
            elif ('дублен' in row['title'].lower()
                and 'авиатор' in row['title'].lower()):
                return 'Дубленки авиатор'
            elif ('дублен' in row['title'].lower()
                and ('двух' in row['title'].lower() or 'двуст' in row['title'].lower())):
                return 'Дубленки двухсторонние'
            else:
                return row['subject']


    wb_charakteristic_sku['Subcategory'] = wb_charakteristic_sku.apply(lambda row: sub_category(row), axis=1).fillna('нет категории')



    # Реклама ВБ (ads)

    ads = ads_2024
    ads['date'] = pd.to_datetime(ads['date'],format='%d/%m/%y')
    ads['internal_ads_sum'] = ads['internal_ads_sum'].astype(float)
    ads = ads[(ads['date']>=date_from)&(ads['date']<=date_to)]
    ads = ads.groupby(['nm_id'])[['internal_ads_sum']].sum().reset_index()
    ads = ads.rename(columns={'nm_id':'SKU'})
    ads['SKU'] = ads['SKU'].astype(int)



    # Внешка  
    vnesh_ads = pd.read_excel('vnesh_ads.xlsx',engine="openpyxl",sheet_name='Внешняя реклама ОТЧЕТ')   
    vnesh_ads['Дата выхода'] = pd.to_datetime(vnesh_ads['Дата выхода'],format='mixed')
    vnesh_ads['date'] = vnesh_ads['Дата выхода'] + pd.Timedelta(days=7)
    vnesh_ads = vnesh_ads[['date','артикул','Цена']]
    vnesh_ads = vnesh_ads[(vnesh_ads['date']>=date_from)&(vnesh_ads['date']<=date_to)]
    vnesh_ads = vnesh_ads.rename(columns={'артикул':'SKU','Цена':'external_ads'})
    vnesh_ads = vnesh_ads.groupby(['SKU'])[['external_ads']].sum().reset_index()

    # Интеграции
    integration = pd.read_excel('vnesh_ads.xlsx',engine='openpyxl',sheet_name='Интеграции с блогерами')
    integration['Дата выхода'] = pd.to_datetime(integration['Дата выхода'],format='mixed')
    integration['date'] = integration['Дата выхода'] + pd.Timedelta(days=7)
    integration = integration[['артикул','Цена','date']]
    integration = integration[(integration['date']>=date_from)&(integration['date']<=date_to)]
    integration = integration.rename(columns={'артикул':'SKU','Цена':'integration_sum'})
    integration = integration.groupby(['SKU'])[['integration_sum']].sum().reset_index()
    integration['SKU'] = integration['SKU'].astype(int)

    #Раздачи
    razdachi = pd.read_excel('vnesh_ads.xlsx',engine='openpyxl',sheet_name='Расходы на раздачи')
    razdachi['дата'] = pd.to_datetime(razdachi['дата выплаты'],format='mixed')
    razdachi['date'] = razdachi['дата'] + pd.Timedelta(days=7)
    razdachi = razdachi[['артикул','сумма+комис.','date']]
    razdachi['сумма+комис.'] = razdachi['сумма+комис.'].astype(float)
    razdachi = razdachi[(razdachi['date']>=date_from)&(razdachi['date']<=date_to)]
    razdachi = razdachi.rename(columns={'артикул':'SKU','сумма+комис.':'razdachi_sum'})
    razdachi = razdachi.groupby(['SKU'])[['razdachi_sum']].sum().reset_index()
    razdachi['SKU'] = razdachi['SKU'].astype(int)




    abc = pd.merge(detal_fin1_for_ABC, paid_acceptance_tranzit, how='outer', on='SKU')
    abc = pd.merge(abc, ads, how='outer', on='SKU')
    abc = pd.merge(abc, vnesh_ads, how='left', on='SKU')
    abc = pd.merge(abc, integration, how='left', on='SKU')
    abc = pd.merge(abc, razdachi, how='left', on='SKU')
    abc = pd.merge(abc, remains_v, how='outer', on='SKU')
    abc = pd.merge(abc, remains_avg, how ='left', on='SKU')
    if diff_date < 31 and paid_acceptance.shape[0] != 0:
        abc = pd.merge(abc, paid_acceptance, how='left', on=['SKU','ИП'])
    elif diff_date < 31 and paid_acceptance.shape[0] == 0:
        abc['paid_acceptance_sum'] = 0
    else:
        pass

    abc = pd.merge(abc, wb_charakteristic_sku, how='left', on='SKU')
    abc['ИП_x'] = abc['ИП_x'].fillna(0)
    abc['ИП'] = abc.apply(lambda row: row['ИП_x'] if row['ИП_x']!=0 else row['ИП_y'], axis=1)
    abc = abc.fillna(0)


    abc['dol_revenue'] = abc['revenue_spp']/abc['revenue_spp'].sum()
    abc['dol_remains'] = abc['ostatki']/abc['ostatki'].sum()
    abc['cost'] = abc['purchase_price'] + abc['package_full']
    abc['vnesh_ads'] = abc['external_ads'] + abc['integration_sum'] + abc['razdachi_sum']
    abc['Логистика до склада МП'] = abc['Логистика до склада МП'] + abc['tranzit_sum']
    abc['wb_compensations'] = abc['wb_compensations']*(-1)


    penalty = abc[abc['SKU'] == 0]['Штрафы ВБ'].sum()
    abc['Штрафы ВБ'] = abc['Штрафы ВБ'] + (abc['dol_revenue']*penalty)

    abc = abc[abc['SKU']!=0]

    abc['mp'] = 'WB'

    abc = abc.fillna(0)

    abc['Менеджер'] = abc['Менеджер'].replace(0,'нет менеджера')

    abc = abc[['mp','ИП','subject','Subcategory','Артикул продавца','SKU','Менеджер','orders','sales_count','sales_with_return', 'return_count','revenue_spp'\
            ,'rrp','mrp','sales', 'return','purchase_price','package_full','Скидка ВБ','Реальная комиссия','Штрафы ВБ'\
                ,'Логистика до продавца','Логистика до склада МП','Логистика продаж','storage_sum','paid_acceptance_sum','internal_ads_sum'\
                ,'ostatki','cost','revenue_op','avg_remains','vnesh_ads']]
    
    print(round(detal_fin1_for_ABC['revenue_spp'].sum())==round(abc['revenue_spp'].sum()))
    print(abc[abc['Артикул продавца'].duplicated()])
    
    abc['SKU'] = abc['SKU'].astype(str).str.strip()

    abc = abc.fillna(0)
    
    return abc



##################################################################################################################


def load_wb(abc):
    mp = abc[['mp']]
    worksheet1.update(f'A4:A{len(mp.values)+4}', mp.values.tolist())

    ip = abc[['ИП']]
    worksheet1.update(f'B4:B{len(ip.values)+4}', ip.values.tolist())

    subject = abc[['subject']]
    worksheet1.update(f'C4:C{len(subject.values)+4}', subject.values.tolist())

    art = abc[['Артикул продавца']]
    worksheet1.update(f'I4:I{len(art.values)+4}', art.values.tolist())

    sku1 = abc[['SKU']]
    worksheet1.update(f'J4:J{len(sku1.values)+4}', sku1.values.tolist())

    orders = abc[['orders']]
    worksheet1.update(f'K4:K{len(orders.values)+4}', orders.values.tolist())

    sales_count = abc[['sales_count']]
    worksheet1.update(f'L4:L{len(sales_count.values)+4}', sales_count.values.tolist())

    sales_with_return = abc[['sales_with_return']]
    worksheet1.update(f'M4:M{len(sales_with_return.values)+4}', sales_with_return.values.tolist())

    return_count = abc[['return_count']]
    worksheet1.update(f'N4:N{len(return_count.values)+4}', return_count.values.tolist())

    sales = abc[['sales']]
    worksheet1.update(f'Y4:Y{len(sales.values)+4}', sales.values.tolist())

    return1 = abc[['return']]
    worksheet1.update(f'Z4:Z{len(return1.values)+4}', return1.values.tolist())

    spp = abc[['Скидка ВБ']]
    worksheet1.update(f'AP4:AP{len(spp.values)+4}', spp.values.tolist())

    commision = abc[['Реальная комиссия']]
    worksheet1.update(f'AR4:AR{len(commision.values)+4}', commision.values.tolist())

    log_to_seller = abc[['Логистика до продавца']]
    worksheet1.update(f'AU4:AU{len(log_to_seller.values)+4}', log_to_seller.values.tolist())

    log_to_wb = abc[['Логистика до склада МП']]
    worksheet1.update(f'AV4:AV{len(log_to_wb.values)+4}', log_to_wb.values.tolist())

    log_sales = abc[['Логистика продаж']]
    worksheet1.update(f'AW4:AW{len(log_sales.values)+4}', log_sales.values.tolist())

    storage_cost = abc[['storage_sum']]
    worksheet1.update(f'BE4:BE{len(storage_cost.values)+4}', storage_cost.values.tolist())

    paid_acceptance = abc[['paid_acceptance_sum']]
    worksheet1.update(f'BL4:BL{len(paid_acceptance.values)+4}', paid_acceptance.values.tolist())

    penalty1 = abc[['Штрафы ВБ']]
    worksheet1.update(f'BM4:BM{len(penalty1.values)+4}', penalty1.values.tolist())

    internal_ads_sum1 = abc[['internal_ads_sum']]
    worksheet1.update(f'BR4:BR{len(internal_ads_sum1.values)+4}', internal_ads_sum1.values.tolist())

    vnesh_ads1 = abc[['vnesh_ads']]
    worksheet1.update(f'BV4:BV{len(vnesh_ads1.values)+4}', vnesh_ads1.values.tolist())

    ost1 = abc[['ostatki']]
    worksheet1.update(f'CS4:CS{len(ost1.values)+4}', ost1.values.tolist())

    avg_remains = abc[['avg_remains']]
    worksheet1.update(f'CU4:CU{len(avg_remains.values)+4}', avg_remains.values.tolist())

###################################################################################################################
def load_wb_past(abc):
    abc['revenue_netto'] = abc['sales'] - abc['return']

    abc['revenue_our_price'] = abc['revenue_netto'] + abc['Скидка ВБ']

    def taxes(row):
        if row['ИП']=='Организация 1':
            return row['revenue_netto']*0.07 + row['revenue_netto']*0.05
        elif row['ИП']=='Организация 5':
            return row['revenue_netto']*0.11 + row['revenue_netto']*0.05
        elif row['ИП']=='Организация 3':
            return row['revenue_netto']*0.04 + row['revenue_netto']*0.05
        else:
            return row['revenue_netto']*0.02 + row['revenue_netto']*0.05
        
    abc['taxes'] = abc.apply(lambda x: taxes(x),axis=1)

    abc['expenses'] = abc['Скидка ВБ'] + abc['Реальная комиссия'] + abc['Логистика до продавца'] + abc['Логистика до склада МП'] \
        + abc['Логистика продаж'] + abc['storage_sum'] + abc['paid_acceptance_sum'] + abc['Штрафы ВБ']
    



    arts = abc[['Артикул продавца']]
    worksheet1.update(f'DQ4:DQ{len(arts.values)+4}', arts.values.tolist())

    revenue_our_price = abc[['revenue_our_price']]
    worksheet1.update(f'DR4:DR{len(revenue_our_price.values)+4}', revenue_our_price.values.tolist())

    revenue_netto = abc[['revenue_netto']]
    worksheet1.update(f'DS4:DS{len(revenue_netto.values)+4}', revenue_netto.values.tolist())

    sales_with_return = abc[['sales_with_return']]
    worksheet1.update(f'DT4:DT{len(sales_with_return.values)+4}', sales_with_return.values.tolist())

    taxes_s = abc[['taxes']]
    worksheet1.update(f'DU4:DU{len(taxes_s.values)+4}', taxes_s.values.tolist())

    expenses = abc[['expenses']]
    worksheet1.update(f'DV4:DV{len(expenses.values)+4}', expenses.values.tolist())

    internal_ads_sum = abc[['internal_ads_sum']]
    worksheet1.update(f'DW4:DW{len(internal_ads_sum.values)+4}', internal_ads_sum.values.tolist())



##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################


# OZON

def ET_oz(supplier_arts, oz_sku, oz_remains, date_from, date_to):
    
    oz = pd.read_excel('oz_nach_new.xlsx',engine='openpyxl',sheet_name='Sheet1')

    # Трафареты (+ реклама в поиске, вывод в топ) поартикульно
    ads_oz = pd.read_excel('oz_ads_new.xlsx',engine='openpyxl')
    ads_oz['date'] = pd.to_datetime(ads_oz['date'],format="%d.%m.%Y")
    ads_oz = ads_oz[['organization','date','SKU','internal_ads_sum']]
    ads_oz = ads_oz[(ads_oz['date']>=date_from)&(ads_oz['date']<=date_to)]
    ads_oz = ads_oz.groupby(['SKU'])[['internal_ads_sum']].sum().reset_index()
    ads_oz['SKU'] = ads_oz['SKU'].astype(object)

    remains_oz = oz_remains
    remains_oz['Дата'] = pd.to_datetime(remains_oz['Дата'])
    remains_oz_v = remains_oz[(remains_oz['Дата']==date_to)]
    remains_oz_v = remains_oz_v.groupby(['Артикул'])[['remains']].sum().reset_index() 
    # Средние остатки
    remains_avg = remains_oz[(remains_oz['Дата']>=date_from)&(remains_oz['Дата']<=date_to)]
    remains_avg = remains_avg.groupby(['Дата','Артикул'])[['remains']].sum()
    remains_avg_gr = remains_avg.groupby(['Артикул'])[['remains']].mean().reset_index()
    remains_avg_gr = remains_avg_gr.rename(columns={'remains':'remains_avg'})

    # Себес
    cost_oz = pd.read_excel('cost_oz.xlsx',engine='openpyxl',sheet_name='Номенклатура ОЗОН').fillna(0)
    cost_oz = cost_oz[['Менеджер','Артикул продавца','Артикул по цветам','Артикул озон','Себестоимость','Честный знак']]
    cost_oz['Закупочная цена'] = cost_oz['Себестоимость'] - 150
    cost_oz['Упаковка'] = 150
    cost_oz = cost_oz.rename(columns={'Артикул продавца':'Артикул','Артикул озон':'SKU','Закупочная цена':'purchase_price','Упаковка':'package','Честный знак':'chz'})
    cost_oz['package_full'] = cost_oz['package'] + cost_oz['chz']
    cost_oz = cost_oz.groupby(['Менеджер','Артикул'])[['purchase_price','package_full']].mean().reset_index()

    def orders(row, type):
        if type == 'Логистика' or type == 'Обратная логистика':
            return row
        else:
            return 0
        
    def sale_count(row, type):
            if type == 'Выручка':
                return row
            else:
                return 0
            
    def return_count(row, type):
        if type == 'Возврат выручки':
            return row
        else:
            return 0
        
    def sale_with_return(row, type):
        if type == 'Выручка':
            return row
        elif type == 'Возврат выручки':
            return row*(-1)
        else:
            return 0
        
    ###################################################################################################
    def sale_op(row, type, group):
        if type == 'Выручка'\
            or (type == 'Баллы за скидки' \
                and group == 'Продажи')\
            or (type == 'Программы партнёров'\
                and group == 'Продажи'):
            return row
        else:
            return 0
        
    def return_op(row, type, group):
        if type == 'Возврат выручки'\
            or (type == 'Баллы за скидки' \
                and group == 'Возвраты')\
            or (type == 'Программы партнёров'\
                and group == 'Возвраты'):
            return row*(-1)
        else:
            return 0
        
    def revenue_op(row, type):
        if type == 'Выручка'\
            or type == 'Возврат выручки'\
            or type == 'Баллы за скидки'\
            or type == 'Программы партнёров':
            return row
        else:
            return 0
        
    ###################################################################################################

    def sales_spp(row, type, group):
        if type == 'Выручка'\
            or (group == 'Продажи' and type == 'Программы партнёров'):
            return row
        else:
            return 0

    def return_spp(row, type, group):
        if type == 'Возврат выручки'\
            or (group == 'Возвраты' and type == 'Программы партнёров'):
            return row*(-1)
        else:
            return 0
        
    def revenue_spp(row, type):
        if type == 'Выручка'\
            or type == 'Возврат выручки'\
            or type == 'Программы партнёров':
            return row
        else:
            return 0

    ###################################################################################################
    def commission(row, type, group):
        if group == 'Вознаграждение Ozon'\
            or type == 'Эквайринг':
            return row*(-1)
        else: 
            return 0
        
    def spp(row, type):
        if type == 'Баллы за скидки':
            return row
        else:
            return 0
        
    ###################################################################################################
    def log_to_ozon(row, type):
        if type == 'Кросс-докинг':
            return row*(-1)
        else:
            return 0
    def log_to_seller(row, type):
        if type == 'Вывоз товара со склада силами Ozon: Доставка курьером'\
            or type == 'Доставка возвратов до склада продавца силами Ozon':
            return row*(-1)
        else:
            return 0
        
    def logistics_full(row, type, group):
        if group == 'Услуги доставки'\
            or (group == 'Услуги агентов' and type == 'Последняя миля')\
            or (group == 'Услуги агентов' and type == 'Последняя миля - отмена начисления'):
            return row*(-1)
        else:
            return 0
        
    ###################################################################################################
        
    def other(row, type, group):
        if group == 'Другие услуги'\
            or (group == 'Услуги агентов' and type != 'Эквайринг' and type != 'Последняя миля' and type != 'Последняя миля - отмена начисления')\
            or group == 'Компенсации и декомпенсации'\
            or group == 'Прочие начисления':
            return row*(-1)
        else:
            return 0
        
    def storage(row, type, group):
        if (group == 'Услуги FBO'\
            and type != 'Кросс-докинг'\
            and type != 'Доставка возвратов до склада продавца силами Ozon'\
            and type != 'Вывоз товара со склада силами Ozon: Доставка курьером'):
            return row*(-1)
        else:
            return 0
        
    ###################################################################################################
    def reviews(row, type):
        if type == 'Баллы за отзывы'\
            or type == 'Приобретение отзывов на платформе':
            return row*(-1)
        else:
            return 0
    def ads_prod(row, type, group):
        if group == 'Продвижение и реклама' and (type != 'Трафареты' 
                                                and type != 'Приобретение отзывов на платформе' 
                                                and type != 'Баллы за отзывы'
                                                and type != 'Продвижение в поиске'
                                                and type != 'Вывод в топ'):
            return row*(-1)
        else:
            return 0
        
    oz = oz[(oz['Дата начисления']>=date_from)&(oz['Дата начисления']<=date_to)]
    oz['orders'] = oz.apply(lambda row: orders(row['Количество'],row['Тип начисления']),axis=1)
    oz['sales_count'] = oz.apply(lambda row: sale_count(row['Количество'],row['Тип начисления']),axis=1)
    oz['return_count'] = oz.apply(lambda row: return_count(row['Количество'],row['Тип начисления']),axis=1)
    oz['sales_with_return'] = oz.apply(lambda row: sale_with_return(row['Количество'],row['Тип начисления']),axis=1)

    oz['sales_op'] = oz.apply(lambda row: sale_op(row['Сумма итого, руб'],row['Тип начисления'],row['Группа услуг']),axis=1)
    oz['return_op'] = oz.apply(lambda row: return_op(row['Сумма итого, руб'],row['Тип начисления'],row['Группа услуг']),axis=1)
    oz['revenue_op'] = oz.apply(lambda row: revenue_op(row['Сумма итого, руб'],row['Тип начисления']),axis=1)

    oz['sales_spp'] = oz.apply(lambda row: sales_spp(row['Сумма итого, руб'],row['Тип начисления'],row['Группа услуг']),axis=1)
    oz['return_spp'] = oz.apply(lambda row: return_spp(row['Сумма итого, руб'],row['Тип начисления'],row['Группа услуг']),axis=1)
    oz['revenue_spp'] = oz.apply(lambda row: revenue_spp(row['Сумма итого, руб'],row['Тип начисления']),axis=1)


    oz['Комиссия за продажу'] = oz.apply(lambda row: commission(row['Сумма итого, руб'],row['Тип начисления'],row['Группа услуг']),axis=1)
    oz['СПП'] = oz.apply(lambda row: spp(row['Сумма итого, руб'],row['Тип начисления']),axis=1)

    oz['Логистика до склада Озон'] = oz.apply(lambda row: log_to_ozon(row['Сумма итого, руб'],row['Тип начисления']),axis=1)
    oz['Логистика до продавца'] = oz.apply(lambda row: log_to_seller(row['Сумма итого, руб'],row['Тип начисления']),axis=1)
    oz['Логистика продаж'] = oz.apply(lambda row: logistics_full(row['Сумма итого, руб'],row['Тип начисления'],row['Группа услуг']),axis=1)

    oz['Хранение OZ'] = oz.apply(lambda row: storage(row['Сумма итого, руб'],row['Тип начисления'],row['Группа услуг']),axis=1)
    oz['Другие услуги'] = oz.apply(lambda row: other(row['Сумма итого, руб'],row['Тип начисления'],row['Группа услуг']),axis=1)

    oz['Приобретение отзывов'] = oz.apply(lambda row: reviews(row['Сумма итого, руб'],row['Тип начисления']),axis=1)
    oz['Услуги продвижения товаров'] = oz.apply(lambda row: ads_prod(row['Сумма итого, руб'],row['Тип начисления'],row['Группа услуг']),axis=1)

    oz = oz.fillna(0)

    a = oz.groupby(['Артикул','SKU','Организация'])\
        [['orders','sales_count','sales_with_return','return_count',\
        'sales_spp','revenue_spp','return_spp', 'СПП',\
        'sales_op','return_op','revenue_op','Комиссия за продажу',\
        'Логистика продаж','Логистика до склада Озон','Логистика до продавца',
        'Хранение OZ','Другие услуги',\
        'Приобретение отзывов','Услуги продвижения товаров']]\
        .sum()\
        .reset_index().fillna(0)

    a = a.merge(remains_oz_v, how='outer', on='Артикул').fillna(0)
    a = a.merge(remains_avg_gr, how='left', on='Артикул').fillna(0)
    a = a.merge(ads_oz, how='outer', on='SKU')

    a['SKU'] = a['SKU'].astype(str).str.strip()

    sp_oz = oz_sku
    sp_oz = sp_oz.rename(columns={'organization':'Организация','atrs':'Артикул'})
    sp_oz = sp_oz.groupby(['Артикул','Организация'])[['product_id']].count().reset_index()
    sp_oz = sp_oz[['Организация','Артикул']]


    a = a.merge(sp_oz, how='left', on='Артикул').fillna(0)
    a['Организация'] = a.apply(lambda row: row['Организация_x'] if row['Организация_x']!=0 else row['Организация_y'], axis=1)

    a = a.groupby(['Артикул','Организация'])\
        [['orders','sales_count','sales_with_return','return_count',\
        'sales_spp','revenue_spp','return_spp', 'СПП',\
        'sales_op','return_op','revenue_op','Комиссия за продажу',\
        'Логистика продаж','Логистика до склада Озон','Логистика до продавца',\
        'Хранение OZ','Другие услуги',\
        'Приобретение отзывов','Услуги продвижения товаров',\
        'internal_ads_sum',\
        'remains',
        'remains_avg']]\
        .agg({
            'orders':'sum',
            'sales_count':'sum',
            'sales_with_return':'sum',
            'return_count':'sum',
            'sales_op':'sum',
            'return_op':'sum',
            'sales_spp':'sum',
            'revenue_spp':'sum',
            'revenue_op':'sum',
            'return_spp':'sum',
            'СПП':'sum',
            'Комиссия за продажу':'sum',
            'Логистика продаж':'sum',
            'Хранение OZ':'sum',
            'Другие услуги':'sum',
            'Логистика до склада Озон':'sum',
            'Логистика до продавца':'sum',
            'Приобретение отзывов':'sum',
            'Услуги продвижения товаров':'sum',
            'internal_ads_sum':'sum',
            'remains':'sum',
            'remains_avg':'sum'   
        })\
        .reset_index()


    a['dol_revenue'] = a['revenue_op']/a['revenue_op'].sum()
    a['dol_remains_org'] = a['remains_avg'] / a.groupby('Организация')['remains_avg'].transform('sum')
    a['dol_remains'] = a['remains_avg']/a['remains_avg'].sum()

    a['Приобретение отзывов'] = a['dol_revenue']*a['Приобретение отзывов'].sum()

    a['Хранение OZ по организациям'] = a.groupby('Организация')['Хранение OZ'].transform('sum')
    
    a['Хранение OZ'] = a['dol_remains_org']*a['Хранение OZ по организациям']
    a['Другие услуги'] = a['dol_remains']*a['Другие услуги'].sum()
    a['Логистика до продавца'] = a['dol_remains']*a['Логистика до продавца'].sum()
    a['Логистика до склада Озон'] = a['dol_remains']*a['Логистика до склада Озон'].sum()
    a['Комиссия Озон'] = a['Комиссия за продажу']
    a['price'] = a.apply(lambda row: row['revenue_op']/row['sales_with_return'] if row['sales_with_return']>0 else 0, axis=1)
    a['orders_sum'] = a['orders']*a['price']
    a = a.fillna(0)
    a['Артикул'] = a['Артикул'].astype(str)
    a['Артикул по цветам']= a['Артикул'].apply(lambda text: text[:text.rfind('-')] if text.rfind('-') != -1 else text)
    a = a[['Организация','Артикул','Артикул по цветам','orders','sales_count','sales_with_return', 'return_count',\
                    'orders_sum',\
                    'sales_spp','revenue_spp','return_spp','СПП',\
                    'revenue_op','sales_op','return_op',\
                        'Комиссия Озон','Хранение OZ',\
                        'Другие услуги','Логистика до продавца','Логистика до склада Озон', 'Логистика продаж',\
                        'Приобретение отзывов','Услуги продвижения товаров', 'internal_ads_sum',\
                        'remains','remains_avg']]
    a = a[a['Артикул']!=0]


    a1 = a.groupby(['Артикул по цветам','Организация'])[[
                    'orders','sales_count','sales_with_return', 'return_count',\
                    'sales_spp','revenue_spp','return_spp','СПП',\
                    'revenue_op','sales_op','return_op',\
                    'Комиссия Озон','Хранение OZ',\
                    'Другие услуги','Логистика до продавца','Логистика до склада Озон', 'Логистика продаж',\
                    'Приобретение отзывов','Услуги продвижения товаров', 'internal_ads_sum',\
                    'remains','remains_avg']]\
            .agg({
                    'orders':'sum',
                    'sales_count':'sum',
                    'sales_with_return':'sum',
                    'return_count':'sum',
                    'sales_op':'sum',
                    'return_op':'sum',
                    'sales_spp':'sum',
                    'return_spp':'sum',
                    'revenue_spp':'sum',
                    'СПП':'sum',
                    'revenue_op':'sum',
                    'Комиссия Озон':'sum',
                    'Хранение OZ':'sum',
                    'Другие услуги':'sum',
                    'Логистика до продавца':'sum',
                    'Логистика до склада Озон':'sum',
                    'Логистика продаж':'sum',
                    'Приобретение отзывов':'sum',
                    'Услуги продвижения товаров':'sum',
                    'internal_ads_sum':'sum',
                    'remains':'sum',
                    'remains_avg':'sum' 
                    }).reset_index()
    a1['mp'] = 'OZ'
    a1 = a1[a1['Артикул по цветам']!='нет']
    print(oz['revenue_op'].sum()==a1['revenue_op'].sum())
    print(a1[a1['Артикул по цветам'].duplicated()])


    return a1


##############################################################################################

def load_oz(abc, a1):
    start_oz = 4 + len(abc.values)
    end_oz = start_oz + len(a1.values)

    mp = a1[['mp']]
    worksheet1.update(f'A{start_oz}:A{end_oz}', mp.values.tolist())

    ip = a1[['Организация']]
    worksheet1.update(f'B{start_oz}:B{end_oz}', ip.values.tolist())

    arts = a1[['Артикул по цветам']]
    worksheet1.update(f'I{start_oz}:I{end_oz}', arts.values.tolist())

    orders = a1[['orders']]
    worksheet1.update(f'K{start_oz}:K{end_oz}', orders.values.tolist())

    sales_count = a1[['sales_count']]
    worksheet1.update(f'L{start_oz}:L{end_oz}', sales_count.values.tolist())

    sales_with_return = a1[['sales_with_return']]
    worksheet1.update(f'M{start_oz}:M{end_oz}', sales_with_return.values.tolist())

    return_count = a1[['return_count']]
    worksheet1.update(f'N{start_oz}:N{end_oz}', return_count.values.tolist())


    sales = a1[['sales_spp']]
    worksheet1.update(f'Y{start_oz}:Y{end_oz}', sales.values.tolist())

    return1 = a1[['return_spp']]
    worksheet1.update(f'Z{start_oz}:Z{end_oz}', return1.values.tolist())

    spp = a1[['СПП']]
    worksheet1.update(f'AP{start_oz}:AP{end_oz}', spp.values.tolist())

    commision = a1[['Комиссия Озон']]
    worksheet1.update(f'AR{start_oz}:AR{end_oz}', commision.values.tolist())

    log_to_seller = a1[['Логистика до продавца']]
    worksheet1.update(f'AU{start_oz}:AU{end_oz}', log_to_seller.values.tolist())

    log_to_wb = a1[['Логистика до склада Озон']]
    worksheet1.update(f'AV{start_oz}:AV{end_oz}', log_to_wb.values.tolist())

    log_sales = a1[['Логистика продаж']]
    worksheet1.update(f'AW{start_oz}:AW{end_oz}', log_sales.values.tolist())

    storage = a1[['Хранение OZ']]
    worksheet1.update(f'BE{start_oz}:BE{end_oz}', storage.values.tolist())

    other = a1[['Другие услуги']]
    worksheet1.update(f'BF{start_oz}:BF{end_oz}', other.values.tolist())

    internal_ads_sum1 = a1[['internal_ads_sum']]
    worksheet1.update(f'BT{start_oz}:BT{end_oz}', internal_ads_sum1.values.tolist())

    reviews1 = a1[['Приобретение отзывов']]
    worksheet1.update(f'BU{start_oz}:BU{end_oz}', reviews1.values.tolist())

    internal_ads_sum1 = a1[['Услуги продвижения товаров']]
    worksheet1.update(f'BS{start_oz}:BS{end_oz}', internal_ads_sum1.values.tolist())

    remains1 = a1[['remains']]
    worksheet1.update(f'CS{start_oz}:CS{end_oz}', remains1.values.tolist())

    remains_avg1 = a1[['remains_avg']]
    worksheet1.update(f'CU{start_oz}:CU{end_oz}', remains_avg1.values.tolist())

    print(start_oz)
    print(end_oz)


def load_oz_past(abc, a1):

    start_oz = 4 + len(abc.values)
    end_oz = start_oz + len(a1.values)
    a1['revenue_netto'] = a1['revenue_spp']

    a1['revenue_our_price'] = a1['revenue_op']

    def taxes(row):
        if row['Организация']=='Организация 4':
            return row['revenue_netto']*0.07 + row['revenue_netto']*0.05
        elif row['Организация']=='Организация 5':
            return row['revenue_netto']*0.11 + row['revenue_netto']*0.05
        elif row['Организация']=='Организация 3':
            return row['revenue_netto']*0.04 + row['revenue_netto']*0.05
        else:
            return row['revenue_netto']*0.02 + row['revenue_netto']*0.05
        
    a1['taxes'] = a1.apply(lambda x: taxes(x),axis=1)

    a1['ads'] = a1['internal_ads_sum'] + a1['Приобретение отзывов'] + a1['Услуги продвижения товаров']

    a1['expenses'] = a1['Комиссия Озон'] + a1['Хранение OZ'] + a1['Другие услуги'] \
        + a1['Логистика до продавца'] + a1['Логистика до склада Озон'] \
        + a1['Логистика продаж']
    



    arts = a1[['Артикул по цветам']]
    worksheet1.update(f'DQ{start_oz}:DQ{end_oz}', arts.values.tolist())

    revenue_our_price = a1[['revenue_our_price']]
    worksheet1.update(f'DR{start_oz}:DR{end_oz}', revenue_our_price.values.tolist())

    revenue_netto = a1[['revenue_netto']]
    worksheet1.update(f'DS{start_oz}:DS{end_oz}', revenue_netto.values.tolist())

    sales_with_return = a1[['sales_with_return']]
    worksheet1.update(f'DT{start_oz}:DT{end_oz}', sales_with_return.values.tolist())

    taxes_s = a1[['taxes']]
    worksheet1.update(f'DU{start_oz}:DU{end_oz}', taxes_s.values.tolist())

    expenses = a1[['expenses']]
    worksheet1.update(f'DV{start_oz}:DV{end_oz}', expenses.values.tolist())

    ads = a1[['ads']]
    worksheet1.update(f'DW{start_oz}:DW{end_oz}', ads.values.tolist())

    print(start_oz)
    print(end_oz)
