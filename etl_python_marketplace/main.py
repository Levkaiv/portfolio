from etl_python_marketplace.avto_abc import ET_wb, ET_oz, load_wb, load_oz, extract_data, load_wb_past, load_oz_past
from data_input import date_from, date_from_q, date_to, date_to_q, date_from_str, date_to_str, diff_date, \
    date_from_past, date_from_q_past, date_to_past, date_to_q_past, date_from_str_past, date_to_str_past, diff_date_past


ads_2024, remains_2024, detal_fin1_for_ABC, supplier_arts, wb_sku, paid_acceptance, oz_sku, oz_remains = extract_data(date_from, \
                                                                                date_from_q, date_to, date_to_q, date_from_str, date_to_str, diff_date)

print('############---Обработка данных для WB---##################')
wb = ET_wb(ads_2024, remains_2024, detal_fin1_for_ABC, \
                          wb_sku, paid_acceptance, date_from, date_to, diff_date)

print('############---Загрузка данных в Google Sheets по WB---##################')
load_wb(wb)

print('############---Обработка данных для Ozon---##################')
oz = ET_oz(supplier_arts, oz_sku, oz_remains, date_from, date_to)

print('############---Загрузка данных в Google Sheets по Ozon---##################')
load_oz(wb, oz)

print("############---Загружены основные данные---##################\n\n")
###########################################################################################
###########################################################################################
###########################################################################################
print("############---Загрузка данных за прошлый период---##################")

ads_2024, remains_2024, detal_fin1_for_ABC, supplier_arts, wb_sku, paid_acceptance, oz_sku, oz_remains = extract_data(date_from_past, \
                                                        date_from_q_past, date_to_past, date_to_q_past, date_from_str_past, date_to_str_past, diff_date_past)

print('############---Обработка данных для WB (прошлый период)---##################')
wb_past = ET_wb(ads_2024, remains_2024, detal_fin1_for_ABC, \
                               wb_sku, paid_acceptance, date_from_past, date_to_past, diff_date_past)

print('############---Загрузка данных в Google Sheets по WB (прошлый период)---##################')
load_wb_past(wb_past)

print('############---Обработка данных для Ozon (прошлый период)---##################')
oz_past = ET_oz(supplier_arts, oz_sku, oz_remains, date_from_past, date_to_past)

print('############---Загрузка данных в Google Sheets по Ozon (прошлый период)---##################')
load_oz_past(wb_past, oz_past) 

print('Всё прошло успешно!')