import pandas as pd
import gspread
from datetime import date
import datetime
################################################################################
################################################################################
date_from =         pd.to_datetime('2025-01-01')
date_from_str =                    '2025-01-01'
date_from_q =         datetime.date(2025, 1, 1)
date_from_api =   datetime.datetime(2025, 1, 1, 0, 0, 0, 0).isoformat() + 'Z'

date_to =           pd.to_datetime('2025-05-18')
date_to_str =                      '2025-05-18'
date_to_q =           datetime.date(2025, 5,18)
date_to_api =     datetime.datetime(2025, 5,18, 0, 0, 0, 0).isoformat() + 'Z'
marketplace2 = 'Ozon'
marketplace1 = 'Wilberries'

diff_date = (date_to - date_from).days
################################################################################
############################---Прошлый период---################################
date_from_past =         pd.to_datetime('2025-05-12')
date_from_str_past =                    '2025-05-12'
date_from_q_past =         datetime.date(2025, 5,12)
date_from_api_past =   datetime.datetime(2025, 5,12, 0, 0, 0, 0).isoformat() + 'Z'

date_to_past =           pd.to_datetime('2025-05-18')
date_to_str_past =                      '2025-05-18'
date_to_q_past =           datetime.date(2025, 5,18)
date_to_api_past =     datetime.datetime(2025, 5,18, 0, 0, 0, 0).isoformat() + 'Z'
marketplace2 = 'Ozon'
marketplace1 = 'Wilberries'

diff_date_past = (date_to_past - date_from_past).days
################################################################################
################################################################################


gc = gspread.service_account(filename='filename.json')

spreadsheet = gc.open('Поартикульный анализ 2024-2025')

worksheet1 = spreadsheet.worksheet('test')


############---Обновляем даты в отчёта в гугл таблице---##################
date_update = f'{pd.to_datetime(date.today())}'
worksheet1.update('C1', [[date_update]])

date_from1 = f'{date_from}'
worksheet1.update('I1', [[date_from1]])

date_to1 = f'{date_to}'
worksheet1.update('J1', [[date_to1]])

############---Обновляем даты за прошлый период---##################
date_from1_past = f'{date_from_past}'
worksheet1.update('CJ1', [[date_from1_past]])

date_to1_past = f'{date_to_past}'
worksheet1.update('CK1', [[date_to1_past]])