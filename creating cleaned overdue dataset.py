import pandas as pd
import json
import os
import numpy as np

def files(path):
    fds = sorted(os.listdir(path))
    filenames = []
    for file in fds:
        if file.endswith(('.json')): 
            filenames.append(file)
    return filenames

# =============================================================================
#                       Читаем данные из json
# =============================================================================
    
overdue_filds = ['no', 'active_summa', 'active_summa_nt', 'calc_peny_debt', 'calc_peny_debt_nt',
                 'comment_from_cft', 'contract_id', 'date_prolongation', 'debt_all',
                 'debt_all_nt', 'debt_on_date', 'debt_on_date_nt', 'last_pay_date',
                 'overdue_day', 'overdue_prc_debt', 'overdue_prc_debt_nt', 'plan_debt_on_date',
                 'plan_debt_on_date_nt', 'plan_prc_debt', 'plan_prc_debt_nt', 'download_date']
overdue = pd.DataFrame(columns = overdue_filds)

path = 'cs2018-07-17'
filenames = files(path)

passed = 0
added = 0

for file in filenames:
    f = open(path + '/' + file)
    row = json.load(f)
    f.close()
    
    for credit in row['credit']:
        data = credit['overdue']
        if data is None: 
            # Пропускаем записи, по которым нет overdue
            print(file, ' пропускаю')
            passed += 1
        else:
            # Добавляем в датасет записи, по которым есть overdue
            print(file, ' добавляю')
            overdue = overdue.append(data)
            added += 1

overdue.to_csv('overdue_initial.csv', index = False)

# =============================================================================
#    Работа с датасетом, очистка данных, выбор необходимой информации
# =============================================================================

overdue.drop(['Unnamed: 0', 'active_summa', 'calc_peny_debt',
              'debt_all', 'debt_on_date', 'overdue_prc_debt', 
              'plan_debt_on_date', 'plan_prc_debt'], axis = 1, inplace = True)

overdue['download_month'] = overdue['download_date'].apply(lambda x: pd.to_datetime(x).month)
overdue['date_prolongation'].fillna(0, inplace = True)
overdue['prolongated'] = overdue['date_prolongation'].apply(lambda x: 0 if x == 0 else 1)

overdue['comment_from_cft'].fillna(0, inplace = True)
overdue['refunded'] = overdue['comment_from_cft'].apply(lambda x: 0 if x == 0 else 1)
overdue.drop(['date_prolongation', 'comment_from_cft'], axis = 1, inplace = True)

# =============================================================================
#         Создание target-переменной и отбор данных для модели
# =============================================================================

contracts = list(overdue['contract_id'].value_counts().index)
dates = sorted(list(overdue['download_date'].value_counts().index))

cleaned_overdue = pd.DataFrame(columns = overdue.columns) # здесь будем формировать итоговый очищенный датасет
doubt_overdue = pd.DataFrame(columns = overdue.columns) # здесь будем хранить данные по клиентам, которые на 30 апреля не вышли из просрочки
temp_overdue = pd.DataFrame(overdue) # временный датасет, из которого будем постепенно удалять данные по клиентам, которые уже помещены в итоговый датасет

# первичная работа с клиентами, не вышедшими из просрочки на 30 апреля
for contract in contracts:
    temp_data = overdue[overdue['contract_id'] == contract]
    contract_dates = sorted(list(temp_data['download_date'].value_counts().index))
    last_day = contract_dates[-1]
    
    if last_day == '2018-04-30':
        last_day_data = temp_data[temp_data['download_date'] == last_day]
        if len(last_day_data) > 1:
            max_no = last_day_data['no'].max()
            last_day_data = last_day_data[last_day_data['no'] == max_no]
        
        if list(last_day_data['overdue_day'])[0] >= 365:
            # Считаем, что эти клиенты уже не выйдут из просрочки
            last_day_data['target'] = -1
            cleaned_overdue = cleaned_overdue.append(last_day_data)
            temp_overdue.drop(index = temp_overdue[temp_overdue['contract_id'] == contract].index, inplace = True)
        elif list(last_day_data['overdue_day'])[0] > 61:
            # Для использования даннных этих клиентов в обучении модели недостаточно информации
            doubt_overdue = doubt_overdue.append(last_day_data)
            temp_overdue.drop(index = temp_overdue[temp_overdue['contract_id'] == contract].index, inplace = True)
        
contracts = list(temp_overdue['contract_id'].value_counts().index)
overdue_march = temp_overdue[overdue['download_month'] == 3]
overdue_april = temp_overdue[overdue['download_month'] == 4]

# Работа отдельно с данными за март и апрель; получение target-переменной для клиентов, 
# которые ещемесячно выходят на просрочку, но через несколько дней выплачивают долг

# ------------ МАРТ ---------------
for contract in contracts:
    temp_data = overdue_march[overdue_march['contract_id'] == contract]
    if len(temp_data) == 0:
        continue
    contract_dates = sorted(list(temp_data['download_date'].value_counts().index))
    last_day = contract_dates[-1]
    
    last_day_data = temp_data[temp_data['download_date'] == last_day]
    if len(last_day_data) > 1:
        max_no = last_day_data['no'].max()
        last_day_data = last_day_data[last_day_data['no'] == max_no]
    cleaned_overdue = cleaned_overdue.append(last_day_data)

# ----------- АПРЕЛЬ --------------
for contract in contracts:    
    temp_data = overdue_april[overdue_april['contract_id'] == contract]
    if len(temp_data) == 0:
        continue
    contract_dates = sorted(list(temp_data['download_date'].value_counts().index))
    last_day = contract_dates[-1]
    
    last_day_data = temp_data[temp_data['download_date'] == last_day]
    if len(last_day_data) > 1:
        max_no = last_day_data['no'].max()
        last_day_data = last_day_data[last_day_data['no'] == max_no]
    cleaned_overdue = cleaned_overdue.append(last_day_data)

# Удаление из итогового датасета данных о контрактах, долг по которым выл выплачен в апреле 
# (в датасете остаются данные за апрель и удаляются данные за март)
for contract in contracts:    
    temp_data = cleaned_overdue[cleaned_overdue['contract_id'] == contract]
    if len(temp_data) == 0:
        continue
    elif len(temp_data) > 1:
        debt_march = list(temp_data[temp_data['download_month'] == 3]['overdue_day'])[0]
        debt_april = list(temp_data[temp_data['download_month'] == 4]['overdue_day'])[0]        
        if debt_march > 30 and debt_april > debt_march:
            cleaned_overdue.drop(index = temp_data[temp_data['download_month'] == 3].index, inplace = True)

new_contracts = list(cleaned_overdue['contract_id'].value_counts().index)

# Удаление из итогового датасета данных о контрактах, долг по которым не выплачен на 30 апреля
# (данные переносятся в датасет doubt_overdue)
for contract in new_contracts:
    temp_data = cleaned_overdue[cleaned_overdue['contract_id'] == contract]
    if '2018-04-30' in list(temp_data['download_date']):
        if np.isnan(list(temp_data[temp_data['download_date'] == '2018-04-30']['target'])[0]):
            doubt_overdue = doubt_overdue.append(temp_data)
            index = temp_data[temp_data['contract_id'] == contract][temp_data['download_date'] == '2018-04-30'].index
            cleaned_overdue.drop(index = index, inplace = True)
            print('Удалил ', contract)

cleaned_overdue['target'].fillna(value = cleaned_overdue['overdue_day'].apply(lambda x: x), inplace = True)
cleaned_overdue.to_csv('cleaned_overdue_with_target.csv', index = False)

doubt_overdue = doubt_overdue[doubt_overdue['download_month'] == 4]
doubt_overdue.to_csv('doubt.overdue.csv', index = False)
