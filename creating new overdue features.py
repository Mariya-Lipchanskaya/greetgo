import pandas as pd

df = pd.read_csv('cleaned_overdue_with_target.csv')
all_data = pd.read_csv('overdue_initial.csv')

contracts = list(df['contract_id'].value_counts().index)

def get_max_reduction(vect):
    ''' Функция принимает данные о значении сумм задолженности по определенному контракту 
    (по результатам всех миграций, информация о которых есть в датасете), и возвращает 
    значение максимального уменьшения данной суммы между двумя соседними миграциями
    (максимальную сумму разового взноса в счет погашения задолженности)
    '''
    max_reduction = 0
    prev = vect[0]
    for i in range(1, len(vect)):
        if (vect[i] - prev) > max_reduction:
            max_reduction = vect[i] - prev
        prev = vect[i]
    return max_reduction

def get_num_reduction(vect):
    ''' Функция принимает данные о значении сумм задолженности по определенному контракту 
    (по результатам всех миграций, информация о которых есть в датасете), и возвращает 
    количество уменьшений данной суммы между двумя соседними миграциями
    (количество взносов в счет погашения задолженности)
    '''
    num_reduction = 0
    prev = vect[0]
    for i in range(1, len(vect)):
        if (vect[i] - prev) > 0:
            num_reduction += 1
        prev = vect[i]
    return num_reduction

new_features = pd.DataFrame(columns = ['contract_id', 'download_date', 'max_reduction', 'num_reduction'])

# Вычисляем значение фичей max_reduction (максимальная сумма, внесенная в счет погашения долга)
# и num_reduction (количество платежей в счет погашения долга)
for contract in contracts:    
    temp_data = all_data[all_data['contract_id'] == contract] # все записи по данному контракту
    target_data = df[df['contract_id'] == contract] # записи по данному контракту из итогового датасета
    
    if len(target_data) == 1:
        data = list(temp_data['active_summa_nt'])
        max_reduction = get_max_reduction(data)
        num_reduction = get_num_reduction(data)
        download_date = list(target_data['download_date'])[0]
        row = pd.DataFrame({'contract_id': [contract], 'download_date': [download_date],
                            'max_reduction': [max_reduction], 'num_reduction': [num_reduction]})
        new_features = new_features.append(row)
    else:
        ind = list(target_data.index)
        data = list(temp_data[temp_data['download_date'] <= target_data['download_date'][ind[0]]]['active_summa_nt'])
        max_reduction = get_max_reduction(data)
        num_reduction = get_num_reduction(data)
        download_date = target_data['download_date'][ind[0]]
        row = pd.DataFrame({'contract_id': [contract], 'download_date': [download_date],
                            'max_reduction': [max_reduction], 'num_reduction': [num_reduction]})
        new_features = new_features.append(row)
        
        data = list(temp_data[temp_data['download_date'] > target_data['download_date'][ind[0]]]['active_summa_nt'])
        max_reduction = get_max_reduction(data)
        num_reduction = get_num_reduction(data)
        download_date = target_data['download_date'][ind[1]]
        row = pd.DataFrame({'contract_id': [contract], 'download_date': [download_date],
                            'max_reduction': [max_reduction], 'num_reduction': [num_reduction]})
        new_features = new_features.append(row)

# Определение длительности периода (в днях) с момента проведения последнего платежа в счет погашения долга
df_dates = pd.DataFrame(df[['contract_id', 'download_date', 'last_pay_date']])
df_dates['last_pay_date'] = df_dates['last_pay_date'].apply(lambda x: pd.to_datetime(x))
df_dates['downloaded'] = df_dates['download_date'].apply(lambda x: pd.to_datetime(x))

def get_last_payment(dates):
    '''Функция принимает дату загрузки данных в базу и дату последнего платежа, 
    и возвращает количество дней между датами. Если дата загрузки данных предшествует
    дате последнего платежа, функция возвращает 0.
    '''
    t = (dates['downloaded'] - dates['last_pay_date']).days
    if t < 0:
        return 0
    else:
        return t

def get_target(row):
    '''Для контрактов, по которым нет информации о дате последнего платежа, 
    за количество дней, прошедших после последнего платежа, принимается значение 
    поля overdue_day
    '''
    contract = row['contract_id']
    download_date = row['download_date']
    target = df[df['contract_id'] == contract]
    target = list(target[target['download_date'] == download_date]['overdue_day'])[0]
    return target
    

df_dates['last_payment'] = df_dates.apply(func = (lambda x: get_last_payment(x)), axis = 1)
df_dates['last_payment'].fillna(value = df_dates.apply(func = (lambda x: get_target(x)), axis = 1), inplace = True)

# Добавление в датасет с новыми фичами информации о времени, 
# прошедшем с момента последнего платежа
def get_payment_info(row):
    contract = row['contract_id']
    download_date = row['download_date']
    target = df_dates[df_dates['contract_id'] == contract]
    target = list(target[target['download_date'] == download_date]['last_payment'])[0]
    return target

new_features['last_payment'] = new_features.apply(func = (lambda x: get_payment_info(x)), axis = 1)

new_features['contract_id'] = new_features['contract_id'].apply(lambda x: str(x))
df['contract_id'] = df['contract_id'].apply(lambda x: str(x))
overdue_with_new_features = df.merge(new_features, on = ['contract_id', 'download_date'])
overdue_with_new_features.to_csv('overdue_with_new_features.csv', index = False)

