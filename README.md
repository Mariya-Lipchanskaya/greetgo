# Описание файлов:
## Файлы с кодом:

<p><b><i>Имя файла:</i></b> get client data from json files and preprocessing.ipynb
<p><b><i>Описание:</i></b> обирает из json-файлов общие данные о клиентах
<p><b><i>Результат:</i></b> client_data_cleaned.csv - общая информация по клиентам
<br><br>
 
<p><b><i>Имя файла:</i></b> Credit.ipynb
<p><b><i>Описание:</i></b> собирает из json-файлов общие данные обо всех контрактах
<p><b><i>Результат:</i></b> credit_cleaned.csv - общие сведения о контрактах 
<br><br>
  
<b><i>Имя файла:</i></b> creating cleaned overdue dataset.py<p>
<b><i>Описание:</i></b> собирает из json-файлов данные о контрактах, имеющих просрочку; вычисляет значение target-переменной; формирует итоговый датасет с данными по контрактам, вышедшим из просрочки.<p>
<b><i>Результат:</i></b> 
  <br>overdue_initial.csv - результат парсинга json-файлов.
  <br>cleaned_overdue_with_target.csv - очищенный файл с данными по overdue, включая target-переменную.
  <br>doubt.overdue.csv - данные по клиентам, для которых невозможно получить target-переменную.<p>
<br>

<p><b><i>Имя файла:</i></b> creating new overdue features.py
<p><b><i>Описание:</i></b> создание дополнительных фичей, касающихся частоты внесения средств в счет погашения долга, максимальной суммы, внесенной в счет погашения долга, а также времени (в днях), прошедшем после последней оплаты.
<p><b><i>Результат:</i></b> overdue_with_new_features.csv - итоговые данные по overdue, которые будут включены в датасет, используемый при обучении и тестировании модели
<br><br>

<p><b><i>Имя файла:</i></b> Collateral.ipynb
<p><b><i>Описание:</i></b> собирает из json-файлов данные о контрактах, выданных под залог
<p><b><i>Результат:</i></b> collaterals_optimized.csv - сведения о контрактах по кредитам, выданным под залог и информация о залоге.
<br><br>

<p><b><i>Имя файла:</i></b> Final data generation.ipynb
<p><b><i>Описание:</i></b> Объединяет датасеты, содержащие сведения о клиентах, контрактах, просрочке и залоге в единый датасет; генерирует дополнительные фичи, для вычисления которых необходима была информация из разных датасетов.
<p><b><i>Результат:</i></b> data_for_model.csv - итоговый датасет для обучения и тестирования модели
<br>
<br>

<p><b><i>Имя файла:</i></b> RFR for data_for_model final.ipynb
<p><b><i>Описание:</i></b> создает модель зависимости срока погашения долга по кредиту от демографических, финансовых и поведенческих характеристик клиента (на основе алгоритма Random Forest)
<p><b><i>Результат:</i></b> модель и метрики для оценки ее точности
<br><br>

## CSV-файлы:

<p><b><i>Имя файла:</i></b> data_for_model.csv
<p><b><i>Описание:</i></b> Основной файл, используемый для тренировки и тестирования модели.
<p><b><i>Количество строк и столбцов:</i></b> 76 870 строк, 62 столбца.
<br><br>

<p><b><i>Имя файла:</i></b> client_data_cleaned.csv
<p><b><i>Описание:</i></b> общие сведения о клиентах.
<p><b><i>Количество строк и столбцов:</i></b> 43884 строк, 4 столбца.
<br><br>

<p><b><i>Имя файла:</i></b> credit_cleaned.csv
<p><b><i>Описание:</i></b> общие сведения о контрактах.
<p><b><i>Количество строк и столбцов:</i></b> 45147 строк, 36 столбцов.
<br><br>

<p><b><i>Имя файла:</i></b> overdue_initial.csv
<p><b><i>Описание:</i></b> содержит данные по всем контрактам, имеющим просрочку (по результатам всех миграций) .
<p><b><i>Количество строк и столбцов:</i></b> 267784 строк, 22 столбца.
<br><br>

<p><b><i>Имя файла:</i></b> cleaned_overdue_with_target.csv
<p><b><i>Описание:</i></b> содержит данные по всем контрактам, имевшим просрочку и либо погасившим долг, либо имеющим просрочку более 365 дней на 30 апреля 2018 года (результаты последней миграции, при которой данные по контракту попадают в базу). Для всех контрактов вычислена target-переменная: для погасивших долг она равна значению поля "overdue_day" на момент последнего появления в базе, для имевших задолженность более года на 30 апреля 2018 года значение target-переменной равно -1.
<p><b><i>Количество строк и столбцов:</i></b> 76870 строк, 16 столбцов.
<br><br>

<p><b><i>Имя файла:</i></b> overdue_with_new_features.csv
<p><b><i>Описание:</i></b> К данным из файла "cleaned_overdue_with_target.csv" добавлены 3 фичи, содержащие поведенческие характеристики клиентов: 
  <br>1. max_reduction - максимальная сумма разового взноса в счет погашения задолженности.
  <br>2. num_reduction - количество взносов в счет погашения задолженности.
  <br>3. last_payment - количество дней, прошедших с даты последнего платежа в счет погашения долга.
<p><b><i>Количество строк и столбцов:</i></b> 76870 строк, 19 столбцов.
<br><br>

<p><b><i>Имя файла:</i></b> collaterals_optimized.csv
<p><b><i>Описание:</i></b> сведения о кредитах, выданных под залог и информация о залоге и созаемщиках
<p><b><i>Количество строк и столбцов:</i></b> 9546 строк, 16 столбцов
<br><br>

<p><b><i>Имя файла:</i></b> data_for_model.csv
<p><b><i>Описание:</i></b> сведения о контрактах, имеющих непогашенную задолженность на 30 апреля 2018 года, срок просрочки которых не превышает 1 года (результаты миграции от 30.04.2018)
<p><b><i>Количество строк и столбцов:</i></b> 76870 строк, 63 столбца
<br><br>

## Модель
<p><b><i>Имя файла:</i></b> model.pkl
 <p><b><i>Описание:</i></b> Параметры модели для предсказания срока погашения кредита в зависимости от демографических, финансовых и поведенческих характеристик клиента.
