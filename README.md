# Сводные данные по опросу врачей на знание и использование препарата
https://aw-demo.ru/public/dashboard/J5WFkijhbi_xZrEB0dGeNdFjjyVqsLWU
## Задача и подготовка данных
От заказчика поступили данные -- результаты опроса персонала. 
Была поставлена задача: привести их к нормальному виду, представить ответы на все вопросы в агрегированном виде.

Сложность задачи состояла в том, данные были ненормализованы и неконсистентны. Попадались поля с свободным выбором категории, множественным выбором, вопросы, где нужно расставить порядок препаратов. 

## Реализация ETL
Реализация была выполнена в рамках Bi-сервиса Analytic Workspace, на демостенде https://aw-demo.ru/
Было реализовано несколько функциональных этапов.
![image](https://github.com/user-attachments/assets/0f182ea8-37bf-467e-9364-33c3f834309f)

### Подключение источника
Источник был облачным файлом на гугл-диске, оттуда данные парсились c использованием python-кода. Каждое поле таблицы представляло собой вопрос опроса и свод ответов на него. 

Получаемыее данные сохранялись в промежуточную таблицу "Опрос (raw)".


```python

import requests
import re
from pyspark.sql import Row
import pandas as pd
from pyspark.sql.functions import split, col
from pyspark.sql.types import StructType, StructField, StringType

def after_load_virtual(df, spark, app, *args, **kwargs):
    # Переменная содержит ссылку на книгу, # данные которой необходимо использовать
    sheet_link = 'ссылка'

    # В ссылке содержатся идентификатор книги и листа.# Забираем их с помощью регулярных выражений.
    sheet_id = re.findall('/spreadsheets/d/([a-zA-Z0-9-_]+)', sheet_link)[0]
    table_id = re.findall('[#&]gid=([0-9]+)', sheet_link)[0]

    # Чтобы забрать данные воспользуемся экспортом в CSV.# Воспользуемся API гугла, встроив идентификаторы в URL.
    download_file_path = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&id={sheet_id}&gid={table_id}'# Прокинем в лог информацию о полученной ссылке. # Поможет, если что-то пойдет не так.print(f'\nПолученная ссылка: {download_file_path}')
    print(download_file_path)

    # Обратимся за данными. В случае ошибки прокинем ее в лог.
    response = requests.get(download_file_path)
    if not response.ok:
        raise Exception(f'\nОшибка URL: {response.url} \/n {response.status_code}: {response.text}')

    # Данные по умолчанию приходят в кодировке 'ISO-8859-1'.# Меняем на привычную кодировку для корректной обработки кириллицы.
    byte_array = response.text.encode(response.encoding)
    decoded_text = byte_array.decode('utf-8')
    # Отладочные логи, чтобы понимать какие данные в итоге получаются.# Если данных очень много можно порезать выводя часть символовprint(f'\nПолученные данные сплошным текстом: {decoded_text[:2000]}')
    # Создаем из текста массив строк
    decoded_lines = decoded_text.split('\r\n')
    # Заголовки определены вручную, как строка данных они не нужны.# Убираем строку с заголовками.
    decoded_lines.pop(0)

    # Создаем массив, из которого далее будет создан dataFrame
    data_frame_rows = []

    # Проходим по всем строкам.# Считаем и выводим каждую
    iterator = 1 
    for line in decoded_lines:
        line = line.replace("Прожестожель", "Прожестожель гель")
        line = line.replace("гель гель", "гель")
        pattern = re.compile(r',(?=(?:[^"]*"[^"]*")*[^"]*$)')

        # Замена запятых на точку с запятой
        line = pattern.sub(';', line)
        print(line)
        data_frame_rows.append(line)
        # data_frame_rows.append(Row(
        # # Сопоставляем столбцы модели данных со столбцами в гугл-таблице# Дополнительно числовые типы явно приводим в число с плавающей точкой.
        # Date = row[11],
        # FormName = row[10],
        # Source = row[5]
        # ))
        iterator += 1

    r = pd.DataFrame(data_frame_rows)
    df = df.union(spark.createDataFrame(r))

    return df
```


### Трансформирование данных
1. На первом этапе строка данных, приянятая из источника, была распарсена. 

```sql
with data as (
    select split(fio, ';') as parts
    from child
) 
select 
parts[0] fio,
parts[2] scenario,
trim(both '"' from parts[7]) known_meds,
trim(both '"' from parts[8]) priorities,
parts[9] c1,
parts[11] c2,
parts[13] c3,
parts[15] c4,
parts[17] c5,
trim(both '"' from parts[18]) terapy_knoledge,
trim(both '"' from parts[19]) professionalism,
trim(both '"' from parts[20]) attention,
trim(both '"' from parts[21]) atitude_to_cooperation,
trim(both '"' from parts[22]) efficiency_interaction,
parts[23] responder_id,
parts[24] mp,
parts[25] phone_number,
parts[26] pin_interviewer,
to_timestamp(parts[27], 'dd.MM.yyyy H:mm:ss') submitted_date,
parts[28] token
from data
```


2. Далее был разворот 5 вопросов, где каждому препарату начислялись отдельные баллы. Я развернула таблицу, создала единую категорию для такого типа вопросов.

```sql
SELECT 
    *
FROM child
LATERAL VIEW STACK(
    7, -- количество столбцов для преобразования в строки
    'Препарат 1', c1,
    'Препарат 2', c2,
    'Препарат 3', c3,
    'Препарат 4', c4,
    'Препарат 0' , c5,
    'Other', null,
    'Ни один из предложенных', null
) AS Category, Score
```

3.  Далее стояла задача разобрать вопрос, где ответом была строка -- последовательность препаратов. При этом порядковый номер препарата определял его приоритет. 


```sql
SELECT * 
from (
    SELECT 
    *,
    posexplode(split(priorities, ', ')) AS (split_index, split_item)
    FROM child
)
WHERE split_item = category
or (category not in  ('Препарат 0',  'Препарат 1', 'Препарат 2', 'Препарат 3', 'Препарат 4' )
and split_index = 0)
```


4. Финальным этапом ETL была обработка вопроса с множественным выбором. Необходимо было расставить баллы на каждый из препаратов иходя из всех вариантов, которые выбрал пользователь. 

```sql
select 
case when category in  ('Препарат 0',  'Препарат 1', 'Препарат 2', 'Препарат 3', 'Препарат 4' , 'Ни один из предложенных' )
then category else null end as meds,
category as meds_all,
case when score != '' then 'Был опыт взаимодействия'
when category in ('Препарат 0',  'Препарат 1', 'Препарат 2', 'Препарат 3', 'Препарат 4' )
then 'Не было опыта взаимодействия' else null end experince_with_meds,
fio,
scenario,
responder_id,
mp,
phone_number,
pin_interviewer,
cast(submitted_date as timestamp),
token,
CAST(split_index AS int) + 1 as priority_index,
CAST(score as int) as expertise_level,

case when array_contains( split(attention, ', '), category) then 1 else 0 end attention_rate,
case when array_contains( split(terapy_knoledge, ', '), category) then 1 else 0 end terapy_knoledge_rate,
case when array_contains( split(professionalism, ', '), category) then 1 else 0 end professionalism_rate,
case when array_contains( split(atitude_to_cooperation, ', '), category) then 1 else 0 end atitude_to_cooperation_rate,
case when array_contains( split(efficiency_interaction, ', '), category) then 1
when  size(array_except( split(efficiency_interaction, ', ') , array(
    'Препарат 0',  'Препарат 1', 'Препарат 2', 'Препарат 3', 'Препарат 4' , 'Препарат 5', 'Препарат 6')') )) > 0  and category = 'Other' then 1 
else 0 end efficiency_interaction_rate,
split(known_meds, ', ') t1,
category t2,
array_contains(array_except( split(known_meds, ', ') , array(
    'Препарат 0',  'Препарат 1', 'Препарат 2', 'Препарат 3', 'Препарат 4' , 'Препарат 5', 'Препарат 6') ) , category) t3, 
case when array_contains(split(known_meds, ', '), category) then 1
    when  size(array_except( split(known_meds, ', ') , array(
   'Препарат 0',  'Препарат 1', 'Препарат 2', 'Препарат 3', 'Препарат 4' , 'Препарат 5', 'Препарат 6') )) > 0  and category = 'Other' then 1 
    else 0 end known_meds_rate

from child 
```


