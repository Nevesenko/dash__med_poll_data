# Сводные данные по опросу врачей на знание и использование препарата

## Задача и подготовка данных
От заказчика поступили данные -- результаты опроса персонала. 
Была поставлена задача: привести их к нормальному виду, представить ответы на все вопросы в агрегированном виде.

## Реализация ETL
Реализация была выполнена в рамках Bi-сервиса Analytic Workspace, на демостенде https://aw-demo.ru/
Было реализовано несколько функциональных этапов.
![image](https://github.com/user-attachments/assets/0f182ea8-37bf-467e-9364-33c3f834309f)

### Подключение источника
Источник был облачным файлом на гугл-диске, оттуда данные парсились c использованием python-кода. Полученные данные сохранялись в промежуточную таблицу "Опрос (raw)".
<details><summary>Код</summary>
```
import requests
import re
from pyspark.sql import Row
import pandas as pd
from pyspark.sql.functions import split, col
from pyspark.sql.types import StructType, StructField, StringType

def after_load_virtual(df, spark, app, *args, **kwargs):
    # Переменная содержит ссылку на книгу, # данные которой необходимо использовать
    sheet_link = 'https://docs.google.com/spreadsheets/d/1_HFNxtpC07OQ2E3bVpVlrzMjS1MksKgtApa0YpsEquk/edit?gid=1033252738#gid=1033252738'

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


</details>

