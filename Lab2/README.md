Установка зависимостей
Создайте виртуальное окружение (опционально):

```
python -m venv .venv
```
Активируйте виртуальное окружение:

```
source .venv/bin/activate
```

Установите необходимые Python библиотеки:
```
pip install pyspark nltk
```

Запуск скрипта
```
spark-submit main.py
```