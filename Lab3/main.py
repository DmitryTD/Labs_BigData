from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns

# Инициализация Spark сессии
spark = SparkSession.builder.appName("Popular Baby Names Analysis").getOrCreate()

def load_data(file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    # Приведение столбца Count к IntegerType
    from pyspark.sql.functions import col
    df = df.withColumn("Count", col("Count").cast("int"))
    return df

def initial_analysis(df):
    print("Schema:")
    df.printSchema()
    print("\nКоличество записей:")
    print(df.count())
    print("\nПервые 5 строк:")
    df.show(5)

def describe_data(df):
    print("\nОписание данных:")
    df.describe().show()

def unique_values(df):
    for column in df.columns:
        unique_count = df.select(column).distinct().count()
        print(f"Уникальных значений в {column}: {unique_count}")

def visualize_names_by_year(df):
    pdf = df.groupBy('Year of Birth').sum('Count').toPandas()
    pdf['sum(Count)'] = pdf['sum(Count)'] / pdf['sum(Count)'].max()  # Нормализация
    plt.figure(figsize=(12, 6))
    sns.barplot(x='Year of Birth', y='sum(Count)', data=pdf)
    plt.title('Количество имен по годам (нормализовано)')
    plt.xticks(rotation=45)
    plt.show()

def visualize_names_by_gender(df):
    pdf_gender = df.groupBy('Gender').sum('Count').toPandas()
    pdf_gender['sum(Count)'] = pdf_gender['sum(Count)'] / pdf_gender['sum(Count)'].sum()  # Нормализация по полу
    plt.figure(figsize=(8, 6))
    sns.barplot(x='Gender', y='sum(Count)', data=pdf_gender)
    plt.title('Распределение имен по полу (доля)')
    plt.show()

def top_10_names(df):
    pdf_top10 = df.groupBy("Child's First Name").sum('Count').orderBy('sum(Count)', ascending=False).limit(10).toPandas()
    print("Топ-10 имен по популярности:")
    print(pdf_top10)

def name_trend(df, name_to_check):
    pdf_name_trend = df.filter(df["Child's First Name"] == name_to_check).groupBy('Year of Birth').sum('Count').toPandas()
    plt.figure(figsize=(12, 6))
    sns.lineplot(x='Year of Birth', y='sum(Count)', data=pdf_name_trend, marker='o')
    plt.title(f'Динамика популярности имени {name_to_check}')
    plt.xticks(rotation=45)
    plt.show()


def rare_names_analysis(df):
    pdf_rare = df.groupBy("Child's First Name").sum('Count').filter('`sum(Count)` <= 10').toPandas()
    print("Редкие имена (менее 10 случаев):")
    print(pdf_rare)

# Главная функция

def main():
    file_path = './Lab3/Popular_Baby_Names.csv'
    df = load_data(file_path)
    initial_analysis(df)
    describe_data(df)
    unique_values(df)
    
    visualize_names_by_year(df)
    visualize_names_by_gender(df)
    
    top_10_names(df)
    name_trend(df, 'Michael')
    rare_names_analysis(df)

if __name__ == "__main__":
    main()
