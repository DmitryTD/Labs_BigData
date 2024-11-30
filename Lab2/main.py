from pyspark import SparkContext, SparkConf
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import re

conf = SparkConf().setAppName("TextAnalysis").setMaster("local[*]")
sc = SparkContext(conf=conf)

nltk.download('stopwords')
nltk.download('punkt')

file_path = "./text.txt"

text_rdd = sc.textFile(file_path)

def clean_text(line):
    line = re.sub(r'[^a-zA-Z\s]', '', line.lower())
    words = line.split()
    return words

cleaned_rdd = text_rdd.flatMap(clean_text)

stop_words = set(stopwords.words('english'))
cleaned_rdd = cleaned_rdd.filter(lambda word: word not in stop_words)

word_count_rdd = cleaned_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

sorted_word_count = word_count_rdd.collect()
sorted_word_count.sort(key=lambda x: x[1], reverse=True)

top_50 = sorted_word_count[:50]
least_50 = sorted_word_count[-50:]

print("Top 50 Most Common Words:")
for word, count in top_50:
    print(f"{word}: {count}")

print("\nTop 50 Least Common Words:")
for word, count in least_50:
    print(f"{word}: {count}")

stemmer = PorterStemmer()

def stem_words(word):
    return stemmer.stem(word)

stemmed_rdd = cleaned_rdd.map(stem_words)

word_count_stemmed_rdd = stemmed_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
sorted_word_count_stemmed = word_count_stemmed_rdd.collect()
sorted_word_count_stemmed.sort(key=lambda x: x[1], reverse=True)

top_50_stemmed = sorted_word_count_stemmed[:50]
least_50_stemmed = sorted_word_count_stemmed[-50:]

print("\nTop 50 Most Common Words After Stemming:")
for word, count in top_50_stemmed:
    print(f"{word}: {count}")

print("\nTop 50 Least Common Words After Stemming:")
for word, count in least_50_stemmed:
    print(f"{word}: {count}")

sc.stop()
