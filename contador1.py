import sys
from pyspark import SparkContext, SparkConf
import pandas as pd
if __name__ == "__main__":
    sc = SparkContext("local","PySpark Exemplo - Desafio Dataproc10")
    words = sc.textFile("gs://diodataproc/livro.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=False)
    wordCounts = pd.DataFrame(wordCounts.take(10))
    wordCounts.to_csv("gs://diodataproc/resultado1/RESULTADO.TXT",header=False, index = False)
    #wordCounts.saveAsTextFile("gs://diodataproc/resultado1")
