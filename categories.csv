from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("WordCount") \
        .getOrCreate()
    
    # Baca file teks
    text_file = spark.read.text("path/to/book.txt").rdd
    
    # Hitung jumlah kemunculan setiap kata
    counts = text_file.flatMap(lambda line: line.value.split(" ")) \
                      .map(lambda word: (word, 1)) \
                      .reduceByKey(lambda a, b: a + b)
    
    # Simpan hasil
    counts.saveAsTextFile("path/to/output")

    spark.stop()

if __name__ == "__main__":
    main()
