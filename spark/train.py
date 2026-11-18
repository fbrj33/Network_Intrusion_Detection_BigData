import math
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = (SparkSession.builder.appName("train").master("local[3]")
         .config("spark.sql.shuffle.partitions", "12").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

cols = open("/app/columns.txt").read().strip().split(",")
feats = [c for c in cols if c not in ("flow_id", "source_day", "label")]

def schema(with_ts):
    fields = [StructField("ts", StringType())] if with_ts else []
    for c in cols:
        t = LongType() if c == "flow_id" else StringType() if c in ("source_day", "label") else DoubleType()
        fields.append(StructField(c, t))
    return StructType(fields)

b = spark.read.csv("hdfs://namenode:8020/data/raw/flows_batch", schema=schema(False))
a = (spark.read.csv("hdfs://namenode:8020/data/raw/logs/flows/*/*/flows.*", schema=schema(True))
     .drop("ts"))
df = a.unionByName(b).select(*feats, F.trim("label").alias("label")).dropna()
print("ROWS_TOTAL", df.count())

# downsample the majority class (benign); keep all attacks
labels = [r[0] for r in df.select("label").distinct().collect()]
fr = {l: (0.15 if l.upper() == "BENIGN" else 1.0) for l in labels}
df = df.sampleBy("label", fr, seed=42).cache()
print("ROWS_SAMPLED", df.count())

indexer = StringIndexer(inputCol="label", outputCol="label_idx").fit(df)
df = indexer.transform(df)

# class weights (square root keeps very rare classes from dominating)
counts = {r["label_idx"]: r["count"] for r in df.groupBy("label_idx").count().collect()}
total, k = sum(counts.values()), len(counts)
mapping = F.create_map(*[x for i, c in counts.items()
                         for x in (F.lit(float(i)), F.lit(math.sqrt(total / (k * c))))])
df = df.withColumn("w", mapping[F.col("label_idx")])

train, test = df.randomSplit([0.8, 0.2], seed=42)
train.cache()
print("TRAIN_ROWS", train.count())
pipe = Pipeline(stages=[
    VectorAssembler(inputCols=feats, outputCol="features", handleInvalid="skip"),
    RandomForestClassifier(featuresCol="features", labelCol="label_idx", weightCol="w",
                           numTrees=20, maxDepth=10, maxBins=32, seed=42)])
model = pipe.fit(train)
pred = model.transform(test).select("label_idx", "prediction").cache()

ev = MulticlassClassificationEvaluator(labelCol="label_idx", predictionCol="prediction")
for m in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
    print("METRIC", m, round(ev.setMetricName(m).evaluate(pred), 4))

conf = {(r["label_idx"], r["prediction"]): r["count"]
        for r in pred.groupBy("label_idx", "prediction").count().collect()}
for i, l in enumerate(indexer.labels):
    fi = float(i)
    tp = conf.get((fi, fi), 0)
    p = sum(v for (x, y), v in conf.items() if y == fi)
    r = sum(v for (x, y), v in conf.items() if x == fi)
    prec, rec = tp / max(p, 1), tp / max(r, 1)
    f1 = 2 * prec * rec / max(prec + rec, 1e-9)
    print("CLASS %-32s support=%-7d precision=%.3f recall=%.3f f1=%.3f" % (l, r, prec, rec, f1))

imp = model.stages[-1].featureImportances
top = sorted(zip(feats, imp.toArray()), key=lambda x: -x[1])[:10]
for n, v in top:
    print("IMPORTANCE", n, round(float(v), 4))

model.write().overwrite().save("hdfs://namenode:8020/models/rf_ids")
indexer.write().overwrite().save("hdfs://namenode:8020/models/label_indexer")
print("MODEL SAVED")
train.unpersist()
df.unpersist()