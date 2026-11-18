from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


spark = SparkSession.builder.appName("ids-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

with open("/app/columns.txt", encoding="utf-8") as f:
    columns = f.read().strip().split(",")

fields = [StructField("ts", StringType())]
for column in columns:
    dtype = (
        LongType()
        if column == "flow_id"
        else StringType()
        if column in ("source_day", "label")
        else DoubleType()
    )
    fields.append(StructField(column, dtype))
input_schema = StructType(fields)

indexer = StringIndexerModel.load("hdfs://namenode:8020/models/label_indexer")
labels = indexer.labels
label_map = F.create_map(
    *[
        item
        for index, label in enumerate(labels)
        for item in (F.lit(float(index)), F.lit(label))
    ]
)
kafka_input = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "network_flows")
    .option("startingOffsets", "latest")
    .load()
)
parsed = (
    kafka_input.select(
        F.from_csv(
            F.col("value").cast("string"), input_schema.simpleString(), {"sep": ","}
        ).alias("row")
    )
    .select("row.*")
    .filter(F.col("flow_id").isNotNull())
)

model = PipelineModel.load("hdfs://namenode:8020/models/rf_ids")
predictions = model.transform(parsed).select(
    "flow_id",
    F.col("ts").alias("event_ts"),
    "source_day",
    label_map[F.col("prediction")].alias("predicted_label"),
    F.col("label").alias("actual_label"),
    F.array_max(vector_to_array(F.col("probability"))).alias("confidence"),
)

hdfs_query = (
    predictions.writeStream.format("csv")
    .option("path", "hdfs://namenode:8020/data/results")
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/ids-results")
    .option("header", "false")
    .outputMode("append")
    .start()
)

kafka_output = predictions.select(
    F.col("flow_id").cast("string").alias("key"),
    F.to_json(
        F.struct(
            "flow_id",
            "event_ts",
            "source_day",
            "predicted_label",
            "actual_label",
            "confidence",
        )
    ).alias("value"),
)
kafka_query = (
    kafka_output.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "alerts")
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/ids-alerts")
    .outputMode("append")
    .start()
)

print("STREAM_READY: HDFS results and Kafka alerts queries started", flush=True)
spark.streams.awaitAnyTermination()
