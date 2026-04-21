import os
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    BooleanType
)
from utils import build_spark_session, get_base_paths


FLOWS_SCHEMA = StructType([
    StructField("zone", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("updatedAt", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("fossilFreePercentage", IntegerType(), True),
    StructField("renewablePercentage", IntegerType(), True),
    StructField("powerConsumptionTotal", LongType(), True),
    StructField("powerProductionTotal", LongType(), True),
    StructField("powerImportTotal", LongType(), True),
    StructField("powerExportTotal", LongType(), True),
    StructField("isEstimated", BooleanType(), True),
    StructField("estimationMethod", StringType(), True),
    StructField("temporalGranularity", StringType(), True),

    StructField("consumption_nuclear", LongType(), True),
    StructField("consumption_geothermal", LongType(), True),
    StructField("consumption_biomass", LongType(), True),
    StructField("consumption_coal", LongType(), True),
    StructField("consumption_wind", LongType(), True),
    StructField("consumption_solar", LongType(), True),
    StructField("consumption_hydro", LongType(), True),
    StructField("consumption_gas", LongType(), True),

    StructField("production_nuclear", LongType(), True),
    StructField("production_geothermal", LongType(), True),
    StructField("production_biomass", LongType(), True),
    StructField("production_coal", LongType(), True),
    StructField("production_wind", LongType(), True),
    StructField("production_solar", LongType(), True),
    StructField("production_hydro", LongType(), True),
    StructField("production_gas", LongType(), True),
])

MIX_SCHEMA = StructType([
    StructField("zone", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("updatedAt", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("carbonIntensity", IntegerType(), True),
    StructField("isEstimated", BooleanType(), True),
    StructField("estimationMethod", StringType(), True),
])


def safe_long(value):
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def load_bronze_files(base_path, stream):
    records = []
    path = os.path.join(base_path, stream)

    if not os.path.exists(path):
        return records

    folders = sorted(os.listdir(path), reverse=True)
    if not folders:
        return records

    latest_folder = os.path.join(path, folders[0])

    for root, _, files in os.walk(latest_folder):
        for f in files:
            if f.endswith(".json"):
                with open(os.path.join(root, f), "r") as file:
                    records.append(json.load(file))

    return records


def process_flows(records, spark):
    rows = []

    for r in records:
        data = r.get("data", {})
        if not data:
            continue

        cb = data.get("powerConsumptionBreakdown", {}) or {}
        pb = data.get("powerProductionBreakdown", {}) or {}

        rows.append({
            "zone": data.get("zone"),
            "datetime": data.get("datetime"),
            "updatedAt": data.get("updatedAt"),
            "ingestion_timestamp": r.get("ingestion_timestamp"),
            "source_url": r.get("source_url"),
            "fossilFreePercentage": data.get("fossilFreePercentage"),
            "renewablePercentage": data.get("renewablePercentage"),
            "powerConsumptionTotal": safe_long(data.get("powerConsumptionTotal")),
            "powerProductionTotal": safe_long(data.get("powerProductionTotal")),
            "powerImportTotal": safe_long(data.get("powerImportTotal")),
            "powerExportTotal": safe_long(data.get("powerExportTotal")),
            "isEstimated": data.get("isEstimated"),
            "estimationMethod": data.get("estimationMethod"),
            "temporalGranularity": data.get("temporalGranularity"),

            "consumption_nuclear": safe_long(cb.get("nuclear")),
            "consumption_geothermal": safe_long(cb.get("geothermal")),
            "consumption_biomass": safe_long(cb.get("biomass")),
            "consumption_coal": safe_long(cb.get("coal")),
            "consumption_wind": safe_long(cb.get("wind")),
            "consumption_solar": safe_long(cb.get("solar")),
            "consumption_hydro": safe_long(cb.get("hydro")),
            "consumption_gas": safe_long(cb.get("gas")),

            "production_nuclear": safe_long(pb.get("nuclear")),
            "production_geothermal": safe_long(pb.get("geothermal")),
            "production_biomass": safe_long(pb.get("biomass")),
            "production_coal": safe_long(pb.get("coal")),
            "production_wind": safe_long(pb.get("wind")),
            "production_solar": safe_long(pb.get("solar")),
            "production_hydro": safe_long(pb.get("hydro")),
            "production_gas": safe_long(pb.get("gas")),
        })

    if not rows:
        return spark.createDataFrame([], FLOWS_SCHEMA)

    df = spark.createDataFrame(rows, schema=FLOWS_SCHEMA)

    return (
        df.withColumn("data_datetime", F.to_timestamp("datetime"))
          .withColumn("year", F.year("data_datetime").cast("string"))
          .withColumn("month", F.lpad(F.month("data_datetime").cast("string"), 2, "0"))
          .withColumn("day", F.lpad(F.dayofmonth("data_datetime").cast("string"), 2, "0"))
          .drop("datetime", "ingestion_timestamp")
          .dropDuplicates(["zone", "data_datetime"])
    )


def process_mix(records, spark):
    rows = []

    for r in records:
        data = r.get("data", {})
        if not data:
            continue

        rows.append({
            "zone": data.get("zone"),
            "datetime": data.get("datetime"),
            "updatedAt": data.get("updatedAt"),
            "ingestion_timestamp": r.get("ingestion_timestamp"),
            "source_url": r.get("source_url"),
            "carbonIntensity": data.get("carbonIntensity"),
            "isEstimated": data.get("isEstimated"),
            "estimationMethod": data.get("estimationMethod"),
        })

    if not rows:
        return spark.createDataFrame([], MIX_SCHEMA)

    df = spark.createDataFrame(rows, schema=MIX_SCHEMA)

    return (
        df.withColumn("data_datetime", F.to_timestamp("datetime"))
          .withColumn("year", F.year("data_datetime").cast("string"))
          .withColumn("month", F.lpad(F.month("data_datetime").cast("string"), 2, "0"))
          .withColumn("day", F.lpad(F.dayofmonth("data_datetime").cast("string"), 2, "0"))
          .drop("datetime", "ingestion_timestamp")
          .dropDuplicates(["zone", "data_datetime"])
    )


def validate_flows(df):
    return (
        df.filter(df.zone.isNotNull())
          .filter(df.data_datetime.isNotNull())
          .filter(df.powerProductionTotal > 0)
    )


def validate_mix(df):
    return (
        df.filter(df.zone.isNotNull())
          .filter(df.data_datetime.isNotNull())
    )


def write_parquet(df, path):
    os.makedirs(path, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_parquet(os.path.join(path, "data.parquet"), index=False)


def run_silver():
    spark = build_spark_session()
    paths = get_base_paths()

    flows = load_bronze_files(paths["bronze"], "electricity_flows")
    if flows:
        df = process_flows(flows, spark)
        df = validate_flows(df)
        write_parquet(df, os.path.join(paths["silver"], "electricity_flows", "parquet"))

    mix = load_bronze_files(paths["bronze"], "electricity_mix")
    if mix:
        df = process_mix(mix, spark)
        df = validate_mix(df)
        write_parquet(df, os.path.join(paths["silver"], "electricity_mix", "parquet"))

    spark.stop()


if __name__ == "__main__":
    run_silver()