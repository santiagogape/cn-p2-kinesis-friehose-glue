# daily_longest_songs.py

import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    args = getResolvedOptions(sys.argv, ['database', 'table', 'output_path'])
    database = args['database']
    table = args['table']
    output_path = args['output_path']

    sc = SparkContext()
    glueContext = GlueContext(sc)

    logger.info(f"Reading from {database}.{table}")

    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table
    )

    df = dyf.toDF()

    # Derivar SOLO el día desde album_release_date
    df = df.withColumn(
        "day",
        F.substring(F.col("album_release_date"), 9, 2)
    )

    # Window por día (aprovechando year/month ya existentes)
    window_spec = Window.partitionBy(
        "year", "month", "day"
    ).orderBy(F.col("duration_min").desc())

    ranked_df = df.withColumn(
        "rank",
        F.row_number().over(window_spec)
    )

    # Top 10 por día
    top10_df = ranked_df.filter(F.col("rank") <= 10)

    result_df = top10_df.select(
        "track_id",
        "track_name",
        "artist",
        "album",
        "album_id",
        "duration_min",
        "rank",
        "year",
        "month",
        "day"
    )

    logger.info(f"Writing daily longest songs to {output_path}")

    output_dyf = DynamicFrame.fromDF(result_df, glueContext, "output")

    glueContext.write_dynamic_frame.from_options(
        frame=output_dyf,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["year", "month", "day"]
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )

    logger.info("Daily longest songs job completed")

if __name__ == "__main__":
    main()
