# monthly_duration_time_aggregation.py

import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
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

    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table
    )

    df = dyf.toDF()

    agg_df = df.groupBy("year", "month").agg(
        F.sum("duration_min").alias("total_duration_min")
    )

    result_df = agg_df.withColumn(
        "total_duration_sec",
        F.col("total_duration_min") * 60
    ).withColumn(
        "total_duration_hours",
        F.col("total_duration_min") / 60
    ).orderBy("year", "month")

    output_dyf = DynamicFrame.fromDF(result_df, glueContext, "output")

    glueContext.write_dynamic_frame.from_options(
        frame=output_dyf,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["year", "month"]
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )

    logger.info("Monthly aggregation completed")

if __name__ == "__main__":
    main()
