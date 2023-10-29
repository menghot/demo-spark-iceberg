package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class SparkIcebergS3 {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName(SparkIcebergS3.class.getName())
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.endpoint", "http://10.194.188.93:9020")
                .config("spark.hadoop.fs.s3a.access.key", "DQJY94XTHA4AB96RKX00")
                .config("spark.hadoop.fs.s3a.secret.key", "JBjl27Rnuz8H70x2SCQ7BBFdwIr70WgEBK959+GP")

                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg/warehouse")
                .config("spark.master", "local")

                .getOrCreate();

//        spark.sql.catalog.spark_catalog.hadoop.fs.s3a.endpoint = http://aws-local:9000

        // Create a sample DataFrame
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "John"),
                RowFactory.create(2, "Jane")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        spark.sql("create schema simon");

        // Write DataFrame to Iceberg table
        df.write().format("iceberg").mode("append").saveAsTable("simon.my_table");

        Dataset<Row> dataset = spark.sql("select * from spark_catalog.simon.my_table");

        dataset.show();
    }
}
