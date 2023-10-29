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

public class SparkIcebergHadoop {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName(SparkIcebergHadoop.class.getName())
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://node-10-194-186-216:8020/user/hive/warehouse")
                .master("local")
                .getOrCreate();

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

        df.show();

        //spark.sql("create schema spark_catalog.simon");

        df.write().format("iceberg").mode("append").partitionBy("name").saveAsTable("simon.my_table3");

        Dataset<Row> newDf = spark.sql("select * from spark_catalog.simon.my_table3");

        newDf.show();
    }
}
