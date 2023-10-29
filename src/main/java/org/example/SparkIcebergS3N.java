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

public class SparkIcebergS3N {

    /*
-Dspark.master=local
-Dspark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
-Dspark.hadoop.fs.s3a.endpoint=http://10.194.188.93:9020
-Dspark.hadoop.fs.s3a.access.key=DQJY94XTHA4AB96RKX00
-Dspark.hadoop.fs.s3a.secret.key=JBjl27Rnuz8H70x2SCQ7BBFdwIr70WgEBK959+GP
-Dspark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
-Dspark.sql.catalog.spark_catalog.type=hadoop
-Dspark.sql.catalog.spark_catalog.warehouse=s3a://iceberg/warehouse
  */

    //Command to summit spark job
    /*
spark-submit --master spark://10.194.188.93:7077 \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.endpoint=hadoop \
--conf spark.hadoop.fs.s3a.access.key=DQJY94XTHA4AB96RKX00 \
--conf spark.hadoop.fs.s3a.secret.key=JBjl27Rnuz8H70x2SCQ7BBFdwIr70WgEBK959+GP \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
--conf spark.sql.catalog.spark_catalog.type=hadoop \
--conf spark.sql.catalog.spark_catalog.warehouse=s3a://iceberg/warehouse \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 1g --executor-cores 1 \
--class org.example.SparkIcebergS3 \
./demo-spark-iceberg-1.0-SNAPSHOT.jar
    */
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName(SparkIcebergS3N.class.getName())
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

        spark.sql("create schema simon");

        // Write DataFrame to Iceberg table
        df.write().format("iceberg").mode("append").saveAsTable("simon.my_table");

        Dataset<Row> dataset = spark.sql("select * from spark_catalog.simon.my_table");

        dataset.show();
    }
}
