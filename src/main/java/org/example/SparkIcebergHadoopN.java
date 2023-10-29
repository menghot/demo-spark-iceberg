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

public class SparkIcebergHadoopN {


    /* Add jvm args for local develop testing
-Dspark.master=local
-Dspark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog
-Dspark.sql.catalog.spark_catalog.type=hadoop
-Dspark.sql.catalog.spark_catalog.warehouse=hdfs://node-10-194-186-216:8020/user/hive/warehouse
    */


    /* Command to summit spark job
spark-submit --master spark://10.194.188.93:7077 \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.spark_catalog.type=hadoop \
--conf spark.sql.catalog.spark_catalog.warehouse=hdfs://node-10-194-186-216:8020/user/hive/warehouse \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 1g --executor-cores 1 \
--class org.example.SparkIcebergHadoop \
./demo-spark-iceberg-1.0-SNAPSHOT.jar

spark-submit --master spark://10.194.188.93:7077 \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.spark_catalog.type=hadoop \
--conf spark.sql.catalog.spark_catalog.warehouse=hdfs://node-10-194-186-216:8020/user/hive/warehouse \
--deploy-mode cluster \
--driver-memory 1g \
--executor-memory 1g --executor-cores 1 \
--class org.example.SparkIcebergHadoop \
hdfs://10.194.186.216:8020/tmp/demo-spark-iceberg-1.0-SNAPSHOT.jar
    */

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName(SparkIcebergHadoopN.class.getName())
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
