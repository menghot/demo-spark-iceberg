package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 */
public class SparkIcebergHiveMetadataN {
    public static void main(String[] args) {

        //SPARK_LOCAL_HOSTNAME=10.100.1.108
        //Add jvm parameters for local development testing
        /*
-Dspark.master=local
-Dspark.sql.catalog.ic=org.apache.iceberg.spark.SparkCatalog
-Dspark.sql.catalog.ic.type=hive
-Dspark.sql.catalog.ic.uri=thrift://10.194.186.216:9083
-Dspark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        */

        //Command to summit spark job
        /*
spark-submit --master spark://10.194.188.93:7077 \
--conf spark.sql.catalog.ic=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.ic.type=hive \
--conf spark.driver.AWS_ACCESS_KEY_ID=DQJY94XTHA4AB96RKX00 --conf spark.driver.AWS_SECRET_KEY=JBjl27Rnuz8H70x2SCQ7BBFdwIr70WgEBK959+GP \
--conf spark.sql.catalog.ic.uri=thrift://10.194.186.216:9083 \
--deploy-mode cluster \
--driver-memory 1g \
--executor-memory 1g --executor-cores 1 \
--class org.example.SparkIcebergHiveMetadataN \
./demo-spark-iceberg-1.0-SNAPSHOT.jar
        */

        SparkSession spark = SparkSession.builder()
                .appName(SparkIcebergHiveMetadataN.class.getName())
                .enableHiveSupport()
                .getOrCreate();


        Dataset<Row> rows = spark.sql("SELECT * FROM ic.simon.ice_person_20w3 limit 10");
        rows.show();

        rows = spark.sql("SELECT * FROM ic.simon.ice_person_20w2 except SELECT * FROM ic.simon.ice_person_20w3");
        rows.show();

        rows = spark.sql("show tables from ic.simon");
        rows.show();

        rows = spark.sql("SELECT * FROM ic.simon.ice_person_20w3 limit 10");
        rows.write().format("iceberg").mode("append").saveAsTable("ic.simon.ice_person_20w3");
        rows = spark.sql("SELECT count(*) FROM ic.simon.ice_person_20w3 limit 10");
        rows.show();

        rows = spark.sql("SELECT * FROM ic.simon.ice_person_20w3 limit 10");
        rows.write().format("iceberg").mode("append").saveAsTable("ic.simon.ice_person_20w_spark");
        rows = spark.sql("SELECT * FROM ic.simon.ice_person_20w_spark limit 10");
        rows.show();

        spark.stop();
    }
}
