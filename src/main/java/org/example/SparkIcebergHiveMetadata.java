package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 */
public class SparkIcebergHiveMetadata {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName(SparkIcebergHiveMetadata.class.getName())
                .config("spark.sql.catalog.ic", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.ic.type", "hive")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.ic.uri", "thrift://10.194.186.216:9083")
                .enableHiveSupport()
//                .master("local")
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
