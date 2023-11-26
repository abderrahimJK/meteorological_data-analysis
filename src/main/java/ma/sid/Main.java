package ma.sid;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {


        SparkSession ss = SparkSession.builder().appName("Meteo analysis").master("local[*]").getOrCreate();
        ss.sparkContext().setLogLevel("OFF");

        Dataset<Row> dataset = ss.read()
                .option("multiline", true)
                .option("inferSchema", true)
                .csv("src/main/resources/2023.csv.gz");

        Dataset<Row> df = dataset.toDF("ID", "date", "element", "value", "m-flag", "q-flag", "s-flag", "OBS-TIME");
        df.createOrReplaceTempView("data");

        df.describe().show();
        System.out.println("****** Average minimum temperature. ******");
        df.filter(col("element").like("TMIN")).show();

        System.out.println("****** Average maximum temperature. ******");
        df.filter(col("element").like("TMAX")).show();

        System.out.println("****** The maximum temperature. ******");
        df.select(max("value").as("Maximum temp")).show();

        System.out.println("****** The minimum temperature. ******");
        df.select(min("value").as("Minimum temp")).show();

        System.out.println("****** Top 5 hottest weather stations. ******");
        df.sqlContext()
                .sql("Select ID, max(value) as maxTemp from data where element like 'TMAX' group By ID order by maxTemp DESC limit 5")
                .show();

        System.out.println("****** Top 5 coldest weather stations. ******");
        df.sqlContext()
                .sql("Select ID, min(value) as minTemp from data where element like 'TMAX' group By ID order by minTemp DESC limit 5")
                .show();

        ss.stop();
    }
}