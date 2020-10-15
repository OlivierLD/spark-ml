package javasamples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.feature.VectorAssembler;

public class Regression {

    private final static String RIDES_PATH = String.format("file://%s/duocar/clean/rides", System.getProperty("user.dir"));

    public static void main(String... args) {
        SparkSession spark = SparkSession.builder()
                .appName("regress")
                .config("spark.master", "local")
                .getOrCreate();
        Dataset<Row> rides = spark.read()
                .parquet(RIDES_PATH);
        System.out.println(String.format("Count: %d", rides.count()));
        rides.printSchema();

        Dataset<Row> regressionData = rides.select("distance", "duration").filter("cancelled = 0");
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[] {"distance"})
                .setOutputCol("features");


        System.out.println("Done!");
        spark.close();
    }
}
