package javasamples;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.feature.VectorAssembler;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.util.List;

/**
 * Graphics with JFreeChart
 */
public class RegressionJFreeChart {

    private final static String RIDES_PATH = String.format("file://%s/duocar/clean/rides", System.getProperty("user.dir"));

    public static void plot(double[] features, double[] labels, String xLabel, String yLabels, String graphLabel) {
        assert(features != null && labels != null && features.length == labels.length);
        XYSeriesCollection dataset = new XYSeriesCollection();
        XYSeries series = new XYSeries("Data");
        for (int i=0; i<features.length; i++) {
            series.add(features[i], labels[i]);
        }
        dataset.addSeries(series);
        final JFreeChart chart = ChartFactory.createScatterPlot(
                graphLabel,
                xLabel,
                yLabels,
                dataset);
        final ChartPanel panel = new ChartPanel(chart);
        final JFrame f = new JFrame();
        f.add(panel);
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        f.pack();
        f.setVisible(true);
    }

    private static double[] rddIntToDoubleArray(JavaRDD<Row> jRdd) {
        JavaDoubleRDD javaDoubleRDD = jRdd.mapToDouble(d -> d.getInt(0));
        List<Double> doubleList = javaDoubleRDD.collect();
        double[] doubles = doubleList.stream().mapToDouble(d -> d).toArray();
        return doubles;
    }
    private static double[] rddDoubleToDoubleArray(JavaRDD<Row> jRdd) {
        JavaDoubleRDD javaDoubleRDD = jRdd.mapToDouble(d -> d.getDouble(0));
        List<Double> doubleList = javaDoubleRDD.collect();
        double[] doubles = doubleList.stream().mapToDouble(d -> d).toArray();
        return doubles;
    }

    public static void main(String... args) {
        System.out.println(String.format("Running Java version [%s]", System.getProperty("java.version")));
        SparkSession spark = SparkSession.builder()
                .appName("regress")
                .config("spark.master", "local")
                .getOrCreate();
        Dataset<Row> rides = spark.read()
                .parquet(RIDES_PATH);
        System.out.println(String.format("Count: %d", rides.count()));
        rides.printSchema();

        Dataset<Row> regressionData = rides.select("distance", "duration").filter("cancelled = 0");
        regressionData.show(5);

        // Display data
        Dataset<Row> sample;
        boolean doSample = false;
        if (doSample) {
            sample = regressionData.sample(false, 0.5, 12345);
        } else {
            sample = regressionData;
        }

        double[] x = rddIntToDoubleArray(sample.select("distance").javaRDD());
        double[] y = rddIntToDoubleArray(regressionData.select("duration").javaRDD());
        plot(x, y, "Distances (m)", "Durations (s)", "Regression - before");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[] {"distance"})
                .setOutputCol("features");

        Dataset<Row> assembled = assembler.transform(regressionData);
        assembled.printSchema();
        assembled.show(5);

        // Create train (0) and test (1) data sets
        Dataset<Row>[] randomSplit = assembled.randomSplit(new double[] {0.7, 0.3}, 23451);

        LinearRegression linearRegression = new LinearRegression().setFeaturesCol("features").setLabelCol("duration");
        System.out.println(linearRegression.explainParams());

        LinearRegressionModel model = linearRegression.fit(randomSplit[0]);
        double intercept = model.intercept();
        Vector coefficients = model.coefficients();
        double[] coeffs = coefficients.toArray();
        System.out.println("Intercept:" + intercept);
        System.out.println(String.format("-- %d Coefficient(s): --", coeffs.length));
        for (double d : coeffs) {
            System.out.println(d);
        }
        System.out.println("-----------------------");

        // ...

        // Applying the model, making predictions
        Dataset<Row> predictions = model.transform(randomSplit[1]);
        predictions.printSchema();
        predictions.show(5);

        double[] xPred = rddIntToDoubleArray(predictions.select("distance").javaRDD());
        double[] yPred = rddDoubleToDoubleArray(predictions.select("prediction").javaRDD());
        plot(xPred, yPred, "Distances (n)", "Durations (s)", "Regression - after");

        System.out.println("Done!");
        spark.close();
    }
}
