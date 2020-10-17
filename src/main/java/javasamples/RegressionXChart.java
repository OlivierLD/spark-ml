package javasamples;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.Styler;

import java.awt.*;
import java.util.Arrays;
import java.util.List;

/**
 * Graphics with XChart
 * See https://knowm.org/open-source/xchart/xchart-example-code/
 */
public class RegressionXChart {

    private final static String RIDES_PATH = String.format("file://%s/duocar/clean/rides", System.getProperty("user.dir"));

    private static List<Double>  rddIntToDoubleArray(JavaRDD<Row> jRdd) {
        JavaDoubleRDD javaDoubleRDD = jRdd.mapToDouble(d -> d.getInt(0));
        List<Double> doubleList = javaDoubleRDD.collect();
        return doubleList;
    }
    private static List<Double> rddDoubleToDoubleArray(JavaRDD<Row> jRdd) {
        JavaDoubleRDD javaDoubleRDD = jRdd.mapToDouble(d -> d.getDouble(0));
        List<Double> doubleList = javaDoubleRDD.collect();
        return doubleList;
    }

    private static class PlotData {
        List<Double> xData;
        List<Double> yData;
        String plotLabel;
        public PlotData xData(List<Double> xData) {
            this.xData = xData;
            return this;
        }
        public PlotData yData(List<Double> yData) {
            this.yData = yData;
            return this;
        }
        public PlotData plotLabel(String plotLabel) {
            this.plotLabel = plotLabel;
            return this;
        }
    }

    public static void plot(List<PlotData> plotData) {
        final XYChart chart = new XYChartBuilder()   // Also try QuickChart
                .width(800)
                .height(600)
                .build();
        // Customize Chart
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setChartTitleVisible(false);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setMarkerSize(8);
        chart.getStyler().setSeriesColors(new Color[] { Color.RED, Color.BLACK, Color.CYAN }); // Warning: May need more elements...
        plotData.stream().forEach(plot -> {
            chart.addSeries(plot.plotLabel, plot.xData, plot.yData);
        });
//        chart.addSeries("Distance vs Duration", xData, yData);
        new SwingWrapper<XYChart>(chart).displayChart();
    }

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
        regressionData.show(5);

        // Display data
        Dataset<Row> sample;
        boolean doSample = false;
        if (doSample) {
            sample = regressionData.sample(false, 0.5, 12345);
        } else {
            sample = regressionData;
        }

        List<Double> x = rddIntToDoubleArray(sample.select("distance").javaRDD());
        List<Double> y = rddIntToDoubleArray(regressionData.select("duration").javaRDD());

        List<PlotData> plotData = Arrays.asList(new PlotData[] { new PlotData()
                .plotLabel("Duration vs Distance")
                .xData(x)
                .yData(y) });
        plot(plotData); // , "Distances (m)", "Durations (s)", "Regression - before");

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

        List<Double> xPred = rddIntToDoubleArray(predictions.select("distance").javaRDD());
        List<Double> yPred = rddDoubleToDoubleArray(predictions.select("prediction").javaRDD());

        List<PlotData> plotData2 = Arrays.asList(
                new PlotData[]{
                        new PlotData()
                                .plotLabel("Duration vs Distance")
                                .xData(x)
                                .yData(y),
                        new PlotData()
                                .plotLabel("Prediction")
                                .xData(xPred)
                                .yData(yPred)});
        plot(plotData2);

        System.out.println("Done!");
        spark.close();
    }
}
