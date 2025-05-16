package org.insat.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.data.category.DefaultCategoryDataset;

import java.awt.Color;
import java.awt.Font;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: org.insat.hadoop.Main <input path> <output path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        String input = args[0];
        String outputBase = args[1];

        // Define colors for chart customization
        Color[] colors = {Color.BLUE, Color.GREEN, Color.RED};

        // Job 1: Average Electricity Consumption by Building Type
        runJob(conf, input, outputBase + "/building_type", "Avg Electricity by Building Type",
                BuildingTypeMapper.class, BuildingTypeReducer.class);
        createChart(conf, outputBase + "/building_type", "Average Electricity Consumption by Building Type",
                "Building Type", "Avg KWH", "JOB1_CHART.png", colors, 0); // No label skipping

        // Job 2: Monthly Energy Usage Trend by Community
        runJob(conf, input, outputBase + "/monthly_trend", "Monthly Energy Trend",
                MonthlyTrendMapper.class, MonthlyTrendReducer.class);
        createChart(conf, outputBase + "/monthly_trend", "Monthly Energy Usage Trend by Community",
                "Community:Month", "KWH", "JOB2_CHART.png", colors, 5); // Show every 20th label

        // Job 3: Therm Usage per Capita
        runJob(conf, input, outputBase + "/therms_per_capita", "Therms per Capita",
                ThermsPerCapitaMapper.class, ThermsPerCapitaReducer.class);
        createChart(conf, outputBase + "/therms_per_capita", "Therms Usage per Capita by Community",
                "Community", "Therms per Person", "JOB3_CHART.png", colors, 5); // Show every 5th label

        System.exit(0);
    }

    private static void runJob(Configuration conf, String input, String output, String jobName,
                               Class<? extends org.apache.hadoop.mapreduce.Mapper> mapperClass,
                               Class<? extends org.apache.hadoop.mapreduce.Reducer> reducerClass)
            throws Exception {
        Path outputPath = new Path(output);
        FileSystem fs = FileSystem.getLocal(conf); // Use local filesystem
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(Main.class);
        job.setMapperClass(mapperClass);
        job.setCombinerClass(reducerClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

    private static void createChart(Configuration conf, String outputPath, String title,
                                    String xLabel, String yLabel, String fileName, Color[] colors, int labelSkip) throws Exception {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        FileSystem fs = FileSystem.getLocal(conf); // Use local filesystem
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outputPath + "/part-r-00000"))));

        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                String key = parts[0];
                double value = Double.parseDouble(parts[1]);
                dataset.addValue(value, yLabel, key);
            }
        }
        reader.close();

        // Debug: Print the number of categories for this chart
        System.out.println("Chart for " + title + ": " + dataset.getColumnCount() + " categories");

        JFreeChart chart = ChartFactory.createBarChart(
                title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, true, true, false);

        // Customize the chart
        CategoryPlot plot = chart.getCategoryPlot();
        plot.getRenderer().setSeriesPaint(0, colors[2]); // Use red color for bars
        plot.setBackgroundPaint(Color.WHITE);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

        // Customize x-axis
        CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setTickLabelFont(new Font("SansSerif", Font.PLAIN, 10)); // Smaller font for readability

        // Rotate x-axis labels to 45 degrees and skip labels if labelSkip > 0
        if (labelSkip > 0) {
            // Show only every nth label
            for (int i = 0; i < dataset.getColumnCount(); i++) {
                if (i % labelSkip != 0) {
                    domainAxis.setTickLabelPaint(dataset.getColumnKey(i), new Color(0, 0, 0, 0)); // Transparent label
                }
            }
        }
        domainAxis.setCategoryLabelPositions(
                CategoryLabelPositions.createUpRotationLabelPositions(Math.toRadians(45))
        );

        // Increase chart width for Charts 2 and 3 to accommodate more categories
        int chartWidth = (labelSkip > 0) ? 1200 : 800;
        ChartUtils.saveChartAsPNG(new File(fileName), chart, chartWidth, 600);
    }
}