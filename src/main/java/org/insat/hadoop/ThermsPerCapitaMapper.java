package org.insat.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ThermsPerCapitaMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text communityArea = new Text();
    private DoubleWritable thermsPerCapita = new DoubleWritable();
    private boolean isHeader = true;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return;
        }

        String[] fields = value.toString().split(",");
        try {
            if (fields.length > 65 && !fields[0].isEmpty() && !fields[29].isEmpty() && !fields[65].isEmpty()) {
                double therms = Double.parseDouble(fields[29]); // TOTAL THERMS
                double population = Double.parseDouble(fields[65]); // TOTAL POPULATION
                if (population > 0) {
                    communityArea.set(fields[0]); // COMMUNITY AREA NAME
                    thermsPerCapita.set(therms / population);
                    context.write(communityArea, thermsPerCapita);
                }
            }
        } catch (NumberFormatException e) {
            System.err.println("Skipping invalid row: " + value.toString());
        }
    }
}






