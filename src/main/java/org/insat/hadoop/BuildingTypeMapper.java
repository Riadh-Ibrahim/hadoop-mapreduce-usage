package org.insat.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class BuildingTypeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text buildingType = new Text();
    private DoubleWritable kwh = new DoubleWritable();
    private boolean isHeader = true;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return;
        }

        String[] fields = value.toString().split(",");
        try {
            if (fields.length > 16 && !fields[2].isEmpty() && !fields[16].isEmpty()) {
                buildingType.set(fields[2]); // BUILDING TYPE
                kwh.set(Double.parseDouble(fields[16])); // TOTAL KWH
                context.write(buildingType, kwh);
            }
        } catch (NumberFormatException e) {
            System.err.println("Skipping invalid row: " + value.toString());
        }
    }
}






