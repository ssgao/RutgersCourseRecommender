package edu.rutgers.ess.crs.testingtrainingcross;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestingTrainingCrossReducer extends Reducer<Text, Text, Text, Text>
{
    public void reduce(final Text key, final Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
        final List<String> vals = new ArrayList<String>();
        for (final Text t : values) {
            vals.add(t.toString());
        }
        
        Collections.sort(vals, Collections.reverseOrder());
        
        for (int i = 0; i < 50 && i < vals.size(); ++i) {
            context.write(key, new Text(vals.get(i)));
        }
    }
}
