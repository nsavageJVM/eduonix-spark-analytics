package com.eduonix.prepare;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by ubu on 03.09.15.
 */
public class PrepareDataReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key,  Iterator<Text> values, Context context)  throws IOException, InterruptedException {

        String valueData = values.next().toString();

        context.write(key, new Text(valueData));

    }
}
