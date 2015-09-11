package com.eduonix.prepare;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by ubu on 03.09.15.
 */
public class PrepareData extends Configured implements Tool {


    private Path output;
    private Path input;

    public static void main(String[] args) throws Exception {


        int res = ToolRunner.run(new Configuration(), new PrepareData(), args);

        System.exit(res);


    }


    @Override
    public int run(String[] args) throws Exception {
        // When implementing tool
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "PrepareData Job");

        setTextoutputformatSeparator(job, "|");

        job.setJarByClass(PrepareData.class);

        //Key type coming out of mapper
        job.setMapOutputKeyClass(Text.class);

        //value type coming out of mapper
        job.setMapOutputValueClass(Text.class);

        //Defining the mapper class name
        // job.setMapperClass(SimpleMapper.class);
        job.setMapperClass(PrepareDataMapper.class);
        //Defining the reducer class name
        job.setReducerClass(PrepareDataReducer.class);

        //Defining input Format class which is responsible to parse the dataset into a key value pair
        job.setInputFormatClass(TextInputFormat.class);


        //Defining output Format class which is responsible to parse the dataset into a key value pair
        job.setOutputFormatClass(TextOutputFormat.class);

        output = new Path("/user/root/analytic_out");
        input = new Path("/user/root/raw_data.txt");

        output.getFileSystem(conf).delete(output);

        FileInputFormat.addInputPath(job, input);

        FileOutputFormat.setOutputPath(job, output);

    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
        job.setJobName("PrepareData Driver");


        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    void setTextoutputformatSeparator(final Job job, final String separator) {
        final Configuration conf = job.getConfiguration(); //ensure accurate config ref

        conf.set("mapred.textoutputformat.separatorText", separator); // ?
    }
}
