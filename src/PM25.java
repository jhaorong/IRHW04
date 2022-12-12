import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PM25 {

 public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line,",");
        float sum = 0;
        int count=0;

            String token0 = tokenizer.nextToken();
            String token1 = tokenizer.nextToken();
            String token2 = tokenizer.nextToken();


                if(token2.equals("PM2.5")){
                        while(tokenizer.hasMoreTokens()){
                                String token = tokenizer.nextToken();
                                try{

                                        sum = sum + Float.parseFloat(token);
                                        count = count+1;
                                }
                                catch (Exception e){

                                }
                        }
                        sum=sum/count;
                        context.write(new Text(token0+","+token1+","+token2),new FloatWritable(sum));

                }

     }
 }

 public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {

        float sum=0;

        for(FloatWritable val : values){
                sum += val.get();
        }

        context.write(key,new FloatWritable(sum));
    }
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(WordCount.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
 }

}

