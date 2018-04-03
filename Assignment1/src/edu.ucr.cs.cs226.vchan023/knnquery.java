package edu.ucr.cs.cs226.vchan023;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import com.google.common.collect.Lists;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class knnquery {
    public static double distance(double x1, double y1, double x2, double y2)
    {
        final double d = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
        return d;
    }

    public static class DMapper extends Mapper<Object, Text, DoubleWritable, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] s = value.toString().split(",");
            String[] points = context.getConfiguration().get("point").split(",");
            double d = distance(Double.parseDouble(s[1]), Double.parseDouble(s[2]), Double.parseDouble(points[0]), Double.parseDouble(points[1]));
            context.write(new DoubleWritable(d),value);

        }
    }

    public static class DCombiner extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {


        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key,val);

            }
        }
    }

    public static class DReducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable> {


        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int k = context.getConfiguration().getInt("k", 1);

            for (Text val : values) {
                if (k > 0) {
                    context.write(val, key);
                    k--;
                    context.getConfiguration().setInt("k", k);
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("point",args[2]);
        conf.setInt("k", Integer.parseInt(args[3]));
        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(knnquery.class);
        job.setMapperClass(DMapper.class);
        job.setCombinerClass(DCombiner.class);
        job.setReducerClass(DReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
