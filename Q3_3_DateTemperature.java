package practice02;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import practice02.gc.DateTemperatureGroupingComparator;
import practice02.patitioner.DateTemperaturePartitioner;
import practice02.pojo.DateTemperaturePair;

public class Q3_3_DateTemperature {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);
		
		
		job.setJarByClass(Q3_3_DateTemperature.class);
		job.setMapperClass(MR_Mapper.class);
		job.setReducerClass(MR_Reducer.class);
		
		job.setMapOutputKeyClass(DateTemperaturePair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(DateTemperaturePair.class);
		job.setOutputValueClass(Text.class);
		
//		job.setPartitionerClass(DateTemperaturePartitioner.class);
		job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);
		
		Path inputPath = new Path("D:\\bigdata\\flow\\input\\temp");
		Path outputPath = new Path("D:\\bigdata\\flow\\output\\temp");
		FileInputFormat.setInputPaths(job, inputPath);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);
	}
	
	public static class MR_Mapper extends Mapper<LongWritable, Text, DateTemperaturePair, IntWritable>{
		DateTemperaturePair outKey = new DateTemperaturePair();
		IntWritable outValue = new IntWritable();
		//2012,01,02,15
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String yearMonth = tokens[0]+tokens[1];
			String day = tokens[2];
			int temperature = Integer.parseInt(tokens[3]);
			outKey.setYearMonth(yearMonth);
			outKey.setDay(day);
			outKey.setTemperature(temperature);
			outValue.set(temperature);
			context.write(outKey, outValue);
		}
		
	}
	
	public static class MR_Reducer extends Reducer<DateTemperaturePair, IntWritable, DateTemperaturePair, Text>{
		//输出格式：201301	90,80,70,-10,
		@Override
		protected void reduce(DateTemperaturePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			StringBuilder sortedTemperatureList = new StringBuilder();
			for(IntWritable tempera : values){
				sortedTemperatureList.append(tempera).append(",");
			}
			Text outValue = new Text();
			outValue.set(sortedTemperatureList.toString());
			context.write(key, outValue);
			
		}
	}
}