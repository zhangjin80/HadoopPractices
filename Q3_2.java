package practice02;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.jetty.servlet.Context;

import practice02.patitioner.CoursePartitioner;
import practice02.pojo.Course;

public class Q3_2 {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Q3_1");
		job.setJarByClass(Q3_2.class);
		
		job.setMapperClass(MyCFMapper.class);
		job.setReducerClass(MyCFReducer.class);
		
		job.setOutputKeyClass(Course.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("d:/bigdata/flow/input/grad"));
		FileOutputFormat.setOutputPath(job, new Path("d:/bigdata/flow/output/grad2"));
		
		job.setPartitionerClass(CoursePartitioner.class);
		job.setNumReduceTasks(4);
		
		boolean completion = job.waitForCompletion(true);
		System.exit(completion?0:1);
		
	}
	
	public static class MyCFMapper extends Mapper<LongWritable, Text, Course, NullWritable>{
		/**
		 *algorithm,huangzitao,85,86,41,75,93,42,85,75
		 *2、统计每门课程参考学生的平均分，并且按课程存入不同的结果文件，
		 *	要求一门课程一个结果文件，并且按平均分从高到低排序，分数保留一位小数
		 *
		 *map 输出
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			String name = split[0];
			String student = split[1];
			double avg = 0;
			int count = 0;
			int sum = 0;
			for(int i = 2;i<split.length;i++){
				count++;
				sum +=Integer.parseInt(split[i]);
			}
			avg = sum/(double)count;
			avg = (double)(Math.round(avg*100)/100.0);
			Course course = new Course(name,student,avg);
			context.write(course, NullWritable.get());
		}
	}
	
	public static class MyCFReducer extends Reducer<Course, NullWritable, Course, NullWritable>{
		@Override
		protected void reduce(Course key, Iterable<NullWritable> values, Context context)	throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
}
