package practice02;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.ObjectUtils.Null;
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

import practice02.pojo.Course2;

public class Q3_3_2 {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Q3_3");
		job.setJarByClass(Q3_3_2.class);
		
		job.setMapperClass(MyCFMapper.class);
		job.setReducerClass(MyCFReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Course2.class);
		job.setOutputKeyClass(Course2.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("d:/bigdata/flow/input/grad"));
		FileOutputFormat.setOutputPath(job, new Path("d:/bigdata/flow/output/grad32"));
		
		boolean completion = job.waitForCompletion(true);
		System.exit(completion?0:1);
		
	}
	
	public static class MyCFMapper extends Mapper<LongWritable, Text, Text, Course2>{
		/**
		 *algorithm,huangzitao,85,86,41,75,93,42,85,75
		 *3、求出每门课程参考学生成绩最高的学生的信息：课程，姓名和平均分
		 */
		Course2 course = new Course2();
		Text outKey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			String courseName = split[0];
			String student = split[1];
			double avg = 0;
			double maxScore = 0;
			int count = 0;
			int sum = 0;
			for(int i = 2;i<split.length;i++){
				count++;
				int score = Integer.parseInt(split[i]);
				sum += score;
				if(score>maxScore){
					maxScore = score;
				}
			}
			avg = sum/(double)count;
			course.setName(courseName).setAverage(avg).setStudent(student).setMaxScore(maxScore);
			outKey.set(courseName);
			context.write(outKey, course);
		}
	}
	
	public static class MyCFReducer extends Reducer<Text, Course2, Course2, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Course2> values, Context context)	throws IOException, InterruptedException {
			Course2 maxC = null;
			double maxScore = 0;
			for(Course2 c:values){
				if(c.getMaxScore()>maxScore)
					maxC = c;
			}
			context.write(maxC, NullWritable.get());
			
		}
	}
	
}
