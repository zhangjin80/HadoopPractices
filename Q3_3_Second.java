package practice02;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import practice02.gc.ClazzScoreGroupComparator;
import practice02.pojo.CourseScore;

public class Q3_3_Second {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Q3_3_Second.class);
		job.setMapperClass(MR_Mapper.class);
		job.setReducerClass(MR_Reducer.class);
		
		job.setOutputKeyClass(CourseScore.class);
		job.setOutputValueClass(NullWritable.class);
		
		//设置分组比较器
		job.setGroupingComparatorClass(ClazzScoreGroupComparator.class);
		
		Path inputPath = new Path("D:\\bigdata\\flow\\input\\grad");
		Path outputPath = new Path("D:\\bigdata\\flow\\output\\q3");
		FileInputFormat.setInputPaths(job, inputPath);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);
	}
	/**
	 * 原始数据示例：
	 *  math,liujialing,85,86,41,75,93,42,85,75
	 *	english,huangxiaoming,85,86,41,75,93,42,85
	 */
	public static class MR_Mapper extends Mapper<LongWritable, Text, CourseScore, NullWritable>{
		//创建课程分数类对象
		CourseScore cs = new CourseScore();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			String courseName = split[0];
			String studentName = split[1];
			double avgScore=0;
			int sum = 0;
			for(int i=2;i<split.length;i++){
				sum += Integer.parseInt(split[i]);
			}
			//求平均分
			avgScore=sum/(split.length-2.0);
			avgScore =(Math.round(avgScore*100)/100.0);//保留两位小数
			//把平均分，课程名，学生姓名封装进对象
			cs.setAvgScore(avgScore);
			cs.setCourseName(courseName);
			cs.setStudentName(studentName);
			context.write(cs, NullWritable.get());
		}
	}
	
	public static class MR_Reducer extends Reducer<CourseScore, NullWritable, CourseScore, NullWritable>{
		@Override
		protected void reduce(CourseScore key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			//如果只想输出前N项可以设置计数器int count = 0，放在for循环中为N的时候退出
			//遍历每一组的values并输出这一组key
			for(NullWritable v:values){
				context.write(key, NullWritable.get());
			}
		}
	}
}
