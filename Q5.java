package practice02;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import practice02.pojo.Table;

public class Q5 {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);
		
		
		job.setJarByClass(Q5.class);
		job.setMapperClass(MR1_Mapper.class);
		job.setReducerClass(MR1_Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		Path inputPath = new Path("D:\\bigdata\\flow\\input\\visitalbe");
		Path outputPath = new Path("D:\\bigdata\\flow\\output\\visitalbe");
		FileInputFormat.setInputPaths(job, inputPath);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		Configuration conf2 = new Configuration();
		FileSystem fs2 = FileSystem.get(conf2);
		Job job2 = Job.getInstance(conf2);
		
		
		job2.setMapperClass(MR2_Mapper.class);
		job2.setReducerClass(MR2_Reducer.class);
		
		job2.setMapOutputKeyClass(Table.class);
		job2.setMapOutputValueClass(NullWritable.class);
		job2.setOutputKeyClass(Table.class);
		job2.setOutputValueClass(NullWritable.class);
		
		
		Path inputPath2 = new Path("D:\\bigdata\\flow\\output\\visitalbe");
		Path outputPath2 = new Path("D:\\bigdata\\flow\\output\\visitalbe_last");
		FileInputFormat.setInputPaths(job2, inputPath2);
		if(fs2.exists(outputPath2)){
			fs2.delete(outputPath2, true);
		}
		FileOutputFormat.setOutputPath(job2, outputPath2);
		
		ControlledJob ajob = new ControlledJob(job.getConfiguration());
		ControlledJob bjob = new ControlledJob(job2.getConfiguration());
		bjob.addDependingJob(ajob);
		
		JobControl control = new JobControl("myControl");
		control.addJob(ajob);
		control.addJob(bjob);
		
		Thread t = new Thread(control);
		t.start();
		while(!control.allFinished()){
			Thread.sleep(1000);
		}
		System.exit(0);
	}
	/**
	 * 
	 * @author Administrator
	 *TableName(表名)，Time(时间)，User(用户)，TimeSpan(时间开销)
	 */
	public static class MR1_Mapper extends Mapper<LongWritable, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			if(split[1].equals("10:00")){
				outKey.set(split[0]);//表名设置为key
				String str = split[1]+","+split[2]+","+split[3];
				outValue.set(str);//其它字段设置为value
				context.write(outKey, outValue);
			}
		}
		
	}
	//TableName(表名)   Time(时间)，User(用户)，TimeSpan(时间开销)
	public static class MR1_Reducer extends Reducer<Text, Text, NullWritable, Text>{
		HashMap<String, Integer> userMap = new HashMap<>();
		HashMap<String, Double> totalTimeMap = new HashMap<>();
		Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int visiTableCount = 0;
			for(Text value : values){
				visiTableCount++;
				String[] split = value.toString().split(",");
				String user = split[1];
				double timeSpan = Double.parseDouble(split[2]);
				if(userMap.get(user)==null){
					userMap.put(user, 1);
					totalTimeMap.put(user, timeSpan);
				}else{
					userMap.put(user, userMap.get(user)+1);
					totalTimeMap.put(user, totalTimeMap.get(user)+timeSpan);
				}
			}
			//记录最高访问次数
			int maxCountUser = 0;
			//记录访问最多的用户
			String maxUser = null;
			for(Entry<String, Integer> entry:userMap.entrySet()){
				if(entry.getValue()>maxCountUser){
					maxCountUser = entry.getValue();
					maxUser = entry.getKey();
				}
			}
			//获取访问最多的用户总共的访问时间
			double maxUserTime = totalTimeMap.get(maxUser);
			//表名，这张表的访问次数，访问这张表最多的用户，这他一共访问了多长时间
			String all =key.toString()+","+ visiTableCount + ","+ maxUser +","+maxUserTime ;
			outValue.set(all);
			context.write(NullWritable.get(),outValue);
			userMap.clear();
			totalTimeMap.clear();
		}
	}
	/**
	 * 读取到的数据
	 * 表名，访问次数，访问最多的用户，该用户访问的时间
	 */
	public static class MR2_Mapper extends Mapper<LongWritable, Text, Table, NullWritable>{
		Table table = new Table();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			String tableName = split[0];
			int visitCount = Integer.parseInt(split[1]);
			String userName = split[2];
			String totalTime = split[3];
			table.setTableName(tableName);
			table.setUserName(userName);
			table.setVisitCount(visitCount);
			table.setTotalTime(totalTime);
			context.write(table, NullWritable.get());
		}
		
	}
	
	public static class MR2_Reducer extends Reducer<Table, NullWritable, Table, NullWritable>{
		int maxValue = Integer.MIN_VALUE;
		@Override
		protected void reduce(Table key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			if(key.getVisitCount()>maxValue){
				context.write(key, NullWritable.get());
				maxValue = key.getVisitCount();
			}
		}
	}
}
