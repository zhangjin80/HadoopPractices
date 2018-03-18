package practice02;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1CommonFriend {
	public static void main(String[] args) throws Exception {
		/**
		 * 设置job1
		 */
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1);
		job1.setJarByClass(Q1CommonFriend.class);
		job1.setMapperClass(MyCF1Mapper.class);
		job1.setReducerClass(MyCF1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		Path inPath = new Path("d:/bigdata/flow/input/friends");
		Path outPath = new Path("d:/bigdata/flow/output/friends");
		FileInputFormat.setInputPaths(job1,inPath);
		FileOutputFormat.setOutputPath(job1, outPath);
		
		/**
		 * 设置job2
		 */
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2);
		job2.setMapperClass(MyCF2Mapper.class);
		job2.setReducerClass(MyCF2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		Path inPath2 = new Path("d:/bigdata/flow/output/friends");
		Path outPath2 = new Path("d:/bigdata/flow/output/friends_last");
		FileInputFormat.setInputPaths(job2, inPath2);
		FileOutputFormat.setOutputPath(job2, outPath2);
		
		/**
		 * 配置jobControl来控制job的执行顺序
		 * 设置执行顺序
		 */
		JobControl control = new JobControl("CF");
		ControlledJob ajob = new ControlledJob(job1.getConfiguration());
		ControlledJob bjob = new ControlledJob(job2.getConfiguration());
		bjob.addDependingJob(ajob);
		
		control.addJob(ajob);
		control.addJob(bjob);
		
		Thread th = new Thread(control);
		th.start();
		/**
		 * 主进程等待control执行完再向下执行
		 */
		while(!control.allFinished()){
			Thread.sleep(1000);
		}
		
//		boolean completion = job2.waitForCompletion(true);
//		System.exit(completion?0:1);
		System.exit(0);
		
	}
	
	public static class MyCF1Mapper extends Mapper<LongWritable, Text, Text, Text>{
		/**
		 * 	A:B,C,D,F,E,O
			1、求所有两两用户之间的共同好友
		 */
		Text keyOut = new Text();
		Text valueOut = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(":");
			String user = split[0];
			String[] friends = split[1].split(",");
			valueOut.set(user);
			//装换成friend	user
			for(int i=0;i<friends.length;i++){
				keyOut.set(friends[i]);
				context.write(keyOut, valueOut);
			}
		}
	}
	public static class MyCF1Reducer extends Reducer<Text, Text, Text, Text>{
		Text keyOut = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
			//把friend user1	user2 user3....转成user1-user2	friend
			List<String> users = new ArrayList<>();
			for(Text user: values){
				users.add(user.toString());
			}
			Collections.sort(users);
			for(int i = 0;i<users.size()-1;i++){
				for(int j=i+1;j<users.size();j++){
					keyOut.set(users.get(i)+"-"+users.get(j));
					context.write(keyOut, key);
				}
			}
		}
	}
	
	public static class MyCF2Mapper extends Mapper<LongWritable, Text, Text, Text>{
		/**
		 * B-C	A
		 */
		Text keyOut = new Text();
		Text valueOut = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			keyOut.set(split[0]);
			valueOut.set(split[1]);
			context.write(keyOut, valueOut);
		}
	}
	
	public static class MyCF2Reducer extends Reducer<Text, Text, Text, Text>{
		Text valueOut = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text user:values){
				sb.append(user).append(",");
			}
			valueOut.set(sb.toString());
			context.write(key, valueOut);
		}
	}
}
