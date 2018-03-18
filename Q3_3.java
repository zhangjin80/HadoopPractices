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

public class Q3_3 {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Q3_3");
		job.setJarByClass(Q3_3.class);
		
		job.setMapperClass(MyCFMapper.class);
		job.setReducerClass(MyCFReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("d:/bigdata/flow/input/grad"));
		FileOutputFormat.setOutputPath(job, new Path("d:/bigdata/flow/output/grad3"));
		
		boolean completion = job.waitForCompletion(true);
		System.exit(completion?0:1);
		
	}
	
	public static class MyCFMapper extends Mapper<LongWritable, Text, Text, Text>{
		/**
		 *algorithm,huangzitao,85,86,41,75,93,42,85,75
		 *3、求出每门课程参考学生成绩最高的学生的信息：课程，姓名和平均分
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			String course = split[0];
			String name = split[1];
			double avg = 0;
			int count = 0;
			int sum = 0;
			for(int i = 2;i<split.length;i++){
				count++;
				sum +=Integer.parseInt(split[i]);
			}
			avg = sum/(double)count;
			context.write(new Text(course), new Text(name +"\t"+avg));
			
		}
	}
	
	public static class MyCFReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
			double maxScore=0;
			String maxName = "";
			for(Text v:values){
				String[] split = v.toString().split("\t");
				String name = split[0];
				double avg = Double.parseDouble(split[1]);
				if(avg>maxScore){
					maxScore = avg;
					maxName= name;
				}
			}
			context.write(key, new Text(maxName + "\t"+ maxScore));
			
		}
	}
	
}
