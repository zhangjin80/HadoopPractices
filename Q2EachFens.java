package practice02;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q2EachFens {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"EachFens");
		job.setJarByClass(Q2EachFens.class);
		
		job.setMapperClass(MyCFMapper.class);
		job.setReducerClass(MyCFReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("d:/bigdata/flow/input/friends"));
		FileOutputFormat.setOutputPath(job, new Path("d:/bigdata/flow/output/echfends"));
		
		boolean completion = job.waitForCompletion(true);
		System.exit(completion?0:1);
		
	}
	
	public static class MyCFMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		/**
		 *
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(":");
			String host = split[0];
			String[] slaver = split[1].split(",");
			
			for(String str : slaver){
				if(host.compareTo(str)<0){
					context.write(new Text(host+"<=>"+str), NullWritable.get());
				}else{
					context.write(new Text(str+"<=>"+host), NullWritable.get());
				}
			}
			
		}
	}
	
	public static class MyCFReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)	throws IOException, InterruptedException {
			int count =0;
			for(NullWritable n : values){
				count++;
			}
			if(count>1){
				context.write(key, NullWritable.get());
			}
		}
	}
	
}
