package practice02.patitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import practice02.pojo.DateTemperaturePair;

public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {

	@Override
	public int getPartition(DateTemperaturePair key, Text value, int numPartitions) {
		return Math.abs(key.getYearMonth().hashCode()%numPartitions);
	}

}
