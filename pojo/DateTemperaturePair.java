package practice02.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class DateTemperaturePair implements WritableComparable<DateTemperaturePair> {
	private String yearMonth;
	private String day;
	private int temperature;
	
	public DateTemperaturePair(String yearMonth, String day, int temperature) {
		super();
		this.yearMonth = yearMonth;
		this.day = day;
		this.temperature = temperature;
	}
	public DateTemperaturePair() {
	}
	
	public String getYearMonth() {
		return yearMonth;
	}
	public void setYearMonth(String yearMonth) {
		this.yearMonth = yearMonth;
	}
	public String getDay() {
		return day;
	}
	public void setDay(String day) {
		this.day = day;
	}
	public int getTemperature() {
		return temperature;
	}
	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}
	
	@Override
	public String toString() {
		return yearMonth+":"+temperature;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(yearMonth);
		out.writeUTF(day);
		out.writeInt(temperature);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.yearMonth = in.readUTF();
		this.day = in.readUTF();
		this.temperature = in.readInt();
	}
	
	@Override
	public int compareTo(DateTemperaturePair o) {
		int compareValue = this.yearMonth.compareTo(o.yearMonth);
		if(compareValue==0){
			compareValue = this.temperature-o.temperature;
		}
		return -1*compareValue;
	}
	
}
