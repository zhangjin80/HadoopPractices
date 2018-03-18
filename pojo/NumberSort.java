package practice02.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NumberSort implements WritableComparable<NumberSort> {
	private int group;
	private int num;
	public NumberSort() {
	}
	
	@Override
	public String toString() {
		return num+"";
	}

	public int getGroup() {
		return group;
	}
	public void setGroup(int group) {
		this.group = group;
	}
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(group);
		out.writeInt(num);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.group = in.readInt();
		this.num = in.readInt();
	}

	@Override
	public int compareTo(NumberSort o) {
		int temp = this.group - o.group;
		if(temp == 0){
			return this.num - o.num;
		}else{
			return temp;
		}
	}
	
}
