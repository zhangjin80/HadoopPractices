package practice02.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Table implements WritableComparable<Table>{
	private String tableName;
	private String userName;
	private int visitCount;
	private String totalTime;
	
	@Override
	public String toString() {
		return tableName + ","+userName+","+ visitCount + "," + totalTime;
	}
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public int getVisitCount() {
		return visitCount;
	}
	public void setVisitCount(int visitCount) {
		this.visitCount = visitCount;
	}
	public String getTotalTime() {
		return totalTime;
	}
	public void setTotalTime(String totalTime) {
		this.totalTime = totalTime;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(tableName);
		out.writeUTF(userName);
		out.writeInt(visitCount);
		out.writeUTF(totalTime);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.tableName = in.readUTF();
		this.userName = in.readUTF();
		this.visitCount = in.readInt();
		this.totalTime = in.readUTF();
	}
	@Override
	public int compareTo(Table o) {
		return o.getVisitCount()-this.getVisitCount();
	}
	

}
