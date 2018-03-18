package practice02.gc;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import practice02.pojo.NumberSort;

public class NumberSrotGroupComparator extends WritableComparator {
	public NumberSrotGroupComparator() {
		super(NumberSort.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		NumberSort ag = (NumberSort)a;
		NumberSort bg = (NumberSort)b;
		return ag.getGroup()-bg.getGroup();
	}

}
