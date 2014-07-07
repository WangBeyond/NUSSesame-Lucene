package lucene;

import java.io.Serializable;

import tool.Aggregation;

public class QueryVector implements Serializable {
	//Lp norm p
	public float p;
	public long theta;
	public int dim_range;
	public long value_range = -1;
	public int binary_value_range_length = -1;
	
	private long[] valueVector;
	private float[] weightVector;
	private int[] rangeVector;
	
	
	public QueryVector(int dim_range){
		this.dim_range = dim_range;
		valueVector = new long[dim_range];
		weightVector = new float[dim_range];
		rangeVector = new int[dim_range];
	}
	
	public long calcDistance(long[] values, Aggregation aggregation) {
		return aggregation.calcDistance(valueVector, values, p);
	}
	
	public void setValueRange (long valueRange) {
		this.value_range = valueRange;
		this.binary_value_range_length = Long.toBinaryString(value_range).length();
	}

	public void setValue(int index, long value) {
		valueVector[index] = value;
	}
	
	public void setWeight(int index, float weight) {
		weightVector[index] = weight;
	}
	
	public void setRange(int index, int range) {
		rangeVector[index] = range;
	}
	
	public long getValueAt(int index) {
		return valueVector[index];
	}
	
	public int getRangeAt(int index) {
		return rangeVector[index];
	}
	
	public float getWeightAt(int index) {
		return weightVector[index];
	}
}
