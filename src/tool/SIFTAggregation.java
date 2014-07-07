package tool;

import java.util.ArrayList;

public class SIFTAggregation extends Aggregation {

	private ArrayList<Long> weightVector;
	private long pValue;
	
	
	public SIFTAggregation(ArrayList<Long> weightVector, long pValue)
	{
		this.weightVector.addAll(weightVector);
		this.pValue = pValue;
	}
	
	
	
	@Override
	public long calcDistance(long[] valueVector1, long[] valueVector2) {
		long acculDist = 0; 
		for (int i = 0 ; i< valueVector1.length; i++) {
			acculDist += weightVector.get(i) * Math.pow(valueVector1[i]- valueVector2[i], pValue);
		}
		return acculDist;
	}
	
}

