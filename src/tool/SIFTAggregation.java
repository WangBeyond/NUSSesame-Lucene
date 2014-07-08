package tool;

import java.util.ArrayList;

public class SIFTAggregation extends Aggregation {

	
	
	public SIFTAggregation()
	{

	}
	
	
	
	@Override
	public long calcDistance(long[] valueVector1, long[] valueVector2) {
		long acculDist = 0; 
		for (int i = 0 ; i< valueVector1.length; i++) {
			acculDist += Math.pow(valueVector1[i]- valueVector2[i], 2);
		}
		return acculDist;
	}
	
}

