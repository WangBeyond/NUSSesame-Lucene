package vector_knn;

import lucene.QueryConfig;

public class SIFTConfig extends QueryConfig {
	
	public double pValue = 2;
	
	public SIFTConfig(int id) {
		super(id);
		// TODO Auto-generated constructor stub
	}

	@Override
	public float calcDistance(long a, long b) {
		// TODO Auto-generated method stub
		return (float)Math.pow(a-b, pValue);
		//L1
		//return Math.abs(a - b);
	}

	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return QueryConfig.VECTOR;
	}
	
}