package vector_knn;

import lucene.QueryConfig;

public class SIFTConfig extends QueryConfig {
	
	public double pValue;
	
	public SIFTConfig(int id, int pValue) {
		super(id);
		// TODO Auto-generated constructor stub
		this.pValue = pValue;
	}
	
	public SIFTConfig(int id){
		super(id);
		this.pValue = 2;
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