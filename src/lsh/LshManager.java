package lsh;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import vector_knn.SIFTConfig;

import jclient.JClient;
import lucene.QueryConfig;
import lucene.ReturnValue;
import lucene.Index;


public class LshManager {
	private boolean test = true;
	
	private static int COMBINE_DIM = 128;	
	private static boolean GENERATE_PROJECTION = true;
	private static boolean READ_PROJECTION = false;
	
	private int numberOfHashTables;
	private int numberOfHashes;
	private int numberOfNeighbours;
	private double radius;
	
	private List<Vector> dataset;
	private List<Vector> queries;
	private String index_file;
	
	private int dimensions;
	private DistanceMeasure measure;
	private int timeout = 40; //seconds timeout for radius search.
	private HashFamily family;
	
	private boolean benchmark;
	private boolean printHelp;
	
	private JClient jclient;

	
	public LshManager(JClient jclient ) throws Throwable{
		this.jclient = jclient;
	}
	
	public LshManager() throws Throwable{
	}
	
	public void configure(int numberOfHashTables, int numberOfHashes, int numberOfNeighbours, int radius){
		this.dimensions = 128;
		this.numberOfHashTables = numberOfHashTables;
		this.numberOfHashes = numberOfHashes;
		this.numberOfNeighbours = numberOfNeighbours;
		this.radius = radius;
	}
	
//	public void parseQuery(List<QueryConfig> qlist) {
//		queries = new ArrayList<Vector>();
//		if(qlist != null){
//			queries.add(LSH.readQueryConfig(qlist));
//		}
//	}
//	
	public void setDataset(String dataFile) {
		if(dataFile !=null){
			long startTime = System.currentTimeMillis();
			dataset = LSH.readDataset(dataFile,Integer.MAX_VALUE);
			dimensions = dataset.get(0).getDimensions();
			long endTime = System.currentTimeMillis();
			if(test)
				System.out.println("read dataset time: "+(endTime - startTime)+" ms");
		}
	}
	
	public void setQuerys(String queryFile) {
		if(queryFile !=null){
			queries = LSH.readDataset(queryFile,Integer.MAX_VALUE);
			dimensions = queries.get(0).getDimensions();
		}
	}
	
	public void setIndexFile(String indexFile){
		this.index_file = indexFile;
	}
	
	//currently only return euclidian hash family
	private HashFamily getHashFamily(double radius,String hashFamilyType, int dimensions){
		if(hashFamilyType.equalsIgnoreCase("l2")){
			int w = (int) (10 * radius);
			w = w == 0 ? 1 : w;
			family = new EuclidianHashFamily(w, dimensions, (long)Integer.MAX_VALUE);
		} else{
			new IllegalArgumentException(hashFamilyType + " is unknown, should be one of cos|l1|l2" );
		}
		return family;
	}
	
	public void lshDistributeIndex(){
		
		family = getHashFamily(radius, "l2", dimensions);
		LSH lsh = new LSH(dataset, family, GENERATE_PROJECTION);
		System.out.println("Indexing...");
		List<List<Long>> combinedHashCollection = lsh.buildIndex(numberOfHashes,numberOfHashTables);	
		buildIndexOnNodes(combinedHashCollection);

	}
	
	public void lshLocalIndex() {
		family = getHashFamily(radius, "l2", dimensions);
		LSH lsh = new LSH(dataset, family, GENERATE_PROJECTION);
		System.out.println("Indexing...");
		lsh.buildIndex(numberOfHashes,numberOfHashTables);	
	}
	
	
	public void buildIndexOnNodes(List<List<Long>> combinedHashCollection){
		jclient.connectAllServers(index_file);
		//build index for bi-direction search
		jclient.initAllServers(Index.VECTOR_BUILD, index_file);
		//set the buffer size
		jclient.setMaxVecNum(5000);
		System.out.println("Begin remotely indexing");
		//num_elements indicate the number of the data set
		for (int i = 0; i < this.dataset.size(); i++) {
			Vector vector = dataset.get(i);
			long key = vector.getKey();
			List<Long> combinedHash = combinedHashCollection.get(i);
			long[] combinedhashArray = new long[combinedHash.size()];
			for(int j = 0; j < combinedHash.size(); j++){
				combinedhashArray[j] = combinedHash.get(j);
			}
			jclient.addPairs(
							key
			                , this.numberOfHashTables
			                , combinedhashArray
			                , 32
							, Index.VECTOR_BUILD
							);

			if(i % 10000 == 0)
				System.out.println(i);
		}
		
		jclient.flush();
		jclient.closeAllIndexwriters();
	}

	public ReturnValue startDistributeLSH()throws Throwable{
		long startTime = System.currentTimeMillis();
		ReturnValue revalue = new ReturnValue();
		family = getHashFamily(radius, "l2", dimensions);
		LSH lsh = new LSH(null, family, GENERATE_PROJECTION);
		lsh.buildIndex(numberOfHashes,numberOfHashTables);
		long endTime = System.currentTimeMillis();
		if(test)
			System.out.println("configure query time: "+(endTime - startTime)+" ms");
		if(queries != null){
			System.out.println("query...");
			for(int i = 0; i < queries.size(); i++){
				Vector query = queries.get(i);
				List<Long> combineHashes = lsh.index(query);
				lshSearch(i, combineHashes, query, lsh);
			}
		}
		return revalue;
	}
	
	public void lshSearch(int qid, List<Long> combineValues, Vector query, LSH lsh) throws Throwable{
		
		COMBINE_DIM = 1;
		int K = 100;
		//first, connect servers.
		jclient.connectAllServers(index_file);

		//create the QueryConfig for each dimension
		jclient.initAllServers(Index.VECTOR_BUILD, index_file);

		qid++;
		//maximum number of range and value
		int dim_range= this.numberOfHashTables;
		long value_range = 2*(long)Integer.MAX_VALUE;
		SIFTConfig config[] = new SIFTConfig[dim_range];
		long[] combine_values = new long[dim_range];
		for(int j = 0; j < dim_range; j++) {
			combine_values[j] = combineValues.get(j);
		}
		for(int i = 0; i < dim_range; i++) {
			int[] combine_values_int = new int[1];
			combine_values_int[0] = (int)combine_values[i];
			//initialize a query configuration, set query id
			config[i] = new SIFTConfig(qid);
			//set the domain
			config[i].setDimValueRange(dim_range, value_range);

			config[i].num_combination = COMBINE_DIM;
			//set query 
			config[i].setQuerylong(i, combine_values_int);
			config[i].setDim(i);
			
			//set bi-direction search range
			config[i].setRange(0, 0);		
			//set top K
			config[i].setK(K);
		}
		
		//init the servers before searching
		jclient.initAllServers(Index.VECTOR_SEARCH, index_file);
		//searching
		long starttime, endtime;
		System.out.println("searching...");
		starttime = System.currentTimeMillis();
		long[] indices = jclient.answerQuery(config);
		
//		for(long index : indices){
//			Vector vector = this.dataset.get(index);
//			
//		}
		
		//display the results
		List<Vector> candidates = new ArrayList<Vector>();
		for(int i = 0; i < indices.length; i++) {
			Vector candidate = this.dataset.get((int)indices[i]);
			candidates.add(candidate);
		}
		
		List<Vector> results = lsh.sortCandidates(query, candidates, this.numberOfNeighbours);
		endtime = System.currentTimeMillis();
		
		System.out.println("Results");
		for(int i = 0; i < results.size(); i++){
			System.out.println(results.get(i).getKey());
		}
		
		System.out.println("seraching time: "+(endtime - starttime)+" ms");
	}
	
	public ReturnValue startLocalLSH(){
		ReturnValue revalue = new ReturnValue();
		family = getHashFamily(radius, "l2", dimensions);
		LSH lsh = new LSH(null, family, GENERATE_PROJECTION);
		lsh.buildIndex(numberOfHashes,numberOfHashTables);	
		if(queries != null){
			for(Vector query:queries){
				System.out.println("query...");
				for(int i = 0; i < 128; i++){
					double value = query.get(i);
					System.out.print(Math.round(value)+" ");
				}
				System.out.println();
				List<Vector> neighbours = lsh.query(query, numberOfNeighbours);
				System.out.println(query.getKey()+" "+neighbours.size());
				for(Vector neighbour:neighbours){
					float count_dis[] = new float[2];
					count_dis[0] = 128;
					count_dis[1] = (float)family.createDistanceMeasure().distance(query, neighbour);				
					revalue.table.put(neighbour.getKey(), count_dis);
					System.out.println(neighbour.getKey() + " "+count_dis[1]*count_dis[1]);
					double distance = 0;
					Vector vec = neighbour;
					for(int j = 0; j < 128; j++){
						distance += ((query.get(j) - vec.get(j)) * (query.get(j) - vec.get(j)));
					}
				}
				System.out.print("\n");
			}
		}
		return revalue;
	}


	
	
	public static void main(String args[]){
		
		try{
			long start = System.currentTimeMillis();
			int numberOfHashTables = 8;
			int numberOfHashes = 8;
			int numberOfNeighbours = 10;
			double radius = 400;
			LshManager lshManager = new LshManager();
			lshManager.configure(6, 14, 10, 120);
			lshManager.setDataset("LSHfile_1_100000.txt");
			lshManager.setQuerys("data/query.txt");
			lshManager.lshLocalIndex();
			lshManager.startLocalLSH();
			long end = System.currentTimeMillis();
			System.out.println("total time: "+(end-start));
		} catch(Throwable e){
			System.out.println(e);
			e.printStackTrace();
		}
		
	}
	
}
