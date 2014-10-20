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
			dataset = LSH.readDataset(dataFile,Integer.MAX_VALUE);
			dimensions = dataset.get(0).getDimensions();
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
		ReturnValue revalue = new ReturnValue();
		family = getHashFamily(radius, "l2", dimensions);
		LSH lsh = new LSH(new ArrayList<Vector>(), family, GENERATE_PROJECTION);
		lsh.buildIndex(numberOfHashes,numberOfHashTables);	
		if(queries != null){
			jclient.connectAllServers(this.index_file);
			System.out.println("query...");
			for(int i = 0; i < queries.size(); i++){
				Vector query = queries.get(i);
				List<Long> combineValues = lsh.index(query);
				lshSearch(i, combineValues);
				System.out.println(query.getKey());
				System.out.print("\n");
			}
		}
		return revalue;
	}
	
	public void lshSearch(int qid, List<Long> combineValues) throws Throwable{
		
		COMBINE_DIM = 1;
		int K = 100;
		//first, connect servers.
		jclient.connectAllServers(index_file);

		//create the QueryConfig for each dimension

		qid++;
		//maximum number of range and value
		int dim_range= this.numberOfHashTables;
		long value_range = (long)Integer.MAX_VALUE;
		SIFTConfig config[] = new SIFTConfig[dim_range];
		long[] combine_values = new long[dim_range];
		for(int j = 0; j < COMBINE_DIM; j++) {
			combine_values[j] = combineValues.get(j);
		}
		for(int i = 0; i < dim_range; i++) {
			//initialize a query configuration, set query id
			config[i] = new SIFTConfig(qid);
			//set the domain
			config[i].setDimValueRange(dim_range, value_range);

			config[i].num_combination = COMBINE_DIM;
			//set query 
			config[i].setQuerylong((long)(i*Math.pow(2,32))+combine_values[i]);
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
		long[] index = jclient.answerQuery(config);
		endtime = System.currentTimeMillis();
		
		//display the results
		for(int i = 0; i < index.length; i++) {
			System.out.println(index[i]);
		}
		System.out.println("seraching time: "+(endtime - starttime)+" ms");
	}
	
	public ReturnValue startLocalLSH(){
		ReturnValue revalue = new ReturnValue();
		family = getHashFamily(radius, "l2", dimensions);
		LSH lsh = new LSH(new ArrayList<Vector>(), family, GENERATE_PROJECTION);
		lsh.buildIndex(numberOfHashes,numberOfHashTables);	
		if(queries != null){
			for(Vector query:queries){
				System.out.println("query...");
				for(int i = 0; i < 128; i++){
					double value = query.get(i);
					System.out.print(Math.round(value)+" ");
				}
				List<Vector> neighbours = lsh.query(query, numberOfNeighbours);
				System.out.println(query.getKey());
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
			int numberOfHashTables = 5;
			int numberOfHashes = 4;
			int numberOfNeighbours = 10;
			double radius = 400;
			LshManager lshManager = new LshManager();
			lshManager.configure(10, 8, 10, 300);
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
