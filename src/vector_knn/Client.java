 package vector_knn;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import jclient.JClient;
import jmaster.FunctionHandler;
import tool.DataProcessor;
import lucene.Index;
import lucene.QueryConfig;
import lucene.ReturnValue;

public class Client {
	
	public static boolean debug = true;
	public static int NUM_DIM = 128;
	public static int COMBINE_DIM = 1;
	//Two vectors for each range query
	public static int RANGE_QUERY_NUM = 2;
	public static int K = 1;

	private JClient jclient;
	private Reader reader;
	
	private static int vec_num;
	private static int nodes_num;
	private static String vec_index = "Index"+vec_num;
	private static String vec_scanfile = "Subfile"+vec_num;
	
	Client(String ip) throws Throwable {
		
		jclient = new JClient(ip);
	}
	
	
	public void buildIndex(String filename, int num_elements,String index_file) throws Throwable {
		System.out.println("Building...");
	
		jclient.connectAllServers(index_file);
	
		// to read binary data
		reader = new Reader(filename);
		reader.openReader();
		
		//build index for bi-direction search
		jclient.initAllServers(Index.VECTOR_BUILD, index_file);
		//set the buffer size
		jclient.setMaxVecNum(5000);
		
		//num_elements indicate the number of the test feature
		for (int i = 0; i < num_elements; i++) {
			int value_id[];
			value_id = reader.getFeature(NUM_DIM);
			
			if(debug) {
				System.out.println(value_id.length);
				for(int j = 0; j < value_id.length; j++) {
					System.out.print(value_id[j]+",");
				}
				System.out.println();
			}
			
			long values[] = new long[NUM_DIM/COMBINE_DIM];
			//combine the values
			for(int j = 0; j < NUM_DIM / COMBINE_DIM; j++){
				values[j] = value_id[j];
			}	
			jclient.addPairs(
							value_id[NUM_DIM]
			                , NUM_DIM
			                , values
			                , 8
							, Index.VECTOR_BUILD
							);

			if(i % 10 == 0)
				System.out.println(i);
		}
		jclient.flush();
		jclient.closeAllIndexwriters();
//		handler.closeAllIndexwriters();
		reader.closeReader();
	
	}
	
	public void testTopKsearch() throws Throwable{
		
		COMBINE_DIM = 1;
		K = 5;
		//first, connect servers.
		jclient.connectAllServers(vec_index);
		
		//create the QueryConfig for each dimension
		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream("data/query.txt")));
		String line = "";
		int qid = 0;
		long avergetime = 0;
		while((line = buf.readLine()) != null) {
			qid++;
			String values[] = line.split(" ");
			//maximum number of range and value
			int dim_range= 128 / COMBINE_DIM;
			long value_range = 255;
			SIFTConfig config[] = new SIFTConfig[dim_range];
			for(int i = 0; i < dim_range; i++) {
				//initialize a query configuration, set query id
				config[i] = new SIFTConfig(qid);
				//set the domain
				config[i].setDimValueRange(dim_range, value_range);
				//combine the values and reduce the dimension if necessary
				int combine_values[] = new int[COMBINE_DIM];
				for(int j = 0; j < COMBINE_DIM; j++) {
					combine_values[j] = Integer.valueOf(values[i * COMBINE_DIM + j]);
				}
				config[i].num_combination = COMBINE_DIM;
				//set query 
				config[i].setQuerylong(i, combine_values);
				
				//set bi-direction search range
				config[i].setRange(10, 10);		
				//set top K
				config[i].setK(K);
			}
			
			/*
			 * this part need more modifications
			 * **/ 
			//randomly pick some data and calculate a bound
//			int bound = scan_topK_search();
			//set bound for searching
//			jclient.setBound((float)bound);
			
			//init the servers before searching
			jclient.initAllServers(Index.VECTOR_SEARCH, vec_index);
			//searching
			long starttime, endtime;
			System.out.println("searching...");
			starttime = System.currentTimeMillis();
			long index[] = jclient.answerQuery(config);
			endtime = System.currentTimeMillis();
			avergetime += (endtime - starttime);
			
			//display the results
			for(int i = 0; i < index.length; i++) {
				System.out.println(index[i]);
			}
			System.out.println("seraching time: "+(endtime - starttime)+" ms");
		}
		System.out.println("average searching time: "+ avergetime/qid+" ms");
	}
	

	int testRangeQuery() throws Throwable {
		jclient.connectAllServers(vec_index);
		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream("data/rangequery.txt")));
		String line = "";	
		int qid = 0;
		int distance = -1;
		long averTime = 0;
		while((line = buf.readLine()) != null) {
			qid++;
			int dim_range= 128 / COMBINE_DIM;
			long value_range = 255;
			SIFTConfig twoConfigs[][] = new SIFTConfig[2][dim_range];
			for(int i = 0; i < RANGE_QUERY_NUM; i++) {
				SIFTConfig config[] = twoConfigs[i];
				String values[] = line.split(" ");
				//maximum number of range and value
				for(int j = 0; j < dim_range; j++) {
					//initialize a query configuration, set query id
					config[j] = new SIFTConfig(qid);
					//set the domain
					config[j].setDimValueRange(dim_range, value_range);
					//Does not support dimension combination
					int combine_values[] = new int[1];
					combine_values[0] = Integer.valueOf(values[j]);
					//set query
					config[j].setQuerylong(j, combine_values);	
				}
				line = buf.readLine();
			}

			jclient.initAllServers(Index.RANGE_QUERY, vec_index);
			System.out.println("range querying...");
			long startTime = System.currentTimeMillis();
			long[] indexList = jclient.rangeQuery(twoConfigs[0], twoConfigs[1]);
			long endTime = System.currentTimeMillis();
			averTime += (endTime - startTime);
			System.out.println("Found "+indexList.length+" vectors:");
			for(int i = 0; i < indexList.length; i++) {
				System.out.println(indexList[i]);
			}
			System.out.println("scanning time: "+(endTime-startTime)+" ms");
		}

		System.out.println("avg time:\t"+averTime/qid);

		return distance;
	}
	
	int scan_topK_search() throws Throwable {
		jclient.connectAllServers(vec_scanfile);

		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream("data/query.txt")));
		String line = "";	
		K = 5;
		int qid = 0;
		int distance = -1;
		long averTime = 0;
		while((line = buf.readLine()) != null) {
			qid++;
			String values[] = line.split(" ");
			//maximum number of range and value
			int dim_range= 128 / COMBINE_DIM;
			long value_range = 255;
			SIFTConfig config[] = new SIFTConfig[dim_range];
			for(int i = 0; i < dim_range; i++) {
				//initialize a query configuration, set query id
				config[i] = new SIFTConfig(qid);
				//set the domain
				config[i].setDimValueRange(dim_range, value_range);
				//Does not support dimension combination
				int combine_values[] = new int[1];
				combine_values[0] = Integer.valueOf(values[i]);
				//set query
				config[i].setQuerylong(i, combine_values);	
				//set top K
				config[i].setK(K);
			}
			jclient.initAllServers(Index.VECTOR_SCAN, vec_scanfile);
			System.out.println("scanning...");
			long startTime = System.currentTimeMillis();
			ReturnValue revalue = jclient.scanQuery(config);
			long endTime = System.currentTimeMillis();
			averTime += (endTime - startTime);
			for(int i = 0; i < K; i++) {
				List<Map.Entry<Long, float[]>>list = revalue.sortedOndis();
				if(i == 0){
					distance = Math.round(list.get(0).getValue()[1]);
				}
				System.out.println(list.get(i).getKey()+"\t"+list.get(i).getValue()[1]);
			}
			System.out.println("scanning time: "+(endTime-startTime)+" ms");
		}

		System.out.println("avg time:\t"+averTime/qid);

		return distance;
	}
	
	public void distributeDatafile(String filename, int num_elements, String scan_file) throws Throwable {
		System.out.println("Partition...");
		jclient.connectAllServers(scan_file);
		reader = new Reader(filename);
		reader.openReader();
		
		jclient.initAllServers(Index.SCAN_BUILD, scan_file);
		jclient.setMaxVecNum(5000);
				
		for (int i = 0; i < num_elements; i++) {
			int[] value_id = reader.getFeature(NUM_DIM);
			
			jclient.addPairs(value_id[NUM_DIM], NUM_DIM, value_id, Index.SCAN_BUILD);
			
			if(i%10 == 0)
				System.out.println(i);
		}
		jclient.flush();
		jclient.closeAllBinwriters();
		reader.closeReader();
	}
	
	//randomly pick 5% of the original data in order to get a bound
	int scan_topK_searchLocally() throws Throwable {
		
		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream("data/query.txt")));
		String line = "";
		Reader reader = new Reader("data/datafile.bin");
		reader.openReader();
		
		int topK = 5;
		PriorityQueue<Combo> pq = new PriorityQueue<Combo>(topK, new scanComparator());
		
		long start = System.currentTimeMillis();
		int nquery = 0;
		int distance;
		System.out.println("scanning...");
		while((line = buf.readLine()) != null) {
			nquery++;
			String values[] = line.split(" ");
			int[] query = new int[values.length];
			for(int i = 0 ; i < values.length - 1; i++) {
				query[i] = Integer.valueOf(values[i]);
			}
			int picked = 0, index = 0;
			Random r = new Random();

			while(picked < vec_num) {
				int vec[] = reader.getFeature(128);
				picked++;
				distance = 0;					
				for(int j = 0; j < 128; j++){
					distance += ((query[j] - vec[j]) * (query[j] - vec[j]));
				}
				if(pq.size() < topK) 
					pq.add(new Combo(distance, index));
				else if(distance < pq.peek().distance) {
					pq.poll();
					pq.add(new Combo(distance, index));
				}
				index++;
			}
		}
		System.out.println("avg time:\t"+(System.currentTimeMillis() - start));
		int dis = pq.peek().distance;
		for(int i = 0; i < topK; i++) {
			Combo combo = pq.poll();
			System.out.println(combo.index+"\t"+combo.distance);
		}
		return dis;
	}
	
	
	public static void main(String args[]) throws Throwable {
		
		String datafile = "data/datafile.bin";
		Client c = new Client("socket://127.0.0.1:8888");
//		Client c = new Client("socket://137.132.145.132:8888");
		vec_num = 250000;
		nodes_num = 1;
		vec_index = "Index_"+nodes_num+"_"+vec_num;
		vec_scanfile = "Scanfile_"+nodes_num+"_"+vec_num+".bin";
//		
//		if(args.length > 0)
//			vec_num = Integer.valueOf(args[0]);
		debug = false;
//		long start = System.currentTimeMillis();
//		c.buildIndex(datafile, vec_num, vec_index);
//		c.distributeDatafile(datafile, vec_num, vec_scanfile);
//		System.out.println("Building Done! Time: "+(System.currentTimeMillis() - start)+"ms");
//		System.out.println();
		
//		c.testTopKsearch();
//		c.scan_topK_search();
		c.testRangeQuery();
	}
}

class SIFTConfig extends QueryConfig {
	
	public SIFTConfig(int id) {
		super(id);
		// TODO Auto-generated constructor stub
	}

	@Override
	public float calcDistance(long a, long b) {
		// TODO Auto-generated method stub
		return (a - b) * (a - b);
		//L1
		//return Math.abs(a - b);
	}

	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return QueryConfig.VECTOR;
	}
	
}

class scanComparator implements Comparator<Combo>{

	@Override
	public int compare(Combo arg0, Combo arg1) {
		// TODO Auto-generated method stub
		return arg1.distance - arg0.distance;
	}
	
}
class Combo {
	
	int distance;
	int index;
	
	Combo(int dis, int index) {
		this.distance = dis;
		this.index = index;
	}
}
