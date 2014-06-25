package readpeer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import jclient.JClient;
import jclient.Param;
import tool.Analyzer;
import tool.DataProcessor;
import tool.Messager;
import lucene.Index;
import lucene.QueryConfig;
import lucene.ReturnValue;

public class Client {

	//flags
	static boolean debug = false;
	
	static String[] ips;
	static int num_ip;
	static final int MAX_ip = 128;
	static JClient jclient;
	
	// name for index file
	static String string_index = "String index";
	// name for data file
	static String passage = "data/annotation_dataset_0.txt";
	

	//to call the functions in master node
	public Param parameter;
	
	//Top K
	private int K = 1;
	
	public Client(String locator) {

		jclient = new JClient(locator);
		// connect all servers
		// content index is the default index
		jclient.connectAllServers(string_index);	
	}
		
	/**
	 * test the insertion
	 * @throws IOException 
	 * */
	private void testInsertion(String filename, String index_file) throws Throwable {
		
		// initialization for building index
		jclient.initAllServers(Index.STRING_BUILD, index_file);
		//read the data
		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
		String line;
		String id;
		jclient.setMaxVecNum(500);
		while((line = buf.readLine()) != null) {
			id = line;
			line = DataProcessor.getGrams(3, buf.readLine());
			jclient.addPairs(Long.valueOf(id), line, Index.STRING_BUILD);
		}
		jclient.flush();
		//we have to close the writer so that it can 
		jclient.closeAllIndexwriters();
		buf.close();
	}
	
	/**
	 * test query
	 * @param String for query
	 * */
	private void testQuery(String qstr, String index_file, int K) {
		
		//initialize the query process
		jclient.initAllServers(Index.STRING_SEARCH, index_file);	
		//set the query configurations
		//get grams
		String qgrams[] = DataProcessor.getGrams(3, qstr).split(" ");
		int num = qgrams.length;
		//set attributes
		STRConfig configs[] = new STRConfig[num];
		for(int i = 0; i < num; i++) {
			if(debug)
				System.out.println("qgrams: "+qgrams[i]);
			configs[i] = new STRConfig(0, qgrams[i]);
			configs[i].setK(K);
		}
		ReturnValue revalue = jclient.answerStringQuery(configs);
		int result_num = Math.min(K, revalue.topk_count.size());
		//display the result
		System.out.println("200: ");
		for(int i = 0; i < revalue.topk_count.size(); i++) {
			System.out.print("index: "+revalue.topk_index.get(i)+"\t");
			System.out.print("count: "+revalue.topk_count.get(i)+"\n");
			System.out.println(revalue.topk_list.get(i));
		}

	}

	
	
	public static void main(String[] args) throws Throwable  {
		
//		if (args.length < 3) {
//			System.out.println(Messager.BAD_REQUEST);
//			System.exit(-1);
//		}		
		
		debug = false;
		long start = System.currentTimeMillis();
		Client client = new Client("socket://127.0.0.1:8888");
		
		/*if (args[0].equals("-index")) {
			System.out.println("building...");
			try {
				client.testInsertion(args[1], args[2]);
			} catch (Throwable e) {
				System.out.println(Messager.INSERTION_FAIL);
				e.printStackTrace();
				System.exit(-1);
			}
			System.out.println("building...Done: " + (System.currentTimeMillis() - start));
		}

		else if (args[0].equals("-search")) {
			client.testQuery(args[1], args[2], Integer.valueOf(args[3]));
		}*/
//		System.out.println("Building...");
//		client.testInsertion(passage, string_index);
//		System.out.println("Build Done.");
		System.out.println("Searching...");
		client.testQuery(" 会被截掉 ", string_index, 10);
		
		client.jclient.disconnectAllServers();	
	}
}

class STRConfig extends QueryConfig {

	public STRConfig(int i, String string) {
		// TODO Auto-generated constructor stub
		super(i, string);
	}

	@Override
	public float calcDistance(long a, long b) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return QueryConfig.STRING;
	}
	
}