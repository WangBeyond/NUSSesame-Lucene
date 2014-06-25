package jserver;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import javax.management.MBeanServer;

import kv.Pair;

import lucene.Index;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.Connector;

import tool.DataProcessor;
import tool.Messager;
import vector_knn.Reader;

public class JServer {

	//default locator
	private String locator = "socket://localhost:9999";
	private Connector connector;
	//default index file name
	private String indexfile;
	
	public JServer() {

	}

	public JServer(String new_locator) {
		
		this.locator = new_locator;
	}
	
	public void setLocator(String new_locator) {
		
		this.locator = new_locator;
	}

	public void setIndexFilename(String filename) {
	
		this.indexfile = filename;
	}
	
	public void init() throws Throwable {
		
		InvokerLocator myLocator = new InvokerLocator(locator);
		connector = new Connector();
		connector.setInvokerLocator(myLocator.getLocatorURI());
		connector.create();
		//create a index for Top-k Search
		Index index = new Index();
		//create a index for simple search
		Index simple_index = new Index();
		//add handlers for invocations
		connector.addInvocationHandler("Add", new AddDocHandler(index));
		connector.addInvocationHandler("Query", new QueryHandler(index));
		connector.addInvocationHandler("Connect", new ConnectHandler(index));
		connector.addInvocationHandler("CloseWriter", new CloseWriterHandler(index));
		connector.addInvocationHandler("Init", new InitHandler(index));
		//add a handler for simple search 
		connector.addInvocationHandler("SimpleSearch", new SimpleSearchHandler(simple_index));
		
		System.out.println("The Server is initialized.");
	}

	public void start() throws Throwable{
		connector.start();
		System.out.println("The server is on...");
	}

	public void stop() {
		connector.stop();
		System.out.println("The server is off.");
	}
	
	/**
	 * for testing purpose
	 * @throws Throwable 
	 * */
//	public static void main(String args[]) throws Throwable {
//		
//		int num_elements = Integer.valueOf(1000);
//		if(args.length != 0)
//			num_elements = Integer.valueOf(args[0]);
//		
//		String vec_index = "Index"+num_elements;
//		long time = 0,total = System.currentTimeMillis();
//		
//		Index index = new Index();
//		index.setIndexfile(vec_index);
//		index.init_building();
//		Reader reader = new Reader("data/siftgeo.bin");
//		reader.openReader();
//	
//		int NUM_DIM = 128;
//		int COMBINE_DIM = 1;	
//		
//		ArrayList<Pair> pairlist = new ArrayList<Pair>(); 
//		
//		//num_elements indicate the number of the test feature
//		for (int i = 0; i < num_elements; i++) {
//			int value_id[];
//			value_id = reader.getFeature(NUM_DIM);
//			//combine the values
//			long long_value[] = new long[NUM_DIM / COMBINE_DIM];
//			long_value = DataProcessor.combineSiftValues(value_id, NUM_DIM, COMBINE_DIM);
//			for(int j = 0; j < NUM_DIM / COMBINE_DIM; j++){
////				index.addDoc(value_id[NUM_DIM], long_value[j]);
//				pairlist.add(new Pair(value_id[NUM_DIM], long_value[j]));
//			}	
//			
////			System.out.println(i);
//		}
//		//sort the list
//		Collections.sort(pairlist, new Comparator<Pair>(){
//			public int compare(Pair p1, Pair p2) {   
//		    	return (int) (p1.value_long - p2.value_long);
//		    }
//		});
//		
//		
//		
//		for(int j = 0;j < pairlist.size(); j++) {
//			Pair p = pairlist.get(j);
////			System.out.println(p.id_long + "\t" + p.value_long);
//			long start = System.currentTimeMillis();
//			index.addDoc(p.id_long, p.value_long);
//			time += System.currentTimeMillis() - start;
//		}
//	
//		System.out.println("Building time:\t"+time);
//		index.closeWriter();
//		reader.closeReader();
//		
//		System.out.println("Total time:\t"+(System.currentTimeMillis() - total));
//	}
}