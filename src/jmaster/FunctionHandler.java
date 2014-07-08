  package jmaster;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.lang.String;

import javax.management.MBeanServer;

import jclient.Param;
import kv.Pair;
import lucene.Index;
import lucene.QueryConfig;
import lucene.ReturnValue;

import org.jboss.remoting.Client;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

import tool.DataProcessor;
import tool.Messager;

public class FunctionHandler implements ServerInvocationHandler {

	//for debug
	private static boolean debug = true;
	//for testing
	private static long time = 0;
	
	//sleep time when waiting for the slave nodes
	private static long SLEEP_TIME = 50;
	//the bound passed from upper layer
	private static float MIN_LOWER_BOUND = Float.MIN_VALUE;
	
	//to manage the salve nodes 
	private InvokerLocator invokerlocator;
	private Vector<Client> machines;
	
	//data queues, each machine has one queue
	private List<List<Pair>> pair_queues;
	
	//create a thread pool to distribute the tasks
	private ExecutorService threadPool;
	int fixed_thread_num = 8;
	
	public FunctionHandler (String ipfile) {
		this.setLocators(ipfile);
		pair_queues = new ArrayList<List<Pair>>();
		for(int i = 0; i < machines.size(); i++) 
			pair_queues.add(new ArrayList<Pair>());
		//a fixed thread pool for building the index
		threadPool = Executors.newFixedThreadPool(fixed_thread_num);
	}
	
	@Override
	public void addListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object invoke(InvocationRequest arg0) throws Throwable {
		// TODO Auto-generated method stub
		Param parameter = (Param)arg0.getParameter();
		//call different functions on master node based on the parameter type
		if(parameter.function_type == Param.FUNCTION_TYPE.connectAllServers_String) {
			this.connectAllServers(parameter.param_String_index_file_name);
		}
		
		if(parameter.function_type == Param.FUNCTION_TYPE.initAllServers_int_String)
			this.initAllServers(parameter.param_int_type, parameter.param_String_index_file_name);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.initAllServers_int)
			this.initAllServers(parameter.param_int_type);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.changeIndexfile_String)
			this.changeIndexfile(parameter.param_String_index_file_name);
		
//		if(parameter.function_type == Param.FUNCTION_TYPE.addPair_long_long_int)
//			this.addPair(parameter.param_long_elementID, parameter.param_long_elementValue, parameter.param_int_type);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.addPairs)
			this.addPairs(parameter.param_long_elementIDs, parameter.param_int_ndims, parameter.param_long_values,
					parameter.param_int_value_bi_length, parameter.param_int_type);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.addPairs_long_String_int) 
			this.addPairs(parameter.param_long_elementIDs, parameter.param_String_elementValues, parameter.param_int_type);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.addPairs_long_int_Array_int) 
			this.addPairs(parameter.param_long_elementIDs, parameter.param_int_ndims, parameter.param_int_values_id, parameter.param_int_type);

		if(parameter.function_type == Param.FUNCTION_TYPE.answerQuery)
			return this.answerQuery(parameter.qconfig);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.answerStringQuery) 
			return this.answerStringQuery(parameter.qconfig);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.scanQuery)
			return this.scanQuery(parameter.qconfig);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.rangeQuery)
			return this.rangeQuery(parameter.qconfig);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.getData)
			return this.getData(parameter.param_long_elementID);
		
		if(parameter.function_type == Param.FUNCTION_TYPE.closeAllIndexwriters)
			this.closeAllIndexwriters();;
		
		if(parameter.function_type == Param.FUNCTION_TYPE.closeAllBinwriters)
			this.closeAllBinwriters();
		
		if(parameter.function_type == Param.FUNCTION_TYPE.disconnectAllServers){
			this.disconnectAllServers();
		}
			
		if(parameter.function_type == Param.FUNCTION_TYPE.setBound) {
			this.MIN_LOWER_BOUND = parameter.param_min_lowerbound;
			System.out.println("A new bound is set:\t"+this.MIN_LOWER_BOUND);
		}
		
		return null;
	}

	@Override
	public void removeListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setInvoker(ServerInvoker arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setMBeanServer(MBeanServer arg0) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * set the location of each Server
	 * */
	private void setLocators(String ipfile) {
		
		machines = new Vector <Client>();
		try {
			BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream(ipfile)));
			String line = "";
			while ((line = buf.readLine()) != null && !line.trim().equals("")) {
				invokerlocator = new InvokerLocator(line);
				machines.add(new Client(invokerlocator));
				System.out.println(line); 
			}
			System.out.println("machine num "+machines.size());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.SET_LOCATOR_FAIL);
			System.exit(0);
			if(debug)
				e.printStackTrace();
		}
	}
	
	/**
	 * change index
	 * */
	public void changeIndexfile (String index_file) {
		
		for(int i = 0; i < machines.size(); i++) {
			
			machines.elementAt(i).setSubsystem("Connect");
			try {
				machines.elementAt(i).invoke(index_file);
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				System.out.println(Messager.CHANGE_INDEX_FAIL);
				if(debug)
					e.printStackTrace();
			}
		}
	}
	
	/**
	 * connect all the Servers
	 * */
	public void connectAllServers(String index_file) {
		
		disconnectAllServers();
		for(int i = 0; i < machines.size(); i++ ) {
			connectServer(i, index_file);
		} 
	}
	
	/**
	 * connect specific Server
	 * */
	public void connectServer(int id, String index_file) {
		
		try {
			machines.elementAt(id).setSubsystem("Connect");
			machines.elementAt(id).connect();
			System.out.println(id);
			machines.elementAt(id).invoke(index_file);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.CONNECT_FAIL);
			if(debug) {
				System.err.println("Server: "+id+" Connection Failure.");
				e.printStackTrace();
				System.exit(0);
			}
			System.exit(0);
		}
		if(debug)
			System.out.println("Server: "+id+" Successfully Connected!");
	}

	/**
	 * initialization for all Servers
	 * */
	public void initAllServers(int type, String indexfile) {

		for(int i = 0; i < machines.size(); i++ ) {
			initServer(i, type, indexfile);
		}
	}
	
	public void initAllServers(int type) {

		for(int i = 0; i < machines.size(); i++ ) {
			initServer(i, type);
		}
	}
	
	/**
	 * initialization for specific Server
	 * declare new index file
	 * */
	public void initServer(int id, int type, String indexfile) {
		
		
		try {
			machines.elementAt(id).setSubsystem("Connect");
			machines.elementAt(id).invoke(indexfile);
			machines.elementAt(id).setSubsystem("Init");
			//we have to send two parameters to the slave nodes
			int param[] = new int[2];
			//one is the operation type, search or build
			param[0] = type;
			//the other is the number of the slave nodes which is used for mapping
			param[1] = machines.size();
			machines.elementAt(id).invoke(param);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.INIT_FAIL);
			if(debug)
				e.printStackTrace();
			System.exit(0);
		}
	}
	
	/**
	 *without declaring new index file
	 * */
	public void initServer(int id, int type) {
		
		try {
			machines.elementAt(id).setSubsystem("Init");
			//we have to send two parameters to the slave nodes
			int param[] = new int[2];
			//one is the operation type, search or build
			param[0] = type;
			//the other is the number of the slave nodes which is used for mapping
			param[1] = machines.size();
			machines.elementAt(id).invoke(param);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.INIT_FAIL);
			if(debug)
				e.printStackTrace();
			System.exit(0);
		}
	}
	
	/**
	 * add id-value pair and build the index
	 * for string 
	 * need modifications
	 **/
	public void addPairs(List<Long> element_ids, List<String> values, int type) {
		
		int id;
		for(int i = 0; i < element_ids.size(); i++) {
			id = Strategy.distributeTask(machines.size(), element_ids.get(i));
			this.pair_queues.get(id).add(new Pair(element_ids.get(i), values.get(i), type));
		}
		flush();
	}

	
	/**
	 * we can insert the whole vector each time.
	 * @param ndims: an list of the number of dimensions
	 * */
	public void addPairs(List<Long> element_ids, List<Integer> ndims, List<Long> values,
			int value_bilength, int type) {

		int value_index = 0, elem_dim;
		long elem_id, value;
		for(int i = 0; i < element_ids.size(); i++) {
			elem_id = element_ids.get(i);
			elem_dim = ndims.get(i);
			long values_long[] = new long[elem_dim];
			for(int j = 0; j < elem_dim; j++) {
				values_long[j] = DataProcessor.generateKey(j, values.get(value_index), value_bilength);
				value_index++;
			}
			this.pair_queues.get(Strategy.distributeTask(machines.size(), elem_id))
				.add(new Pair(elem_id, values_long, type));
		}
		flush();
	}
	
	
	/**
	 * add id-value pair and write the scanned binary file
	 * for linear scan
	 **/
	public void addPairs(List<Long> element_ids, List<Integer> ndims, List<Integer> values_id, int type) {
		
		int value_index = 0, elem_dim;
		long elem_id;
		for(int i = 0; i < element_ids.size(); i++) {
			elem_id = element_ids.get(i);
			elem_dim = ndims.get(i);
			int values_id_int[] = new int[elem_dim + 1];
			for(int j = 0; j < elem_dim+1; j++) {
				values_id_int[j] = values_id.get(value_index);
				value_index++;
			}
			this.pair_queues.get(Strategy.distributeTask(machines.size(), elem_id))
				.add(new Pair(elem_id, values_id_int, type));
		}
		flush();
	}
	
	/**
	 * flush the data queue and call the remote function
	 * @throws Throwable 
	 * */
	public void flush() {
		
		//sending the data and call the remote function
		List<Pair> list;
		//for parallelization 
		Vector<Future<Integer>> future_vec = new Vector<Future<Integer>>();
		
		for(int i = 0; i < pair_queues.size(); i++) {
			machines.elementAt(i).setSubsystem("Add");
			list = pair_queues.get(i);
			AddDocTask addtask = new AddDocTask(machines, i, list);
			Future<Integer> future = threadPool.submit(addtask);
			future_vec.add(future);
		}
		//wait until all the tasks are done
		int num_done = 0;
		while (num_done < pair_queues.size()) {
			//sleep for some time
			try {
				Thread.sleep(SLEEP_TIME);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("System Errors.");
				if(debug)
					e.printStackTrace();
			}
			for(int i = 0;i < future_vec.size(); i++) {
				if(future_vec.elementAt(i) != null && future_vec.elementAt(i).isDone()) {
					num_done++;
					future_vec.set(i, null);
				}
			}
		}
		for (int i = 0; i < pair_queues.size(); i++) { 
			//clear the data queues
			pair_queues.get(i).clear();
		}
	}
	
	/**
	 * answer the string query
	 * master node collect the local TopK results.
	 * */
	public ReturnValue answerStringQuery(QueryConfig qconfigs[]) {
		
		ReturnValue revalue = new ReturnValue();
		try {
			revalue = this.getReturnValueByNode(qconfigs);
		} catch (Throwable e) {
			
			System.out.println(Messager.SEARCH_FAIL);
			if(debug)
			// TODO Auto-generated catch block
				e.printStackTrace();
		}
		// sort the result based on their count
		long tempindex;
		String tempstring;
		int tempcount;
		boolean sorted = false;
		for(int i = 0; i < revalue.topk_count.size(); i++) {
			sorted = true;
			for (int j = 0; j < revalue.topk_count.size() - i - 1; j++) {
				if(revalue.topk_count.get(j) < revalue.topk_count.get(j + 1)){
					sorted = false;
					//exchange count
					tempcount = revalue.topk_count.get(j).intValue();
					revalue.topk_count.set(j, revalue.topk_count.get(j + 1).intValue());
					revalue.topk_count.set(j + 1, tempcount);
					//change index
					tempindex = revalue.topk_index.get(j).longValue();
					revalue.topk_index.set(j, revalue.topk_index.get(j + 1).longValue());
					revalue.topk_index.set(j + 1, tempindex);
					//change string
					tempstring = new String(revalue.topk_list.get(i));
					revalue.topk_list.set(j, new String(revalue.topk_list.get(j + 1)));
					revalue.topk_list.set(j + 1, tempstring);
				}
			}
			if(sorted)
				break;
		}
		return revalue;
	}
	
	/**
	 * answer the query
	 * master node collect the local topK results from slave nodes
	 * then select the global TopK
	 * */
	public long[] answerQuery(QueryConfig qconfigs[]) {
		
		long[] index = new long[qconfigs[0].getK()];
//		Candidates candidates = null;
		ReturnValue revalue = new ReturnValue();
		try {
			revalue = this.getReturnValueByNode(qconfigs);
		} catch (Throwable e) {
			
			System.out.println(Messager.SEARCH_FAIL);
			if(debug)
			// TODO Auto-generated catch block
				e.printStackTrace();
		}
		List<Map.Entry<Long, float[]>>list = revalue.sortedOndis();
		for(int i = 0; i < qconfigs[0].getK(); i++)
			index[i] = list.get(i).getKey();
		return index;
	}
	
	
	/**
	 * scan the vectors without index structure
	 * master node collects the local topK results from slave nodes
	 * then select the global TopK
	 */
	public ReturnValue scanQuery(QueryConfig qconfigs[]) {

		ReturnValue revalue = new ReturnValue();
		try {
			revalue = this.getReturnValueByScanningNode(qconfigs);
		} catch (Throwable e) {
			
			System.out.println(Messager.SCAN_FAIL);
			if(debug)
			// TODO Auto-generated catch block
				e.printStackTrace();
		}
		return revalue;
	}
	
	
	/**Use indexing to answer range query
	 * master node collects the results with the range constrained by the two queries
	 */
	public long[] rangeQuery(QueryConfig qconfigs[]) {
//		Candidates candidates = null;
//		Candidates candidates = null;
		ReturnValue revalue = new ReturnValue();
		try {
			revalue = this.getReturnValueByNode(qconfigs);
		} catch (Throwable e) {
			
			System.out.println(Messager.SEARCH_FAIL);
			if(debug)
			// TODO Auto-generated catch block
				e.printStackTrace();
		}
		List<Map.Entry<Long, float[]>>list = revalue.sortedOndis();
		long[] index = new long[list.size()];
		for(int i = 0; i < list.size(); i++)
			index[i] = list.get(i).getKey();
		return index;
	}
	
	
	/**
	 * distribute the query task and search in parallel
	 * one task defined as all the queries on one slave node
	 * when the number of slave nodes is small, we can decrease 
	 * the cost on remote function calling
	 * */
	private ReturnValue getReturnValueByNode(QueryConfig qconfigs[]) throws Throwable {
		
		// final result
		ReturnValue result = new ReturnValue();
		
		// set sub system
		for(int i = 0; i < machines.size(); i++)
			machines.elementAt(i).setSubsystem("Query");
		//create a thread pool for queries
		ExecutorService executor = Executors.newFixedThreadPool(fixed_thread_num);
		
		List<QueryConfig> list = new ArrayList<QueryConfig>();	
		for(int i = 0;i < qconfigs.length; i++)
			list.add(qconfigs[i]);
		// for vector search, we need to
		if(qconfigs[0].getType() == Index.VECTOR_SEARCH) {
			//sort the query configs by their dimension 
			//O(n)
			for(int i = 0;i < qconfigs.length; i++)
				list.set(qconfigs[i].getDim(), qconfigs[i]);
		}
		Vector<Future<ReturnValue>> future_vec = new Vector<Future<ReturnValue>>();
		int num_of_tasks = 0;
		// submit the task to threadpool
		for(int i = 0; i < machines.size(); i++) {
			num_of_tasks++;
			Future<ReturnValue> future = executor.submit(
					new NodeTask(machines.elementAt(i), list));
			future_vec.add(future);
		}
		
		long start = System.currentTimeMillis();
		long merge_time = 0;
		// wait for all the tasks are done
		int num_of_returned = 0;
		while(num_of_returned < num_of_tasks) {
			Thread.sleep(SLEEP_TIME);
			for(int i = 0; i < future_vec.size(); i++) {
				if(future_vec.elementAt(i) != null && future_vec.elementAt(i).isDone()) {
					num_of_returned++;
					ReturnValue revalue = future_vec.elementAt(i).get();
					System.out.println("node "+i+" finish time "+(System.currentTimeMillis()-start)+" ms");
					// merge the results from different nodes
					long merge_start = System.currentTimeMillis();
					result.merge(revalue);
					merge_time += System.currentTimeMillis() - merge_start;
					// clear the task
					future_vec.setElementAt(null, i);
				}
			}
		}
		System.out.println("Merge time in function:\t"+merge_time+" ms");
		System.out.println("Wait for return:\t"+(System.currentTimeMillis() - start - merge_time+" ms"));
		executor.shutdown();
		return result;
	}
	
	
	
	/**
	 * distribute the query task and search in parallel
	 * one task defined as all the queries on one slave node
	 * when the number of slave nodes is small, we can decrease 
	 * the cost on remote function calling
	 * */
	private ReturnValue getReturnValueByScanningNode(QueryConfig qconfigs[]) throws Throwable {
		
		// final result
		ReturnValue result = new ReturnValue();
		
		// set sub system
		for(int i = 0; i < machines.size(); i++)
			machines.elementAt(i).setSubsystem("Scan");
		//create a thread pool for queries
		ExecutorService executor = Executors.newFixedThreadPool(fixed_thread_num);
		
		List<QueryConfig> list = new ArrayList<QueryConfig>();	
		for(int i = 0;i < qconfigs.length; i++)
			list.add(qconfigs[i]);
		// for vector search, we need to
		if(qconfigs[0].getType() == Index.VECTOR_SEARCH) {
			//sort the query configs by their dimension 
			//O(n)
			for(int i = 0;i < qconfigs.length; i++)
				list.set(qconfigs[i].getDim(), qconfigs[i]);
		}
		Vector<Future<ReturnValue>> future_vec = new Vector<Future<ReturnValue>>();
		int num_of_tasks = 0;
		// submit the task to threadpool
		for(int i = 0; i < machines.size(); i++) {
			num_of_tasks++;
			Future<ReturnValue> future = executor.submit(
					new NodeTask(machines.elementAt(i), list));
			future_vec.add(future);
		}
		
		long start = System.currentTimeMillis();
		long merge_time = 0;
		// wait for all the tasks are done
		int num_of_returned = 0;
		while(num_of_returned < num_of_tasks) {
			Thread.sleep(SLEEP_TIME);
			for(int i = 0; i < future_vec.size(); i++) {
				if(future_vec.elementAt(i) != null && future_vec.elementAt(i).isDone()) {
					num_of_returned++;
					ReturnValue revalue = future_vec.elementAt(i).get();
					System.out.println("node "+i+" finish time "+(System.currentTimeMillis()-start)+" ms");
					// merge the results from different nodes
					long merge_start = System.currentTimeMillis();
					result.merge(revalue);
					merge_time += System.currentTimeMillis() - merge_start;
					// clear the task
					future_vec.setElementAt(null, i);
				}
			}
		}
		System.out.println("Merge time in function:\t"+merge_time+" ms");
		System.out.println("Wait for return:\t"+(System.currentTimeMillis() - start - merge_time+" ms"));
		executor.shutdown();
		return result;
	}
	
	
	/**
	 * get the data using the index
	 * */
	public String getData(long id) {
		
		String data = null;
		//get the server
		int server_id = (int) id % machines.size();
		//set the sub-system of the server
		machines.elementAt(server_id).setSubsystem("SimpleSearch");
		try {
			//invoke the searching function
			data = (String)machines.elementAt(server_id).invoke(id);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.SEARCH_FAIL);
			if(debug)
				e.printStackTrace();
		}
		return data;
	}
	
	/**
	 * close the Index writer after building the index
	 * */
	public void closeIndexwriter(int id) {
		
		machines.elementAt(id).setSubsystem("CloseWriter");
		try {
			machines.elementAt(id).invoke(Index.VECTOR_BUILD);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.CLOSE_INDEX_FAIL);
			if(debug)
				e.printStackTrace();
			System.exit(0);
		
		}
	}
	
	public void closeAllIndexwriters()  {
		
		this.flush();
		for(int i = 0;i < machines.size(); i++)
			this.closeIndexwriter(i);	
	}
	
	public void closeBinwriter(int id) {
		
		machines.elementAt(id).setSubsystem("CloseWriter");
		try {
			machines.elementAt(id).invoke(Index.SCAN_BUILD);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.CLOSE_BIN_WRITER_FAIL);
			if(debug)
				e.printStackTrace();
			System.exit(0);
		
		}
	}
	
	public void closeAllBinwriters() {
		
		this.flush();
		for(int i = 0;i < machines.size(); i++)
			this.closeBinwriter(i);
	}
	
	/**
	 * disconnect all the servers
	 * */
	public void disconnectAllServers() {
		
		for(int i = 0; i < machines.size(); i++ ) {
			disconnectServer(i);
		}
	}
	
	/**
	 * disconnect specific server
	 * */
	public void disconnectServer(int id){
		
		machines.elementAt(id).disconnect();
		if(debug)
			System.out.println(id+" :Disconnected!");	
	}
	
}


/**
 * This class defines some static function for distributed the task
 * */
class Strategy {
	
	/*
	 * the following are different kinds of distributed strategy
	 * */
	static int distributeTask(int total, String strvalue) {
		
		return strvalue.hashCode() % total;
	}
	
	static int distributeTask(int total, long id) {
		
		return (int) (id % total);
	}
	
}


class NodeTask implements Callable<ReturnValue> {

	private Client machine;
	private List<QueryConfig> querylist;
	
	NodeTask(Client machine,List<QueryConfig> qlist) {
		
		this.machine = machine;
		this.querylist = qlist;
	}
	

	@SuppressWarnings("unchecked cast")
	@Override
	public ReturnValue call() throws Exception {
		// TODO Auto-generated method stub
		ReturnValue revalue = null;
		try {
			revalue = (ReturnValue) machine.invoke(this.querylist);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println("There are problems in NodeTask.");
			e.printStackTrace();
		}
		return revalue;
	}
	
}

/**
 * this class is used for distributing the documents to build index
 * */
class AddDocTask implements Callable<Integer>{

	private Vector<Client> machines;
	private int id;
	private List<Pair> list;
	
	public AddDocTask(Vector<Client> machines, int id, List<Pair> list ) {
		
		this.machines = machines;
		this.id = id;
		this.list = list;
	}
	
	@Override
	public Integer call() throws Exception {
		// TODO Auto-generated method stub
		try {
			System.out.println("Add Doc Task in id:\t"+id);
			return (Integer) machines.elementAt(id).invoke(list);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			if(JMaster.debug)
				e.printStackTrace();
			return -1;
		}
	}
	
}
