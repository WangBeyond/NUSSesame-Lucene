package jclient;

import java.net.MalformedURLException;

import lucene.Index;
import lucene.QueryConfig;
import lucene.QueryVector;
import lucene.ReturnValue;

import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

import tool.Messager;


/**
 * This class connect the master node and 
 * */
public class JClient {
	
	//for debugging
	private boolean debug = true;
	
	private InvokerLocator invokerlocator;
	private Client master; 
	private Param parameters;
	//the number of buffered vector with the purpose to reduce the communication cost
	private int maxVecNum = 100;
	private int curVecNum = 0;
	
	public JClient(String locator) {
		
		parameters = new Param();
		initMaster(locator);
	}
	
	private void initMaster(String locator) {
		
		try {
			invokerlocator = new InvokerLocator(locator);
			master = new Client(invokerlocator);
			master.connect();
			master.setSubsystem("Function");
		}catch (Throwable e) {
			System.out.println(Messager.INIT_FAIL);
		}
	}
	
	//call the functions in master node 
	private Object callMasterFunction(Param parameters) {
		
		try {
				return master.invoke(parameters);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			if(debug)
				e.printStackTrace();
			if(parameters.function_type == Param.FUNCTION_TYPE.connectAllServers_String) {
				System.out.println(Messager.CONNECT_FAIL);
				System.exit(0);
			}
			else if(parameters.function_type == Param.FUNCTION_TYPE.initAllServers_int 
					|| parameters.function_type == Param.FUNCTION_TYPE.initAllServers_int_String) {
				System.out.println(Messager.INIT_FAIL);
				System.exit(0);
			}
			else if(parameters.function_type == Param.FUNCTION_TYPE.addPairs_long_String_int
					|| parameters.function_type == Param.FUNCTION_TYPE.addPairs 
					|| parameters.function_type == Param.FUNCTION_TYPE.addPairs_long_int_Array_int) {
				System.out.println(Messager.INSERTION_FAIL);
			}
			else if(parameters.function_type == Param.FUNCTION_TYPE.changeIndexfile_String) {
				System.out.println(Messager.CHANGE_INDEX_FAIL);
			}
			else if(parameters.function_type == Param.FUNCTION_TYPE.answerQuery
					|| parameters.function_type == Param.FUNCTION_TYPE.answerStringQuery
					|| parameters.function_type == Param.FUNCTION_TYPE.getData) {
				System.out.println(Messager.SEARCH_FAIL);
			}
			else if(parameters.function_type == Param.FUNCTION_TYPE.closeAllIndexwriters) {
				System.out.println(Messager.CLOSE_INDEX_FAIL);
			}
			else if(parameters.function_type == Param.FUNCTION_TYPE.closeAllBinwriters) {
				System.out.println(Messager.CLOSE_BIN_WRITER_FAIL);
			}
			else
				System.out.println(Messager.UNKNOWN_ERROR);
		}
		return null;
	}
	
	public void setMaxVecNum(int num) {
		
		this.maxVecNum = num;
	}
	
	public void setBound(float min_lowerbound) {
		
		parameters.function_type = Param.FUNCTION_TYPE.setBound;
		parameters.param_min_lowerbound = min_lowerbound;
		this.callMasterFunction(parameters);
	}
	
	public void connectAllServers(String index_file) {
		
		parameters.function_type = Param.FUNCTION_TYPE.connectAllServers_String;
		parameters.param_String_index_file_name = index_file;
		this.callMasterFunction(parameters);
	}
	
	public void closeAllIndexwriters() {
		
		parameters.function_type = Param.FUNCTION_TYPE.closeAllIndexwriters;
		this.callMasterFunction(parameters);
	}
	
	public void closeAllBinwriters() {
		
		parameters.function_type = Param.FUNCTION_TYPE.closeAllBinwriters;
		this.callMasterFunction(parameters);
	}
	
	public void changeIndexfile(String index_file) {
		
		parameters.function_type = Param.FUNCTION_TYPE.changeIndexfile_String;
		parameters.param_String_index_file_name = index_file;
		this.callMasterFunction(parameters);
	}
	
	public void initAllServers(int type) {
		
		parameters.function_type = Param.FUNCTION_TYPE.initAllServers_int;
		parameters.param_int_type = type;
		this.callMasterFunction(parameters);
	}
	
	public void initAllServers(int type, String index_file) {
		
		parameters.function_type = Param.FUNCTION_TYPE.initAllServers_int_String;
		parameters.param_int_type = type;
		parameters.param_String_index_file_name = index_file;
		this.callMasterFunction(parameters);
	}
	
	
	public void addPairs(long element_id, String element_value, int type) {
		
		parameters.function_type = Param.FUNCTION_TYPE.addPairs_long_String_int;
		parameters.param_int_type = type;
		parameters.param_long_elementIDs.add(element_id);
		parameters.param_String_elementValues.add(element_value);
		
		//if there are enough data, then flush
		this.curVecNum++;
		if(this.curVecNum > this.maxVecNum)
			flush();
	}
	
	public void addPairs(long element_id, int ndim, long values[], int value_bilength, int type) {
		
		/*
		 * the vectors in one block can have different number of dimension
		 * but here they are required to have same type and binary length
		 * */
		parameters.function_type = Param.FUNCTION_TYPE.addPairs;
		parameters.param_long_elementIDs.add(element_id);
		parameters.param_int_ndims.add(ndim);
		parameters.param_int_type = type;
		parameters.param_int_value_bi_length = value_bilength;
		for(int i = 0; i < ndim; i++)
			parameters.param_long_values.add(values[i]);
		
		//if there are enough data, then flush
		this.curVecNum++;
		if(this.curVecNum > this.maxVecNum) 
			flush();
	}
	
	public void addPairs(long element_id, int ndim, int values_id[], int type) {
		
		parameters.function_type = Param.FUNCTION_TYPE.addPairs_long_int_Array_int;
		parameters.param_long_elementIDs.add(element_id);
		parameters.param_int_ndims.add(ndim);
		parameters.param_int_type = type;
		for(int i = 0; i < ndim+1; i++) //iterates through dim of values and an id
			parameters.param_int_values_id.add(values_id[i]);
		
		//if there are enough data, then flush
		this.curVecNum++;
		if(this.curVecNum > this.maxVecNum) 
			flush();
	}	
	public void flush() {
		
		//call the remote function on master node
		this.callMasterFunction(parameters);
		//clear the current data
		parameters.param_long_elementIDs.clear();
		if(parameters.param_int_ndims != null)
			parameters.param_int_ndims.clear();
		if(parameters.param_long_values != null);
			parameters.param_long_values.clear();
		if(parameters.param_String_elementValues != null)
			parameters.param_String_elementValues.clear();
		if(parameters.param_int_values_id != null)
			parameters.param_int_values_id.clear();
		//reset the counters
		curVecNum = 0;
	}

	public long[] answerQuery(QueryConfig config[]) {
		
		parameters.function_type = Param.FUNCTION_TYPE.answerQuery;
		parameters.qconfig = config;
		return (long[]) callMasterFunction(parameters);
	}
	
	public ReturnValue answerStringQuery(QueryConfig config[]) {
	
		parameters.function_type = Param.FUNCTION_TYPE.answerStringQuery;
		parameters.qconfig = config;
		return (ReturnValue) callMasterFunction(parameters);
	}
	
	public ReturnValue scanQuery(QueryConfig config[]) {
		
		parameters.function_type = Param.FUNCTION_TYPE.scanQuery;
		parameters.qconfig = config;
		return (ReturnValue) callMasterFunction(parameters);
	}
	
	public long[] rangeQuery(QueryVector queryVector) {
		
		parameters.function_type = Param.FUNCTION_TYPE.rangeQuery;
		parameters.query_vector = queryVector;
		return (long[]) callMasterFunction(parameters);
		
	}
	
	public String getData(long index) {
		
		parameters.function_type = Param.FUNCTION_TYPE.getData;
		parameters.param_long_elementID = index;
		return (String) callMasterFunction(parameters);
	}
	
	public void disconnectAllServers() {
		
		parameters.function_type = Param.FUNCTION_TYPE.disconnectAllServers;
		callMasterFunction(parameters);
	}
}

class AddThread implements Runnable{

	Client master;
	Param param;
	
	AddThread(Client master) {
		
		this.master = master;
	}
	
	void setParam(Param param) {
		
		this.param = param;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			master.invoke(param);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.INSERTION_FAIL);
//			e.printStackTrace();
		}
	}
	
}
