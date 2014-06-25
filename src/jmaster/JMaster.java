package jmaster;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import jserver.AddDocHandler;
import jserver.CloseWriterHandler;
import jserver.ConnectHandler;
import jserver.InitHandler;
import jserver.QueryHandler;
import jserver.SimpleSearchHandler;
import kv.Key;
import kv.Pair;
import kv.Value;
import lucene.Index;
import lucene.QueryConfig;
import lucene.ReturnValue;

import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;

import tool.DataProcessor;
import tool.Messager;


/**
 * @author huangzhi 
 * This is the master node of the distributed system.
 * It connects all the slave nodes and the client.
 * 
 * */

public class JMaster {

	//for debug
	public static boolean debug = false;
	
	//for the client to call the master
	//default locator
	private String locator = "socket://localhost:8888";
	private Connector connector;
	private String ipfile;
		
	/**
	 * constructor
	 * @param ipfile contains the IP address of each server
	 * @throws Throwable 
	 * */
	public JMaster (String ipfile) {
		this.ipfile = ipfile;
	}
	
	public void setLocator(String locator) {
		this.locator = locator;
		System.out.println("master locator "+locator);
	}
	
	/**
	 * initialize the master node
	 * */
	public void init() throws Throwable {
		
		InvokerLocator myLocator = new InvokerLocator(locator);
		connector = new Connector();
		connector.setInvokerLocator(myLocator.getLocatorURI());
		connector.create();
		//add handlers for invocations
		connector.addInvocationHandler("Function", new FunctionHandler(this.ipfile));
		
		System.out.println("The Master node is initialized.");
	}
	
	/**
	 * start listening
	 * @throws Throwable 
	 * */
	public void start() throws Throwable {
		
		connector.start();
		System.out.println("The master is on...");
	}
	
	public static void main(String[] args) {
		
		JMaster jmaster = new JMaster("Server Locators.ini");
		try {
			jmaster.setLocator(args[0]);
			jmaster.init();
			jmaster.start();
		}catch (Throwable e) {
			System.out.println(Messager.START_SERVICE_FAIL);
			e.printStackTrace();
		}
	}
	

}





