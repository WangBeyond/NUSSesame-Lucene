package jserver;

import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;

import kv.*;

import lucene.Index;
import lucene.QueryConfig;
import lucene.QueryVector;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

public class QueryHandler implements ServerInvocationHandler {

	private Index index;	
	
	QueryHandler (Index index) throws Throwable {
		
		this.index = index;
	}
	
	@Override
	public void addListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@SuppressWarnings("unchecked object cast")
	@Override
	public Object invoke(InvocationRequest arg) throws Throwable {

		// TODO Auto-generated method stub	
		try {
			@SuppressWarnings("unchecked")
			List<QueryConfig> qlist = (List<QueryConfig>) arg.getParameter();
			return index.generalSearch(qlist);
		} catch(ClassCastException e) {
			@SuppressWarnings("unchecked")
			QueryVector queryVector = (QueryVector) arg.getParameter();
			return index.rangeQuery(queryVector);
		}
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

}
