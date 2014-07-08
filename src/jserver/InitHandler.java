package jserver;

import javax.management.MBeanServer;

import lucene.Index;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

public class InitHandler implements ServerInvocationHandler{

	private Index index;
	
	public InitHandler(Index index) throws Throwable {
		
		this.index = index;
	}
	
	@Override
	public void addListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object invoke(InvocationRequest arg0) throws Throwable {
		// TODO Auto-generated method stub
		int param[] = new int[2];
		param = (int[]) arg0.getParameter();
		int type = param[0], num_nodes = param[1];
		
		if(type == Index.VECTOR_BUILD || type == Index.STRING_BUILD) {
			index.init_building();
		} else if(type == Index.STRING_SEARCH){
			index.init_query();
		} else if(type == Index.VECTOR_SEARCH) {
			index.init_query();
		} else if(type == Index.SCAN_BUILD) {
			index.init_binwriter();
		} else if(type == Index.VECTOR_SCAN) {
			index.init_scan();
		} else if(type == Index.RANGE_QUERY) {
			index.init_query();		
		}else {
			System.out.println("Initialization error: invalid invocation type");
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

}
