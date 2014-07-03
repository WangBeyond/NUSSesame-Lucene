package jserver;
/**
 * @author huang zhi
 * this is a handler for closing the index
 * */


import javax.management.MBeanServer;

import lucene.Index;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

public class CloseWriterHandler  implements ServerInvocationHandler {

	Index index;
	CloseWriterHandler(Index index) {
		this.index = index;
	}
	
	@Override
	public void addListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object invoke(InvocationRequest arg0) throws Throwable {
		// TODO Auto-generated method stub
		Object object = arg0.getParameter();
		Integer type = (Integer)object;
		if(type == Index.VECTOR_BUILD) {
			index.closeWriter();
			System.out.println("The index is closed");
		} else {
			index.closeBinwriter();
			System.out.println("The binary writer is closed");
		}
		return "The index is closed";
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
