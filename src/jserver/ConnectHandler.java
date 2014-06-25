package jserver;

/**
 * @author huangzhi
 * This class handle the request of initialization
 * */

import javax.management.MBeanServer;

import lucene.Index;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

public class ConnectHandler implements ServerInvocationHandler {

	private Index index;
	
	ConnectHandler(Index index){
		
		this.index = index;
	}
	
	@Override
	public void addListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object invoke(InvocationRequest arg0) throws Throwable {
		// TODO Auto-generated method stub
		index.setIndexfile((String) arg0.getParameter());
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
