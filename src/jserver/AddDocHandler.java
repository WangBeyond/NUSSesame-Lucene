/**
 * @author huang zhi
 * handle the invocation for adding the doc to index
 * */

package jserver;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lucene.Index;

import javax.management.MBeanServer;

import kv.*;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

public class AddDocHandler implements ServerInvocationHandler {

	private Index index;

	AddDocHandler(Index index) throws Throwable{
		
		this.index = index;
	}
	
	@Override
	public void addListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	/**
	 * Use this method to add the Key-Value pairs into the index
	 * */
	public Object invoke(InvocationRequest arg0) throws Throwable {
		// TODO Auto-generated method stub
		Object object = arg0.getParameter();
		List<Pair> list = (List<Pair>)object;
		
		Pair pair;
		for(int i = 0; i < list.size(); i++) {
			pair = list.get(i);
			if(pair.getType() == Index.VECTOR_BUILD)
				index.addDoc(pair.id_long, pair.values_long);
			else
				index.addDoc(pair.id_long, pair.value_string);
		}
		return 1;
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

