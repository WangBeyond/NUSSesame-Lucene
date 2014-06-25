package readpeer;

import jmaster.JMaster;
import tool.Messager;

public class Master {
	
	public static void main(String args[]) { 
	
		JMaster jmaster = new JMaster("Server Locators.ini");
		try {
			if(args.length == 0)
				jmaster.setLocator("socket://localhost:8888");
			else
				jmaster.setLocator(args[0]);
			jmaster.init();
			jmaster.start();
		}catch (Throwable e) {
			System.out.println(Messager.START_SERVICE_FAIL);
			e.printStackTrace();
		}
	}
}
