package vector_knn;

import jmaster.JMaster;
import tool.Messager;

public class Master {
	
	private static boolean debug = true;
	
	public static void main(String args[]) { 
	
		if(args.length == 0) {
			System.out.println("Argument Error: Need the Master Node locator.");
			System.exit(-1);
		}
		JMaster jmaster = new JMaster("Server Locators.ini");
		try {
			System.out.println("Master Node Maximum Mem: "+Runtime.getRuntime().maxMemory()/1000000+"MB");
			jmaster.setLocator(args[0]);
			jmaster.init();
			jmaster.start();
		}catch (Throwable e) {
			System.out.println(Messager.START_SERVICE_FAIL);
			System.out.println("Please check the input parameter.");
			if (debug) {
				e.printStackTrace();
			}
		}
	}
}
