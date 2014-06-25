package vector_knn;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import tool.Messager;
import jserver.*;

public class Server {
	public static void main(String args[]) {
		
		if(args.length == 0) {
			System.out.println("Argument Error: Need the Server Node locator.");
			System.exit(-1);
		}
		JServer jserver = new JServer();
		try {
			jserver.setLocator(args[0]);
			jserver.init();
			jserver.start();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.START_SERVICE_FAIL);
			System.out.println("Please check the input parameter.");
//			e.printStackTrace();
		}
	}
}
