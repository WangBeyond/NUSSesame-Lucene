package readpeer;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import tool.Messager;
import jserver.*;

public class Server {
	public static void main(String args[]) {
		
		JServer jserver = new JServer();
		try {
			if(args.length == 0)
				jserver.setLocator("socket://localhost:9999");
			else
				jserver.setLocator(args[0]);
			jserver.init();
			jserver.start();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.START_SERVICE_FAIL);
			e.printStackTrace();
		}
	}
}
