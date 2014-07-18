package jsch;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import tool.Messager;


public class DistributedManager {

	private boolean debug = true;
	private ArrayList<SSHManager> sshList;
	private ArrayList<String> ipList;
	
	public DistributedManager() {
		ipList = new ArrayList<String>();
		sshList = new ArrayList<SSHManager>();
	}
	
	/**
	 * set the locators of slave nodes
	 * @param ipfile
	 * 		the file containing the ip information
	 */
	public void setLocators(String ipfile) {
		try {
			System.out.println("set the ip locator of each server");
			ipList = new ArrayList<String>();
			BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream(ipfile)));
			String line = "";
			while ((line = buf.readLine()) != null && !line.trim().equals("")) {
				ipList.add(line);
				System.out.println(line); 
			}
			System.out.println("machine num "+ipList.size());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(Messager.SET_LOCATOR_FAIL);
			System.exit(0);
			if(debug)
				e.printStackTrace();
		}
	}
	
	public void init(String username, String password) {
	     /**
	      * YOU MUST CHANGE THE FOLLOWING
	      * FILE_NAME: A FILE IN THE DIRECTORY
	      * USER: LOGIN USER NAME
	      * PASSWORD: PASSWORD FOR THAT USER
	      * HOST: IP ADDRESS OF THE SSH SERVER
	     **/

	     for (int i = 0; i < this.ipList.size(); i++) {
		     SSHManager sshManager = new SSHManager(username, password, this.ipList.get(i), "");
		     sshList.add(sshManager);
	     }
	}
	
	public void connectAllServer() {
		System.out.println("Connect all servers");
		for (SSHManager sshManager : sshList) {
		     String errorMessage = sshManager.connect();
		     if(errorMessage != null)
		     {
		        System.out.println(errorMessage);
		        System.exit(0);
		     }
		}
	}
	
	public void sendFilesToAllServers(String jarFileName) {
	     String fileDir = "Built Jars/"+jarFileName;
	     String sftpDir = "/home/maindisk/Test/Test4.17/";
	     sendFilesToAllServers(fileDir, sftpDir);
	}
	
	public void sendFilesToAllServers(String fileDir, String sftpDir) {
		System.out.println("Send file: "+fileDir);
		for (SSHManager sshManager : sshList) {
		     try {
		    	 sshManager.sendFile(fileDir, sftpDir);
		     } catch (Throwable e) {
		    	 e.printStackTrace();
		     }
			}
	}
	
	public void sendExecCommandToAllServers(String jarFileName) {
		System.out.println("Send command");
		String cdCommand = "cd /home/maindisk/Test/Test4.17/ && ";
		for (int i = 0; i < sshList.size(); i++) {
			SSHManager sshManager = sshList.get(i);
			String execCommand = "java -jar "+ jarFileName +" socket://" + ipList.get(i) + ":9999";
			System.out.println(sshManager.sendCommand(cdCommand + execCommand));
		}
	}
	
	public void sendCommandToAllServers(String command) {
		System.out.println("Send command");
		for (SSHManager sshManager : sshList) {
			sshManager.sendCommand(command);
		}
	}
	
	public void closeConnections() {
		System.out.println("Close connections");
		String stopCommand = "";
		for (SSHManager sshManager : sshList) {
			sshManager.close();
		}
	}
	
	public static void main(String[] args){
		String jarFileName = "Server4.60.jar";
		if(args.length>0) {
			jarFileName = args[0];
		}
		String ipfile = "Server Locators ip.ini";
	    String username = "root";
	    String password = "root";
		DistributedManager manager = new DistributedManager();
		manager.setLocators(ipfile);
		manager.init(username, password);
		manager.connectAllServer();
		manager.sendFilesToAllServers(jarFileName);
		//manager.sendExecCommandToAllServers(jarFileName);
		manager.closeConnections();
	}
}
