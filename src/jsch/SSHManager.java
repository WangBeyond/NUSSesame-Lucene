package jsch;

import static org.junit.Assert.*;

import com.jcraft.jsch.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

public class SSHManager
{
	private static final Logger LOGGER = 
	    Logger.getLogger(SSHManager.class.getName());
	private JSch jschSSHChannel;
	private String strUserName;
	private String strConnectionIP;
	private int intConnectionPort;
	private String strPassword;
	private Session sesConnection;
	private Channel channel;
	private ChannelSftp channelSftp;
	private int intTimeOut;


	public SSHManager(String userName, String password, 
	   String connectionIP, String knownHostsFileName) 
	{
	   doCommonConstructorActions(userName, password, 
	              connectionIP, knownHostsFileName);
	   intConnectionPort = 22;
	   intTimeOut = 60000;
	}

	public SSHManager(String userName, String password, String connectionIP, 
	   String knownHostsFileName, int connectionPort) 
	{
	   doCommonConstructorActions(userName, password, connectionIP, 
	      knownHostsFileName);
	   intConnectionPort = connectionPort;
	   intTimeOut = 60000;
	}

	private void doCommonConstructorActions(String userName, 
		     String password, String connectionIP, String knownHostsFileName) 
		{
		   jschSSHChannel = new JSch();
		   if(knownHostsFileName != null && !knownHostsFileName.trim().equals("")) {
			   try
			   {
			      jschSSHChannel.setKnownHosts(knownHostsFileName);
			   }
			   catch(JSchException jschX)
			   {
				   logError(jschX.getMessage());
			   }
		   }

		   strUserName = userName;
		   strPassword = password;
		   strConnectionIP = connectionIP;
		}
	
	  public String connect()
	  {
	     String errorMessage = null;

	     try
	     {
	        sesConnection = jschSSHChannel.getSession(strUserName, 
	            strConnectionIP, intConnectionPort);
	        sesConnection.setPassword(strPassword);
	        // UNCOMMENT THIS FOR TESTING PURPOSES, BUT DO NOT USE IN PRODUCTION
	        sesConnection.setConfig("StrictHostKeyChecking", "no");
	        sesConnection.connect(intTimeOut);
	     }
	     catch(JSchException jschX)
	     {
	        errorMessage = jschX.getMessage();
	     }

	     return errorMessage;
	  }
	  
	  public void sendFile(String fileDir, String sftpDir) throws Throwable{
          channel = sesConnection.openChannel("sftp");
          channel.connect();
          channelSftp = (ChannelSftp)channel;
          channelSftp.cd(sftpDir);
          File f = new File(fileDir);
          channelSftp.put(new FileInputStream(f), f.getName());
	  }
	  
	  private String logError(String errorMessage)
	  {
	     if(errorMessage != null)
	     {
	        LOGGER.log(Level.SEVERE, "{0}:{1} - {2}", 
	            new Object[]{strConnectionIP, intConnectionPort, errorMessage});
	     }

	     return errorMessage;
	  }

	  private String logWarning(String warnMessage)
	  {
	     if(warnMessage != null)
	     {
	        LOGGER.log(Level.WARNING, "{0}:{1} - {2}", 
	           new Object[]{strConnectionIP, intConnectionPort, warnMessage});
	     }

	     return warnMessage;
	  }

	  public String sendCommand(String command)
	  {
	     StringBuilder outputBuffer = new StringBuilder();

	     try
	     {
	        Channel channel = sesConnection.openChannel("exec");
	        ((ChannelExec)channel).setCommand(command);
	        channel.connect();
	        InputStream commandOutput = channel.getInputStream();
	        int readByte = commandOutput.read();

	        while(readByte != 0xffffffff)
	        {
	           outputBuffer.append((char)readByte);
	           readByte = commandOutput.read();
	        }

	        channel.disconnect();
	     }
	     catch(IOException ioX)
	     {
	    	 System.out.println(ioX.getMessage());
	        logWarning(ioX.getMessage());
	        return null;
	     }
	     catch(JSchException jschX)
	     {
	    	 
	    	System.out.println(jschX.getMessage());
	        logWarning(jschX.getMessage());
	        return null;
	     }

	     return outputBuffer.toString();
	  }

	  public void close()
	  {
	     sesConnection.disconnect();
	  }

	  /**
	     * Test of sendCommand method, of class SSHManager.
	     */
	  @Test
	  public void testSendCommand()
	  {
	     System.out.println("sendCommand");

	     /**
	      * YOU MUST CHANGE THE FOLLOWING
	      * FILE_NAME: A FILE IN THE DIRECTORY
	      * USER: LOGIN USER NAME
	      * PASSWORD: PASSWORD FOR THAT USER
	      * HOST: IP ADDRESS OF THE SSH SERVER
	     **/
	     String command = "ls FILE_NAME";
	     String userName = "USER";
	     String password = "PASSWORD";
	     String connectionIP = "HOST";
	     SSHManager instance = new SSHManager(userName, password, connectionIP, "");
	     String errorMessage = instance.connect();

	     if(errorMessage != null)
	     {
	        System.out.println(errorMessage);
	        fail();
	     }

	     String expResult = "FILE_NAME\n";
	     // call sendCommand for each command and the output 
	     //(without prompts) is returned
	     String result = instance.sendCommand(command);
	     // close only after all commands are sent
	     instance.close();
	     assertEquals(expResult, result);
	  }

	  //for testing purpose
	  
	  public static void main(String[] args) {
		     System.out.println("sendCommand");

		     /**
		      * YOU MUST CHANGE THE FOLLOWING
		      * FILE_NAME: A FILE IN THE DIRECTORY
		      * USER: LOGIN USER NAME
		      * PASSWORD: PASSWORD FOR THAT USER
		      * HOST: IP ADDRESS OF THE SSH SERVER
		     **/
		     String command = "rm /home/maindisk/Test/Test4.17/Server4.40.jar";
		     String userName = "root";
		     String password = "root";
		     String connectionIP = "192.168.1.11";
		     String fileDir = "Built Jars/Server4.40.jar";
		     String sftpDir = "/home/maindisk/Test/Test4.17/";
		     SSHManager instance = new SSHManager(userName, password, connectionIP, "");
		     String errorMessage = instance.connect();
		     if(errorMessage != null)
		     {
		        System.out.println(errorMessage);
		        System.exit(0);
		     }
		     try {
		    	 instance.sendFile(fileDir, sftpDir);
		     } catch (Throwable e) {
		    	 e.printStackTrace();
		     }
		     //instance.sendCommand(command);
		     instance.close();
	  }
	  
}