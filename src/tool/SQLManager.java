package tool;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import lsh.Vector;

public class SQLManager {

	static final boolean test = true;
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost";

	// Database credentials
	static final String USER = "yichao";
	static final String PASS = "password";

	public void buildDbSchema() {
		Connection conn = null;
		Statement stmt = null;
		try {
			// STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			// STEP 3: Open a connection
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);

			// STEP 4: Execute a query
			System.out.println("Creating statement...");
			stmt = conn.createStatement();
			String sqlCreateDB = "CREATE DATABASE IF NOT EXISTS yichao_lucene;";
			String sqlSelectDB = "USE yichao_lucene";
			String sqlCreateTable = "CREATE TABLE dataset(";
			sqlCreateTable += "vector_key int,";
			for (int i = 0; i < 127; i++) {
				sqlCreateTable += "value" + i + " bigint,";
			}
			sqlCreateTable += "value127 Long";
			sqlCreateTable += ")";
			stmt.execute(sqlCreateDB);
			stmt.execute(sqlSelectDB);
			stmt.executeUpdate(sqlCreateTable);
			stmt.close();
			conn.close();
			System.out.println("Database created successfully!");
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}// end finally try
		}// end try
	}

	public void insertData(String datasetFile){
	   Connection conn = null;
	   Statement stmt = null;
				 
		BufferedReader br = null;
		 
		try {
		 
			String sCurrentLine;
		 
				      //STEP 2: Register JDBC driver
		    Class.forName("com.mysql.jdbc.Driver");
		
		      //STEP 3: Open a connection
		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(DB_URL,USER,PASS);
		
		      //STEP 4: Execute a query
		    System.out.println("Creating statement...");
		    stmt = conn.createStatement();
		    String sqlUseDB = "USE yichao_lucene;";
		    stmt.executeQuery(sqlUseDB);
		      
			br = new BufferedReader(new FileReader(datasetFile));
			
		   while ((sCurrentLine = br.readLine()) != null) {
			   String[] valueStrs = sCurrentLine.split(" ");			   
			   
			      String sqlInsertData = "INSERT INTO dataset value(";
			      for(int i = 0; i < 128; i++){
			    	  sqlInsertData += valueStrs[i]+", ";
			      }
			      sqlInsertData += valueStrs[128];
			      sqlInsertData += ")";
		      
		      stmt.executeUpdate(sqlInsertData);
			}
		      stmt.close();
		      conn.close();
			  System.out.println("Data inserted successfully!");
		   } catch (IOException e) {
				e.printStackTrace();
		   } catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }finally{
		      //finally block used to close resources
		      try{
		         if(stmt!=null)
		            stmt.close();
		      }catch(SQLException se2){
		      }// nothing we can do
		      try{
		         if(conn!=null)
		            conn.close();
		      }catch(SQLException se){
		         se.printStackTrace();
		      }//end finally try
		      
			  try {
			  	if (br != null)br.close();
			  } catch (IOException ex) {
			  	ex.printStackTrace();
			  }
		}//end try
   }

	public List<Vector> selectData(long[] keys) {
		Connection conn = null;
		Statement stmt = null;
		List<Vector> vectorResults = new ArrayList<Vector>();
		
		long start = System.currentTimeMillis();
		
		try {
			// STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			// STEP 3: Open a connection
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);

			// STEP 4: Execute a query
			System.out.println("Creating statement...");
			stmt = conn.createStatement();
			String sqlUseDB = "USE yichao_lucene";
			String sqlSelectRow = "SELECT * FROM dataset WHERE vector_key = "+keys[0];
			for(int i = 1; i < keys.length; i++)
				sqlSelectRow += " OR vector_key = " + keys[i];
			stmt.executeQuery(sqlUseDB);
			ResultSet rs = stmt.executeQuery(sqlSelectRow);
			
			// STEP 5: Extract data from result set
			Vector vector;
			while (rs.next()) {
				// Retrieve by column name
				long id = rs.getLong("vector_key");
				double[] values = new double[128];
				for(int i = 0; i < 128; i++){
					values[i] = rs.getInt("value"+i);
				}
				vector = new Vector(id, values);
			// STEP 6: Clean-up environment
//				System.out.println(vector.getKey()+" "+vector);
				vectorResults.add(vector);
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}// end finally try
		}// end try
		
		long end = System.currentTimeMillis();
		System.out.println("Time for selecting a key:\t"+(end - start)+" ms");
		
		return vectorResults;
	}
	
	public static void main(String[] args){
		String dataFile = "LSHfile_1_100000.txt";
		long[] keys = new long[] {0, 2};
		
		SQLManager sqlManager = new SQLManager();
//		sqlManager.buildDbSchema();
//		sqlManager.insertData(dataFile);
		sqlManager.selectData(keys);
	}
	
	
}// end FirstExample