/*
*      _______                       _        ____ _     _
*     |__   __|                     | |     / ____| |   | |
*        | | __ _ _ __ ___  ___  ___| |    | (___ | |___| |
*        | |/ _` | '__/ __|/ _ \/ __| |     \___ \|  ___  |
*        | | (_| | |  \__ \ (_) \__ \ |____ ____) | |   | |
*        |_|\__,_|_|  |___/\___/|___/_____/|_____/|_|   |_|
*                                                         
* -----------------------------------------------------------
*
*  TarsosLSH is developed by Joren Six at 
*  The School of Arts,
*  University College Ghent,
*  Hoogpoort 64, 9000 Ghent - Belgium
*  
* -----------------------------------------------------------
*
*  Info    : http://tarsos.0110.be/tag/TarsosLSH
*  Github  : https://github.com/JorenSix/TarsosLSH
*  Releases: http://tarsos.0110.be/releases/TarsosLSH/
* 
*/

package lsh;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Random;


public class EuclideanHash implements HashFunction{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3784656820380622717L;
	private Vector randomProjection;
	private int offset;
	private int w;
	private int dimension;
	
	public EuclideanHash(int dimensions,int w){
		Random rand = new Random();
		this.w = w;
		this.offset = rand.nextInt(w);
		this.dimension = dimensions;
		
		randomProjection = new Vector(dimensions);
		for(int d=0; d<dimensions; d++) {
			//mean 0
			//standard deviation 1.0
			double val = rand.nextGaussian();
			randomProjection.set(d, val);
		}
		
		
	}
	
	public void generateProjection(String hashFile, int dimensions) throws Throwable{
    	FileWriter fileWriter = new FileWriter(hashFile);
		Random rand = new Random();
		this.w = w;
		this.offset = rand.nextInt(w);
		
		randomProjection = new Vector(dimensions);
		for(int d=0; d<dimensions; d++) {
			//mean 0
			//standard deviation 1.0
			double val = rand.nextGaussian();
			fileWriter.write(val+" ");
		}
		fileWriter.write(w+"\n");
	}
	
	public void retrieveProjection(String hashFile, int dimensions) throws Throwable{
		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream("data/rangequery.txt")));
		String line = buf.readLine();
		String values[] = line.split(" ");
		randomProjection = new Vector(dimensions);
		for(int i=0;i<dimensions;i++){
			randomProjection.set(i, Integer.valueOf(values[i]));
		}
		this.w = Integer.valueOf(values[dimensions]);
	}
	
	public int hash(Vector vector){
		double hashValue = (vector.dot(randomProjection)+offset)/Double.valueOf(w);
		return (int) Math.round(hashValue);
	}
	
	public int getDimension(){
		return this.dimension;
	}
	
	public Vector getProjection(){
		return this.randomProjection;
	}
	
	public int getW(){
		return this.w;
	}
	

}
