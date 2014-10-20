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

import java.util.Arrays;

public class EuclidianHashFamily implements HashFamily {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3406464542795652263L;
	private final int dimensions;
	private int w;
	private long mod;
		
	public EuclidianHashFamily(int w, int dimensions, long mod){
		this.dimensions = dimensions;
		this.w = w;
		this.mod = mod;
	}
	
	@Override
	public HashFunction createHashFunction(){
		return new EuclideanHash(dimensions, w);
	}
	
	@Override
	public Long combine(int[] hashes){
		return hashCode(hashes);
	}

	@Override
	public DistanceMeasure createDistanceMeasure() {
		return new EuclideanDistance();
	}
	
	 public long hashCode(int a[]) {
		 this.mod = Integer.MAX_VALUE;
		 if (a == null)
	         return 0;
	     long result = 1;
	     for (int element : a)
	         result = 31 * result + element;
	     return (result % this.mod);
	 }
}
