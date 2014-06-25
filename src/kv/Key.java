package kv;

import java.io.Serializable;

/**
 * abstract class for Key
 * @deprecated
 * */
public abstract class Key implements Serializable {
	/**
	 * key types
	 */
	public static final int INT_KEY = 0;
	public static final int DOUBLE_KEY = 1;
	public static final int LONG_KEY = 2;
	public static final int STR_KEY = 3;
	
	public abstract int compare(Key K);
	
	/**
	 * 	@return string info of the key
	 */
	public abstract String toString();
	
	/**
	 * @return type
	 */
	public abstract int getType();
}
