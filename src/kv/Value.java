package kv;

import java.io.Serializable;
 /**
  * abstract class of value
  * @deprecated
  * */
public abstract class Value implements Serializable {
	/**
	 * value types
	 */
	public static final int INTARRAY_VALUE = 0;
	public static final int DOUBLEARRAY_VALUE = 1;
	public static final int LONGARRAY_VALUE = 2;
	
	private String doc;
	
	/**
	 * concatenate the value in array to form a document
	 * splited by white space
	 * */
	public  String getDocument() {
		return doc;
	}
	
	/**
	 * the length of the array
	 * @return
	 * */
	public void setDocument(String doc) {
		this.doc = new String(doc);
	}
}
