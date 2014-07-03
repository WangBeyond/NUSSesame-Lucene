package kv;

import java.io.Serializable;

public class Pair implements Serializable{
	
	private int type;
	
	public long id_long;
	public long value_long; 
	public long values_long[];
	public int values_id_int[];
	public String value_string;
	
	/**
	 * @deprecated
	 * */
	public Pair(Key k, Value v) {
		
//		key = k;
//		value =v;
	}
	
	public Pair(long id_long,long value_long){
		this.id_long = id_long;
		this.value_long = value_long;
	}
	
	public Pair(long id_long,long value_long, int type) {
		
		this.id_long = id_long;
		this.value_long = value_long;
		this.type = type;
	}
	
	public Pair(long id_long, String value_string, int type) {
		
		this.id_long = id_long;
		this.value_string = value_string;
		this.type = type;
	}
	
	public Pair(long id_long, long values_long[], int type) {
		
		this.id_long = id_long;
		this.values_long = values_long;
		this.type = type;
	}
	
	public Pair(long id_long, int values_id_int[], int type) {
		this.id_long = id_long;
		this.values_id_int = values_id_int;
		this.type = type;
	}
	
	public int getType() {
		
		return this.type;
	}
}
