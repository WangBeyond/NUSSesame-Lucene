package lucene;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

import jmaster.JMaster;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;

/**
 * This is the class returned for each query  
 * @author huangzhi
 * */
public class ReturnValue implements Serializable{

	//double[0] is the count; double[1] is the accumulate distance
	public HashMap<Long, float[]> table;
	
	//return the index and data for the TopK
	//they are mainly used to return result of string search
	public List<String> topk_list;
	public List<Long> topk_index;
	public List<Integer> topk_count;
	//the query string
	public String querystring;
	//the query sift
	public long querylong;
	//to calculate the bound
	public float min_dis, max_dis;
	//contain all the vector indices for range query
	public Set<Long> indexSet;
	
	public ReturnValue () {
		
		this.querystring = null;
		this.querylong = Long.MIN_VALUE;
		this.table = new HashMap<Long, float[]>();
		this.min_dis = Float.MAX_VALUE;
		this.max_dis = Float.MIN_VALUE;
		
		this.topk_count = new ArrayList<Integer>();
		this.topk_index = new ArrayList<Long>();
		this.topk_list = new ArrayList<String>();
		
		this.indexSet = new HashSet<Long>();
	}
	
	public List<Map.Entry<Long, float[]>> sortedOncount() {
		
		List<Map.Entry<Long, float[]>> infoIds = 
		    new ArrayList<Map.Entry<Long, float[]>>(this.table.entrySet());
		
		String result = "";		
		//sort the hashmap 
		//by count; then by id
		Collections.sort(infoIds, new Comparator<Map.Entry<Long, float[]>>() {   
		    public int compare(Map.Entry<Long, float[]> o1, Map.Entry<Long, float[]> o2) {   
		    	int delta =(int) (o2.getValue()[0] - o1.getValue()[0]);
		        if(delta != 0)
		        	return delta;
		        else
		        	return (int) (o1.getKey() - o2.getKey()); 
		    }
		}); 

		result = "";
		return infoIds;
	}
	
	public List<Map.Entry<Long, float[]>> sortedOndis() {
		
		List<Map.Entry<Long, float[]>> infoIds = 
		    new ArrayList<Map.Entry<Long, float[]>>(this.table.entrySet());
		
		String result = "";
		//sort the hashmap
		//by distance; then by id
		Collections.sort(infoIds, new Comparator<Map.Entry<Long, float[]>>() {   
		    public int compare(Map.Entry<Long, float[]> o1, Map.Entry<Long, float[]> o2) { 
		    	if(o1.getValue()[1] > o2.getValue()[1])
		    		return 1;
		    	else if(o1.getValue()[1] < o2.getValue()[1])
		    		return -1;
		        else
		        	return (int) (o1.getKey() - o2.getKey());  
		    }
		}); 
	
		return infoIds;
	}
	
	public void merge(ReturnValue value , double weight) {
		
		if(value == null)
			return;
		
		//merge the hash map
		List<Map.Entry<Long, float[]>> infoIds = 
		    new ArrayList<Map.Entry<Long, float[]>>(value.table.entrySet());
		
		for(int i = 0; i < infoIds.size(); i++) {
			Long key = infoIds.get(i).getKey();

			if(this.table.containsKey(key)) {					
				float count_dis1[] = this.table.get(key);
				float count_dis2[] = infoIds.get(i).getValue();
				//add the count
				count_dis1[0] += (count_dis2[0] * weight);
				//add the distance
				count_dis1[1] += (count_dis2[1] * weight);
				this.table.put(key, count_dis1);				
				
			}
			else {
				
				float count_dis[] = infoIds.get(i).getValue();
				count_dis[0] *= weight;
				count_dis[1] *= weight;
				this.table.put(key, count_dis);
			}
		}
		//merge the predict distance
		if(value.min_dis < this.min_dis)
			this.min_dis = value.min_dis;
		
		if(value.max_dis > this.max_dis)
			this.max_dis = value.max_dis;
		
		//merge the topK results
		for(int i = 0;i < value.topk_count.size(); i++) {
			this.topk_count.add(value.topk_count.get(i));
			this.topk_index.add(value.topk_index.get(i));
			this.topk_list.add(value.topk_list.get(i));
		}
	}
	

	
	public void merge(ReturnValue value) {
		merge(value, 1.0);
	}
	
	
	public void intersect(ReturnValue value, double weight) {
		if(value == null)
			return;
		
		//intersect the hash map
		List<Map.Entry<Long, float[]>> infoIds = 
		    new ArrayList<Map.Entry<Long, float[]>>(this.table.entrySet());
		for(int i = 0; i < infoIds.size(); i++) {
			long key = infoIds.get(i).getKey();
			if(value.table.containsKey(key)) {	
	
				float count_dis1[] = this.table.get(key);
				float count_dis2[] = value.table.get(key);
				
				//add the count
				count_dis1[0] += (count_dis2[0] * weight);
				//add the distance
				count_dis1[1] += (count_dis2[1] * weight);
				this.table.put(key, count_dis1);	
				

			} else {
				this.table.remove(key);
			}
		}
	}
	
	public void normalizeDist(double pValue) {
		List<Map.Entry<Long, float[]>> infoIds = 
		    new ArrayList<Map.Entry<Long, float[]>>(this.table.entrySet());
		
		for(int i = 0; i < infoIds.size(); i++) {
			Long key = infoIds.get(i).getKey();
			float count_dis[] = this.table.get(key);
			count_dis[1] = (float)Math.pow(count_dis[1], 1/pValue);
			this.table.put(key, count_dis);						
		}
	}
	
	public void intersect(ReturnValue value) {
		intersect(value, 1.0);
	}
}


