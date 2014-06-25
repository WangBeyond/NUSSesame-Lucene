package lucene;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilter;
import org.apache.lucene.analysis.payloads.IntegerEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttributeImpl;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;

import tool.DataProcessor;

/**
 * building the inverted with the interface of Lucene scanning the inverted list
 * 
 * @author huangzhi
 * */

public class Index {

	// for debug
	static boolean debug = true;

	// for testing
	static boolean test = true;
	static long init_time = 0;
	static long searching_time = 0;
	static long scanning_value_list_time = 0;
	static long scanning_key_list_time = 0;
	static long calc_time = 0;
	static long write_time = 0;
	static int num_build = 0;
	static int num_query = 0;

	// for general use
	public static int VECTOR_BUILD = 1;
	public static int STRING_BUILD = 2;
	public static int STRING_SEARCH = 3;
	public static int VECTOR_SEARCH = 4;

	// used in previous version, @deprecated now
	public static int BUILD = STRING_BUILD;

	private File indexFile;
	private String mapfilename = "key_enum.map";
	private PayloadAnalyzer payload_analyzer;
	private MMapDirectory MMapDir;
	private IndexWriterConfig config;
	private IndexWriter MMwriter;
	private IndexReader indexReader;
	private AtomicReader areader;
	private String fieldname1 = "DocumentID";
	private String fieldname2 = "ElementValue";
	private String fieldname3 = "Data";
	private Field id_field;
	private Field value_field;
	private Field data_field;
	private FieldType data_field_type;
	private PayloadAttributeImpl payload;
	// store the search position of value list enumerator
	private HashMap<String, DocsEnum> position_map;
	// store the search position of value list enumerator for bi-direction search
	private HashMap<String, long[]> bi_position_map;
	//map the lucene id and doc id
	private long[] idmap;
	//map the key and posting list entrance
	private HashMap<String, DocsAndPositionsEnum> enum_map;


	//Maximum Buffer Size
	private int MAX_BUFF = 48;
	// number of dimensions to be combined
	private static int NUM_COMBINATION = 1;
	// number of total dimension
	private static int DIM_RANGE = 0;
	// Top K
	private static int K = 0;

	// expand direction
	private static int UPWARDS = -1;
	private static int DOWNWARDS = 1;

	// to create the TextField for vector insertion
	private StringBuffer strbuf;
	// to create the data_field
	private StringBuffer databuf;


	public Index() {
	}

	public void setIndexfile(String indexfilename) {

		this.indexFile = new File(indexfilename);
		System.out.println("The Index File is set: " + indexfilename);
	}

	/**
	 * initialization for building the index
	 * 
	 * @throws Throwable
	 * */
	public void init_building() throws Throwable {

		// PayloadAnalyzer to map the Lucene id and Doc id
		payload_analyzer = new PayloadAnalyzer(new IntegerEncoder());
		// MMap
		MMapDir = new MMapDirectory(indexFile);
		// set the configuration of index writer
		config = new IndexWriterConfig(Version.LUCENE_45, payload_analyzer);
		config.setRAMBufferSizeMB(MAX_BUFF);
		// the index configuration
		if (test) {
			System.out.println("Max Docs Num:\t" + config.getMaxBufferedDocs());
			System.out.println("RAM Buffer Size:\t" + config.getRAMBufferSizeMB());
			System.out.println("Max Merge Policy:\t" + config.getMergePolicy());
		}
		// use Memory Map to store the index
		MMwriter = new IndexWriter(MMapDir, config);
		
		id_field = new LongField(this.fieldname1, -1, Field.Store.NO);
		value_field = new TextField(this.fieldname2,"-1",Field.Store.YES);	
		data_field = new TextField(this.fieldname3, "", Field.Store.NO);
		
		strbuf = new StringBuffer();
		databuf = new StringBuffer();
	}   

	/**
	 * Add a document. The document contains two fields: one is the element id, 
	 * the other is the values on each dimension
	 * 
	 * @param
	 * 		 	id: vector id
	 * @param
	 * 			values[]: the values of each dimension
	 * */
	public void addDoc(long id, long []values) {
		
		Document doc = new Document();
		//clear the StringBuffer
		strbuf.setLength(0);
		//set new Text for payload analyzer
		for (int i = 0; i < values.length; i++) {
			strbuf.append(values[i]+" ");
		}
		//set fields for document
		id_field.setLongValue(id);
		value_field.setStringValue(strbuf.toString());
		data_field.setStringValue("ID|"+id);

		doc.add(id_field);
		doc.add(value_field);
		doc.add(data_field);
		
		try {
			MMwriter.addDocument(doc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("index writer error");
			if (debug)
				e.printStackTrace();
		}
	}
	

	/**
	 * add a document and build the index the document contains two string
	 * fields
	 * 
	 * @param id
	 *            the element id
	 * @param v
	 *            the value
	 *
	 * */
	public void addDoc(long id, String v) {

		Document doc = new Document();
		
		id_field.setLongValue(id);
		value_field.setStringValue(v);
		data_field.setStringValue("ID|"+id);
		
		doc.add(id_field);
		doc.add(value_field);
		doc.add(data_field);
		try {
			MMwriter.addDocument(doc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("index writer error");
			if (debug)
				e.printStackTrace();
		}
	}

	/**
	 * initialize the query process
	 * */
	public void init_query() throws Throwable {

		if(indexReader != null)
			indexReader.close();
		if(areader != null)
			areader.close();
		
		indexReader = DirectoryReader.open(MMapDirectory.open(indexFile));
		// change the reader
		areader = SlowCompositeReaderWrapper.wrap(indexReader);
		position_map = new HashMap<String, DocsEnum>();
		bi_position_map = new HashMap<String, long[]>();
		
		//The index for datafield (markfield)
		//map the lucene id and doc id
		idmap = new long[indexReader.maxDoc()];
		Term term = new Term(this.fieldname3,"ID");
		DocsAndPositionsEnum dp = areader.termPositionsEnum(term);
		int lucene_id = -1, doc_id = -1;
		BytesRef buf = new BytesRef();	
		while((lucene_id = dp.nextDoc()) != DocsAndPositionsEnum.NO_MORE_DOCS) {
			dp.nextPosition();
			buf = dp.getPayload();
			doc_id = PayloadHelper.decodeInt(buf.bytes, buf.offset);
			idmap[lucene_id] = doc_id;
		}
		mapKeyEnum();
	}
	
	/**
	 * This function is used to map the key and the entrance of its posting list.
	 * This function is not scalable. If the number of the key is too large,the 
	 * map will cost too much space. 
	 * Maybe there will be OutOfMemoryExceptions.
	 * After building the index, we write the map to disk.
	 * */
	private void mapKeyEnum() {
		
		long start = System.currentTimeMillis();
		System.out.println("Starting mapping the keys...");
		enum_map = new HashMap<String, DocsAndPositionsEnum>();
		try {
			//The idnex for valuesfield
			Terms terms = areader.terms(this.fieldname2);
			TermsEnum te =  null;
			te = terms.iterator(te);
			String keystring;
			Bits liveDocs =  areader.getLiveDocs();
			while(te.next() != null) {
				keystring = te.term().utf8ToString();
				DocsAndPositionsEnum enumer = te.docsAndPositions(liveDocs, null);
				//store the index of key strings
				enum_map.put(keystring, enumer);
			}	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("There some problems in mapping the key and Enum");
//			if(debug)
				e.printStackTrace();
		}
		init_time = (System.currentTimeMillis() - start);
		System.out.println("Mapping Done! Time:\t"+init_time+" ms");
		
	}
	
	/**
	 * search for different kinds of data
	 * 
	 * @return 
	 * 			ReturnValue
	 * @throws 
	 * 			Throwable
	 * */
	public ReturnValue generalSearch(QueryConfig config) throws Throwable {

		long start = 0;
		if (test) {
			Index.num_query++;
			start = System.currentTimeMillis();
		}
		
		// start search
		ReturnValue revalue;
		if (config.getType() == QueryConfig.STRING)
			revalue = stringSearch(config);
		else if (config.getType() == QueryConfig.VECTOR)
			revalue = vectorSearch(config);
		// string search by default
		else
			revalue = stringSearch(config);
		if (test) {
			Index.searching_time += (System.currentTimeMillis() - start);
			if (Index.num_query == 128) {
				System.out.println("Calc time:\t" + Index.calc_time);
				System.out.println("scan key list time:\t" + Index.scanning_key_list_time);
				System.out.println("scan value list time:\t" + Index.scanning_value_list_time);
				System.out.println("search total time:\t" + Index.searching_time);
				Index.num_query = 0;
				Index.calc_time = 0;
				Index.scanning_key_list_time = 0;
				Index.scanning_value_list_time = 0;
				Index.searching_time = 0;
			}
		}
		return revalue;
	}

	/**
	 * To lower the remote function calling cost, the salve node handle a batch of every time
	 * */
	public ReturnValue generalSearch(List<QueryConfig> qlist) throws Throwable {
		
		ReturnValue result = new ReturnValue();
		ReturnValue revalue = new ReturnValue();
		K = qlist.get(0).getK();
		//if we search the vector
		if(qlist.get(0).getType() == QueryConfig.VECTOR) {
			// set searching configuration
			NUM_COMBINATION = qlist.get(0).num_combination;
			DIM_RANGE = qlist.get(0).getDimRange();
			
			Candidates candidates = null;	
			int num_round = 0;
			while (true) {
				System.out.println("Round:\t"+num_round++);
				
				for(int i = 0; i < qlist.size(); i++) {
					revalue.merge((generalSearch(qlist.get(i))));
				}
				candidates = getCandidates(revalue, DIM_RANGE, K);
				// stop condition: the maximum of K elements with minimum upper bound is smaller
				//than the minimum lower bound of rest (n-k) elements
				if(candidates.elements_min_upperbound[0].upper_bound <= candidates.elements_min_lowerbound[0].lower_bound) 
					break;
			}
		
			long index[] = new long[K];
			//record the K nearest neighbors
			if(candidates != null) {
				for(int i = 0; i < K; i++) {
					index[i] = candidates.elements_min_upperbound[K - i - 1].element_id;
				}
			}
			float dis[] =  getDistances(index, qlist);
			for(int i = 0;i < dis.length; i++) {
				float count_dis[] = new float[2];
				count_dis[0] = 128;
				count_dis[1] = dis[i];
				System.out.println(index[i]+"\t"+dis[i]);
				result.table.put(index[i], count_dis);
			}
		}
		// if we search for string, it is simpler. we do not have to iterate
		else if(qlist.get(0).getType() == QueryConfig.STRING){
			
			// just combine the result of each
			for(int i = 0; i < qlist.size(); i++)
				revalue.merge(stringSearch(qlist.get(i)));
			// this is not scalable. we can maintain a heap instead of sorting
			List<Entry<Long, float[]>>  list= revalue.sortedOncount();
			// return the topK results
			for(int i = 0; i < Math.min(K, list.size()); i++) {
				result.topk_index.add(list.get(i).getKey());
				result.topk_count.add((int)list.get(i).getValue()[0]);
				result.topk_list.add(DataProcessor.combineGrams(3, getData(list.get(i).getKey())));
			}
		}
		return result;
	}
	
	/**
	 * calculate the bounds and get candidates
	 * */
	private Candidates getCandidates(ReturnValue revalue, int dim_range ,int k) {
		
		List<Map.Entry<Long, float[]>> list = new ArrayList<Map.Entry<Long, float[]>>(revalue.table.entrySet());
		Candidates candidate = new Candidates();
		//calculate the bounds for each element
		//build a priority queue to get the minimum k elements
		QueueElement element[] = new QueueElement[list.size()];
		for(int i = 0;i < list.size();i++)
			element[i] = new QueueElement();
		//create a maximum priority queue
		PriorityQueue<QueueElement> queue = new PriorityQueue<QueueElement>(k, new UpperboundComparator());
		//O(n*log(k))
		for(int i = 0; i < list.size(); i++) {
			Entry<Long, float[]> entry = list.get(i);
			float[] count_dis = entry.getValue();
			element[i].element_id = entry.getKey();
			//upper bound = distance + (dim_range - count) * max_dis
			element[i].upper_bound = count_dis[1] + (dim_range - count_dis[0]) * revalue.max_dis;
			//lower bound = distance + (dim_range - count) * min_dis
			element[i].lower_bound = count_dis[1] + (dim_range - count_dis[0]) * revalue.min_dis;
			//maintain the priority queue
			//insert k element into the queue
			if(i < k)
				queue.add(element[i]);
			//if current element's upper bound smaller than the largest upper bound in the queue
			//then delete the largest upper bound and insert current element
			else if(i >= k && element[i].upper_bound < queue.peek().upper_bound){
				queue.poll();
				queue.add(element[i]);
			}
		}
		candidate.elements_min_lowerbound = new QueueElement[1];
		candidate.elements_min_upperbound = new QueueElement[queue.size()];
		
		//hash map is used to find the element with the minimum lower bound
		HashMap<Long,Long> id_map = new HashMap<Long, Long>();
		for(int i = 0; i < k; i++) {
			candidate.elements_min_upperbound[i] = new QueueElement();
			candidate.elements_min_upperbound[i] = queue.poll();
			id_map.put(candidate.elements_min_upperbound[i].element_id, null);
		}

		//find the one with minimum lower bound
		float min_bound = Float.MAX_VALUE;
		candidate.elements_min_lowerbound[0] = new QueueElement();
		for(int i = 0;i < element.length; i++) {
			if(id_map.containsKey(element[i].element_id) == false
					&& element[i].lower_bound < min_bound) {
				min_bound = element[i].lower_bound;
				candidate.elements_min_lowerbound[0] = element[i];
			}
		}
		return candidate;
	}
	

	/**
	 * random access
	 * we can get data by providing the id
	 * */
	public String getData(long id) {

		// the enumerator of value list
		DocsEnum value_enum;
		// create a term for query
		// It is much easier to create a term for text.
		// To create a term for long, we have to get the prefix code first
		BytesRef bytesref = new BytesRef();
		// the code is stored in a BytesRef object
		NumericUtils.longToPrefixCoded(id, 0, bytesref);
		Term qterm = new Term(fieldname1, bytesref);
		try {
			value_enum = areader.termDocsEnum(qterm);
//			return indexReader.document(value_enum.nextDoc()).getField(fieldname3).stringValue();
			return indexReader.document(value_enum.nextDoc()).getField(fieldname2).stringValue();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * calculate the actual distance for local TopK
	 * */
	private float[] getDistances(long ids[], List<QueryConfig> qlist) {
		
		float distances [] = new float[ids.length];
		String values[];
		int actual_value;
		for(int i = 0; i < ids.length; i++) {
			values = this.getData(ids[i]).split(" ");
			for(int j = 0; j < values.length; j++) {
				//extract the actual value from the number (dim%256+value)
				if(j == 0)
					actual_value = Integer.valueOf(values[j]);
				else
					actual_value = Integer.valueOf(values[j]) % (j << 8);
				distances[i] += qlist.get(j).calcDistance(qlist.get(j).getDimValue(), actual_value);
			}
		}
		
		return distances;
	}
	
	/**
	 * search the sub-sequence
	 * */
	public ReturnValue stringSearch(QueryConfig config) throws Throwable {

		ReturnValue revalue = new ReturnValue();
		// store some information for return value
		revalue.querystring = config.getQuerystring();
		// the enumerator of value list
		DocsEnum value_enum = null;
		// create a term for query
		Term qterm = new Term(fieldname2, config.getQuerystring());
		// store the query id and query string
		String id_query = String.valueOf(config.queryId) + "+" + config.getQuerystring();
		System.out.println(id_query);
//		// if we do not have to scan from the beginning
//		if (config.needRestart == false) {
//			if (position_map.containsKey(id_query)) {
//				if (debug)
//					System.out.println(id_query + " is found in map");
//				// get the enumerator directly from the hash map
//				value_enum = position_map.get(id_query);
//			} 
//		}
//		// else create new enumerator and scan from beginning
//		else {
//			// find the exact entrance of the value list
//			value_enum = areader.termDocsEnum(qterm);
//		}
		// we do not have to scan from the beginning
		if(position_map.containsKey(id_query)) {
			value_enum = position_map.get(id_query);
		}
		// start from the beginning
		else {
			value_enum = enum_map.get(config.getQuerystring());
		}
		
		// scan the list for a certain length
		int count = 0, lucene_doc_id = -1;
		long element_id;
		while (count < config.getLength()) {
			// not found
			if (value_enum == null) {
				System.out.println("the feature is not found");
				return revalue;
			}
			// scan the list
			lucene_doc_id = value_enum.nextDoc();
			// at the end of the list
			if (lucene_doc_id == DocsEnum.NO_MORE_DOCS) {
				position_map.put(id_query, value_enum);
				return revalue;
			}
			count++;

			// update the table
			// get the element id
			element_id = idmap[lucene_doc_id];
			// update count
			float count_distance[] = new float[2];
			if (revalue.table.containsKey(element_id)) {
				count_distance = revalue.table.get(element_id);
				// count++
				count_distance[0] += 1.0;
				revalue.table.put(Long.valueOf(element_id), count_distance);
			} else {
				// count = 1
				count_distance[0] = 1;
				// accumulate distance = 0
				count_distance[1] = 0;
				revalue.table.put(Long.valueOf(element_id), count_distance);
			}
		}
		// update the position map
		position_map.put(id_query, value_enum);
		return revalue;
	}

	/**
	 * scan the list and update the info table
	 * @throws Throwable 
	 * 
	 **/
	public void scanList(Long querylong, QueryConfig config, ReturnValue revalue)throws Throwable {

		if (debug)
			System.out.println("Scanning List: " + querylong);

		long start_time = 0;
		if (test)
			start_time = System.currentTimeMillis();

		DocsAndPositionsEnum enumer = null;
		String keystring = String.valueOf(querylong);

		if(enum_map.containsKey(keystring)) {
			enumer = enum_map.get(keystring);
		}
//		else {
//			// search in the key list
//			Term queryterm = new Term(fieldname2, keystring);
//			// search for the list entrance
//			enumer = areader.termPositionsEnum(queryterm);
//		}
		// the time to scan the key list, should be O(log(n))
		if (test)
			Index.scanning_key_list_time += (System.currentTimeMillis() - start_time);

		if (test)
			start_time = System.currentTimeMillis();

		// all the elements has the same distance, so we have to calculate only once
		float distance = config.calcDistance(config.getDimValue(),
				DataProcessor.getValue(querylong,config.binary_value_range_length* config.num_combination));

		// the time to calculate the distance
		if (test)
			Index.calc_time += (System.currentTimeMillis() - start_time);

		if (enumer == null) {
			if (debug) {
				System.out.println("Not Found: " + querylong);
			}
			return;
		}
		int lucene_doc_id = -1;
		// scanning
		if (test)
			start_time = System.currentTimeMillis();
		
		BytesRef buf = new BytesRef();
		long doc_id;
	
		while ((lucene_doc_id = enumer.nextDoc()) != DocsAndPositionsEnum.NO_MORE_DOCS) {	
			doc_id = idmap[lucene_doc_id];
			float count_dis[] = new float[2];
			// update the table 
			count_dis[0] = 1;
			count_dis[1] = distance;
			revalue.table.put(doc_id, count_dis);
		}

		if (test)
			Index.scanning_value_list_time += (System.currentTimeMillis() - start_time);
	}
	
	/**
	 * search for vectors
	 * 
	 * @return ReturnValue
	 * @throws thowable
	 * */
	public ReturnValue vectorSearch(QueryConfig config) throws Throwable {

		ReturnValue revalue = new ReturnValue();
		revalue.querylong = config.getQuerylong();
		String id_query = String.valueOf(config.queryId) + "+"
				+ config.getQuerylong();
		// to store the keys of last scan
		long[] keys = new long[2];

		// continue scanning from last position
		// find the start position
		if (bi_position_map.containsKey(id_query)) {
			keys = bi_position_map.get(id_query);
			// expand upwards
			long key = keys[0];
			if (key != -1) {
				for (int i = 1; i <= config.getUpRange(); i++) {
					key = createKey(keys[0], 1, UPWARDS, config);
					if (debug)
						System.out.println("expand: " + i + " key: " + key);
					if (key != -1) {
						keys[0] = key;
						scanList(key, config, revalue);
					} else
						break;
				}
			}

			// expand downwards
			key = keys[1];
			if (key != -1) {
				for (int i = 1; i <= config.getDownRange(); i++) {
					key = createKey(keys[1], 1, DOWNWARDS, config);
					if (debug)
						System.out.println("expand: " + i + " key: " + key);
					if (key != -1) {
						keys[1] = key;
						scanList(key, config, revalue);
					} else
						break;
				}
			}

			// store the current position
			bi_position_map.put(id_query, keys);

			// calculate the maximum distance and minimum distance on this dimension
			// minimum distance
			long current_value;
			float dis1, dis2;
			current_value = DataProcessor.getValue(keys[0],
					config.binary_value_range_length
							* config.num_combination);
			dis1 = config.calcDistance(config.getDimValue(), current_value);
			current_value = DataProcessor.getValue(keys[1],
					config.binary_value_range_length
							* config.num_combination);
			dis2 = config.calcDistance(config.getDimValue(), current_value);
			revalue.min_dis = Math.max(dis1, dis2);

			// maximum distance
			dis1 = config.calcDistance(0, config.getDimValue());
			dis2 = config.calcDistance(config.getValueRange(), config
					.getDimValue());
			revalue.max_dis = Math.max(dis1, dis2);

			if (debug) {
				System.out.println("return value distance:"
						+ revalue.max_dis + "\t" + revalue.min_dis);
				if (revalue.max_dis < revalue.min_dis) {
					System.err.println("return value distance:"
							+ revalue.max_dis + "\t" + revalue.min_dis);
					System.exit(-1);
				}
			}
			
		}
		// for the first time
		else {
			scanList(config.getQuerylong(), config, revalue);
			// then expand
			// expand to upwards
			long key = config.getQuerylong();
			for (int i = 1; i <= config.getUpRange(); i++) {
				key = createKey(config.getQuerylong(), i, UPWARDS, config);
				if (debug)
					System.out.println("expand: " + i + " key: " + key);
				if (key != -1) {
					// store the position
					keys[0] = key;
					scanList(key, config, revalue);
				} else
					break;
			}

			key = config.getQuerylong();
			// expand to downwards
			for (int i = 1; i <= config.getDownRange(); i++) {
				key = createKey(config.getQuerylong(), i, DOWNWARDS, config);
				if (debug)
					System.out.println("expand: " + i + " key: " + key);
				if (key != -1) {
					keys[1] = key;
					scanList(key, config, revalue);
				} else
					break;
			}

			// store for next iteration
			bi_position_map.put(id_query, keys);

			// calculate the possible distance
			long current_value;
			float dis1, dis2;
			current_value = DataProcessor.getValue(keys[0],
					config.binary_value_range_length * config.num_combination);
			dis1 = config.calcDistance(config.getDimValue(), current_value);
			current_value = DataProcessor.getValue(keys[1],
					config.binary_value_range_length * config.num_combination);
			dis2 = config.calcDistance(config.getDimValue(), current_value);
			revalue.min_dis = Math.max(dis1, dis2);

			dis1 = config.calcDistance(0, config.getDimValue());
			dis2 = config.calcDistance(config.getValueRange(), config
					.getDimValue());
			revalue.max_dis = Math.max(dis1, dis2);

			if (debug) {
				System.out.println("return value distance:" + revalue.max_dis
						+ "\t" + revalue.min_dis);
				if (revalue.max_dis < revalue.min_dis) {
					System.err.println("return value distance:"
							+ revalue.max_dis + "\t" + revalue.min_dis);
					System.exit(-1);
				}
			}
		}

		return revalue;
	}

	/**
	 * create keys for bi-direction expand
	 * */
	private long createKey(long ori, int length, int direction,
			QueryConfig config) {

		long key = -1;
		if (direction == UPWARDS)
			key = ori - length;
		else
			key = ori + length;
		// expand in the same dimension
		if (DataProcessor.getDim(key, config.binary_value_range_length
				* config.num_combination) == config.getDim())
			return key;
		else
			return -1;
	}


	public void closeWriter() throws Throwable {
		MMwriter.close();
	}

	/**
	 * the following functions serve for the purpose of testing
	 * */ 
	public static void main(String[] args) throws Throwable {

		Index index = new Index();
		int num = 100;
		String indexname = "Index_1_"+num;
		index.setIndexfile(indexname);
		index.init_query();
//		index.testPayload();
	}

	private void testPayload() throws Throwable {
		
		int total = 0;
		
		for(int i = 0; i <128; i++) 
		{
			Term term = new Term(fieldname2, String.valueOf(i));
			BytesRef buf = new BytesRef();	
			int doc_id = -1, lucene_doc_id;
			DocsAndPositionsEnum dp = areader.termPositionsEnum(term);
			
			if(dp != null){ 
					System.out.print(i+":\t");
					//mapping the doc id and lucene id
					while((lucene_doc_id = dp.nextDoc()) != DocsAndPositionsEnum.NO_MORE_DOCS) {
						
						dp.nextPosition();
						buf = dp.getPayload();
						doc_id = PayloadHelper.decodeInt(buf.bytes, buf.offset);
						total++;
						System.out.print("<"+lucene_doc_id+", ");
						System.out.print(doc_id+">  ");
					}
					System.out.println();
			}
		}
		System.out.println(total);

	}
}

class PayloadAnalyzer extends Analyzer {

	private PayloadEncoder encoder;

	PayloadAnalyzer(PayloadEncoder encoder) {
		this.encoder = encoder;
	}

	@Override
	protected TokenStreamComponents createComponents(String fieldname, Reader reader) {
		// TODO Auto-generated method stub
		Tokenizer tokenizer = new WhitespaceTokenizer(Version.LUCENE_45,reader);
		TokenFilter payloadFilter = new DelimitedPayloadTokenFilter(tokenizer, '|', encoder); 
		return new TokenStreamComponents(tokenizer, payloadFilter);
	}
	  
	public void testPayloadAnalyzer() {
		String text = "ID|-1";  
		Analyzer analyzer = new PayloadAnalyzer(new IntegerEncoder());
		Reader reader = new StringReader(text);
		TokenStream ts = null;
		BytesRef br = new BytesRef();
		try {  
            ts = analyzer.tokenStream(null, reader);  
            ts.reset();  
            while(ts.incrementToken()) {  
                CharTermAttribute ta = ts.getAttribute(CharTermAttribute.class);    
                System.out.println(ta.toString());
                br = ts.getAttribute(PayloadAttribute.class).getPayload();
                System.out.println(PayloadHelper.decodeInt(br.bytes,0));
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }   
	}
}

/**
 * The following classes are used to build the priority queue
 * */
class QueueElement {
	
	public long element_id = -1;
	public float upper_bound = Float.MAX_VALUE;
	public float lower_bound = Float.MIN_VALUE;
}


class UpperboundComparator implements Comparator<QueueElement> {

	//build a maximum queue for upper bound
	@Override
	public int compare(QueueElement o1, QueueElement o2) {
		// TODO Auto-generated method stub
		return (int) (o2.upper_bound - o1.upper_bound);
	}
}

class Candidates {
	
	boolean isRealTopK = false;
	QueueElement elements_min_upperbound[];
	QueueElement elements_min_lowerbound[];
}
