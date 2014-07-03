package vector_knn;

//import com.google.common.math.IntMath;

import org.apache.commons.io.EndianUtils;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by yuxin on 12/30/13.
 */
public class Reader {

	ArrayList<ArrayList<Integer>> invertedIndex;

	public static final int NUMBER_OF_DIMENSIONS = 128;
	public static final int NUMBER_OF_VALUES_PER_DIM = 256;

	public static final int MAX_NUMBER_OF_LISTS = NUMBER_OF_DIMENSIONS
			* NUMBER_OF_VALUES_PER_DIM;
	
	private String filename;
	private DataInputStream in;

	public String getFeature() throws IOException {

		String feature = "";
		// read int for id
		int featureID = EndianUtils.swapInteger(in.readInt());
		feature += String.valueOf(featureID)+"|";
		
		for (int dim = 0; dim < NUMBER_OF_DIMENSIONS; dim++) {
			// read int for sift feature
			int valueOfDim = EndianUtils.swapInteger(in.readInt());
			feature += (dim+"+"+valueOfDim+"|");
		}

		return feature;
	}
	
	//the last element in the integer list is the feature ID
	public int[] getFeature(int length) throws IOException {

		int value[] = new int[length + 1];
		// read int for id
		int featureID = EndianUtils.swapInteger(in.readInt());
		value[length] = featureID;	
		for (int dim = 0; dim < NUMBER_OF_DIMENSIONS; dim++) {
			// read int for sift feature
			int valueOfDim = EndianUtils.swapInteger(in.readInt());
			value[dim] = valueOfDim;
		}

		return value;
	}
	
	
	public void closeReader() {
		
		try {
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void openReader() {
		
		try {
			in = new DataInputStream(new BufferedInputStream(
					new FileInputStream(filename)));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Reader (String filename) {
		
		this.filename =filename;  
	}
	
	public static void main(String[] args) throws IOException {
		
		Reader reader = new Reader("data/siftgeo.bin");
		reader.openReader();
		int i = 0;
		while(true) {
			i++;
			if(i % 10000 == 0)
			System.out.println(i);
//			System.out.println(reader.getFeature());
			reader.getFeature();
		}
	}

}
