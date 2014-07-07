package tool;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class FileProcessor {
    public static final int NUMBER_OF_DIMENSIONS = 128;
    public static final int NUMBER_OF_VALUES_PER_DIM = 256;
    
    public static int[] generateVector() {
    	Random rand = new Random();
    	int[] vector = new int[NUMBER_OF_DIMENSIONS];
    	for (int i = 0; i < NUMBER_OF_DIMENSIONS; i++) {
    		vector[i] = rand.nextInt(NUMBER_OF_VALUES_PER_DIM);
    	}
    	return vector;
    }
    
    public static void generateTextFile(int vectorNum, File dataFile) throws IOException{
    	FileWriter fileWriter = new FileWriter( dataFile);
    	for (int i = 0; i < vectorNum; i++) {
        	StringBuffer strbuf = new StringBuffer();
        	int[] vector = generateVector();
        	for (int j = 0; j < vector.length; j++) {
        		strbuf.append(vector[j] + " ");
        	}
        	strbuf.append("\n");
        	fileWriter.write(strbuf.toString());
    	}
    	fileWriter.close();
    }
    
    public static void main(String[] args) throws IOException {
    	generateTextFile(2, new File("data/rangequery.txt"));
    }
    
}
