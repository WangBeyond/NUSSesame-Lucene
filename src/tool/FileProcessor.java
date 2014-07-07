package tool;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Random;

public class FileProcessor {
    public static final int NUMBER_OF_DIMENSIONS = 128;
    public static final int NUMBER_OF_VALUES_PER_DIM = 256;
    public static final int RANGE_RANGE = 40;

    
    
    public static int[] generateValueVector() {
    	Random rand = new Random();
    	int[] vector = new int[NUMBER_OF_DIMENSIONS];
    	for (int i = 0; i < NUMBER_OF_DIMENSIONS; i++) {
    		vector[i] = rand.nextInt(NUMBER_OF_VALUES_PER_DIM);
    	}
    	return vector;
    }
    
    public static int[] generateRangeVector() {
    	Random rand = new Random();
    	int[] vector = new int[NUMBER_OF_DIMENSIONS];
    	for (int i = 0; i < NUMBER_OF_DIMENSIONS; i++) {
    		vector[i] = rand.nextInt(40);
    	}
    	return vector;
    }
    
    public static float[] generateWeightVector() {
    	Random rand = new Random();
    	float[] vector = new float[NUMBER_OF_DIMENSIONS];
    	float sum = 0;
    	for (int i = 0; i < NUMBER_OF_DIMENSIONS; i++) {
    		vector[i] = rand.nextFloat();
    		sum += vector[i];
    	}
    	for (int i = 0; i < NUMBER_OF_DIMENSIONS; i ++) {
    		vector[i] = vector[i]/sum;
    	}
    	return vector;
    }
    
    public static void generateTextFile(int vectorNum, File dataFile) throws IOException{
    	FileWriter fileWriter = new FileWriter( dataFile);
    	for (int i = 0; i < vectorNum; i++) {
        	StringBuffer strbuf = new StringBuffer();
        	int[] valueVector = generateValueVector();
        	int[] rangeVector = generateRangeVector();
        	float[] weightVector = generateWeightVector();
        	
        	//append the three vectors to the string buffer
        	for (int j = 0; j < valueVector.length; j++) {
        		strbuf.append(valueVector[j] + " ");
        	}
        	strbuf.append("\n");
        	for (int j = 0; j < rangeVector.length; j++) {
        		strbuf.append(rangeVector[j] + " ");
        	}
        	strbuf.append("\n");
        	for (int j = 0; j < weightVector.length; j++) {
        		strbuf.append(weightVector[j] + " ");
        	}
        	strbuf.append("\n");
        	
        	fileWriter.write(strbuf.toString());
    	}
    	fileWriter.close();
    }
    
    public static void main(String[] args) throws IOException {
    	generateTextFile(1, new File("data/rangequery.txt"));
    }
    
}
