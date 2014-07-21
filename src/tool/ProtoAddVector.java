package tool;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintStream;

import tool.VectorListProto.Vector;
import tool.VectorListProto.VectorList;

public class ProtoAddVector {
	// This function fills in a Person message based on user input.
	public static Vector createVector(long id, int[] values) {
		Vector.Builder vector = Vector.newBuilder();

		vector.setVectorId(id);
		for (int i = 0; i < values.length; i++) {
			vector.addValue(values[i]);
		}

		return vector.build();
	}

	// This function fills in a Person message based on user input.
	public static Vector createVector(long id, int[] values, int numValues) {
		Vector.Builder vector = Vector.newBuilder();

		vector.setVectorId(id);
		for (int i = 0; i < numValues; i++) {
			vector.addValue(values[i]);
		}

		return vector.build();
	}

	public static void writeToFile(String file) throws IOException {
		VectorList.Builder vectorList = VectorList.newBuilder();
		for ( int i = 0; i < 100; i++) {
			int[] values = new int[128];
			for (int j = 0; j < 128; j++) {
				values[j] = 100;
			}
			Vector vector = createVector(1000, values);
			vectorList.addVector(vector);
		}

		
		// Write the new address book back to disk.
		FileOutputStream output = new FileOutputStream(file);
		vectorList.build().writeTo(output);
		output.close();
	}

	public static void readFile(String file) throws IOException {

		// Read the existing address book.
		VectorList vectorList = VectorList.parseFrom(new FileInputStream(file));
		print(vectorList);

	}

	public static void print(VectorList vectorList) {
		for (int i = 0; i < vectorList.getVectorCount(); i++) {
			Vector vector = vectorList.getVector(i);
			System.out.print(vector.getVectorId() + "\t");
			for (int j = 0; j < vector.getValueCount(); j++) {
				System.out.print(vector.getValue(j) + " ");
			}
			System.out.println();
		}
	}
	
	// Main function: Reads the entire addre ss book from a file,
	// adds one person based on user input, then writes it back out to the same
	// file.
	public static void main(String[] args) throws Exception {
		//writeToFile("data/protoTest.bin");
		readFile("data/protoTest.bin");
	}
}