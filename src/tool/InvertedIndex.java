package tool;
import com.google.common.math.IntMath;
import org.apache.commons.io.EndianUtils;

import java.io.*;
import java.io.FileReader;
import java.util.ArrayList;

/**
 * Created by yuxin on 12/30/13.
 */
public class InvertedIndex {

	public static boolean debug = false;
	
    public ArrayList<ArrayList<Integer>> invertedIndex;

    public static final int NUMBER_OF_DIMENSIONS = 128;
    public static final int NUMBER_OF_VALUES_PER_DIM = 256;

    public static final int MAX_NUMBER_OF_LISTS = NUMBER_OF_DIMENSIONS * NUMBER_OF_VALUES_PER_DIM;


    public InvertedIndex( )
    {
        initialize();
    }

    public void initialize()
    {
        invertedIndex = new ArrayList<ArrayList<Integer>> (MAX_NUMBER_OF_LISTS);
        for ( int i = 0; i < MAX_NUMBER_OF_LISTS; i ++ )
        {
            invertedIndex.add( new ArrayList<Integer> () );
        }
    }


    /**
     *
     * @param line
     * @param offset : where doe the feature description start
     */
    public void addSiftFeature ( String line, int offset )
    {
        String[] blk = line.split(" ");
        int featureID =  Integer.parseInt( blk[0] );

        for ( int dim = 0; dim < NUMBER_OF_DIMENSIONS; dim ++ )
        {
            // get the index key
            int valueOfDimension = Integer.parseInt( blk[dim+offset] );
            int indexKey = dim * NUMBER_OF_VALUES_PER_DIM + valueOfDimension;

            // add it to the corresponding list
            ArrayList<Integer> invertedList = invertedIndex.get(indexKey);
            invertedList.add( featureID );
        }
    }


    public void addSiftFeature ( int featureID, int[] featureDescriptor )
    {
        for ( int dim = 0; dim < NUMBER_OF_DIMENSIONS; dim ++ )
        {
            // get the index key
            int valueOfDimension = featureDescriptor[dim];
            int indexKey = dim * NUMBER_OF_VALUES_PER_DIM + valueOfDimension;

            // add it to the corresponding list
            ArrayList<Integer> invertedList = invertedIndex.get(indexKey);
            invertedList.add( featureID );
        }
    }


    /**
     * the format of a file should be
     * id v1 v2 v3 ... v127 v128
     *
     *
     * @param readfile
     * @throws IOException
     */
    public void addFeaturesOfFile ( File readfile ) throws IOException
    {
        FileReader fileReader = new FileReader(readfile);
        LineNumberReader reader  = new LineNumberReader( fileReader );

        String lineRead = "";
        while ( (lineRead = reader.readLine()) != null )
        {
            this.addSiftFeature( lineRead, 1 );
        }
        reader.close();
    }



    public int addFeaturesOfBinaryFile ( File readFile, int maxNumberOfFeatures ) throws IOException
    {
        int count = 0;

        DataInputStream in = new DataInputStream(new
                BufferedInputStream(new FileInputStream(readFile)));

        try {
            while (true)
            {
                // read int for id
                int featureID = EndianUtils.swapInteger( in.readInt() );


                if (!debug && featureID % 10000 == 1 )
                {
                    System.out.println("processing " + featureID );
                } else if (debug)
                {
                    System.out.println("processing " + featureID );
                }

                if ( featureID >= maxNumberOfFeatures )
                {
                    break;
                }

                // System.out.println("featureID:" + featureID);
                for ( int dim = 0; dim < NUMBER_OF_DIMENSIONS; dim++ )
                {
                    // read int for sift feature
                    int valueOfDim = EndianUtils.swapInteger( in.readInt() );
                    int indexKey = dim * NUMBER_OF_VALUES_PER_DIM + valueOfDim;
                    if(debug)
                    	System.out.println(dim+" "+valueOfDim+" "+indexKey);
                    // System.out.print(indexKey + " ");
                    // System.out.println("dim " + dim + ":" + valueOfDim);

                    // add it to the corresponding list
                    ArrayList<Integer> invertedList = invertedIndex.get(indexKey);
                    invertedList.add( featureID );
                }

                //System.out.println();
                count ++;
            }
            in.close();
            return count;
        }
        catch (EOFException e)
        {
            // end of the file
            in.close();
            return count;
        }
    }



    /**
     * the file is a binary file, each line of the file contains 129 integers.
     * the first integer is an id, the others are the values of the sift feature
     *
     * @param readFile
     * @throws IOException
     */
    public void addFeaturesOfBinaryFile ( File readFile ) throws IOException
    {
        addFeaturesOfBinaryFile ( readFile, Integer.MAX_VALUE );
    }


    public void addFeaturesOfFolder ( File folder, boolean isBinaryFile ) throws IOException
    {
        File[] fileList = folder.listFiles();

        for ( File file: fileList )
        {
            if ( file.isFile() )
            {
                if ( isBinaryFile )
                {
                    addFeaturesOfBinaryFile( file );
                }
                else
                {
                    addFeaturesOfFile( file );
                }
            }
        }
    }



    public void readAFeature ( String readFile, int id ) throws IOException
    {

        DataInputStream in = new DataInputStream(new
                BufferedInputStream(new FileInputStream(readFile)));


        while ( true )
        {
            // read int for id
            int featureID = EndianUtils.swapInteger( in.readInt() );


            for ( int dim = 0; dim < NUMBER_OF_DIMENSIONS; dim++ )
            {
                // read int for sift feature
                int valueOfDim = EndianUtils.swapInteger( in.readInt() );
                int indexKey = dim * NUMBER_OF_VALUES_PER_DIM + valueOfDim;

                if ( featureID == id )
                {
                    System.out.print(indexKey + " ");
                }
            }

            if ( featureID == id )
            {
                break;
            }

        }
        in.close();
    }

    public int readAndWrite(String readFile, String writeFile, int maxNumberOfFeatures) throws IOException
    {
        int count = 0;

        DataInputStream in = new DataInputStream(new
                BufferedInputStream(new FileInputStream(readFile)));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(writeFile)));
        
        try {
            while (true)
            {
                // read int for id
            	int featureIDBinary = in.readInt();
                int featureID = EndianUtils.swapInteger( featureIDBinary );


                if (!debug && featureID % 10000 == 1 )
                {
                    System.out.println("processing " + featureID );
                } else if (debug)
                {
                    System.out.println("processing " + featureID );
                }

                if ( featureID >= maxNumberOfFeatures )
                {
                    break;
                }
                out.writeInt(featureIDBinary);
                // System.out.println("featureID:" + featureID);
                for ( int dim = 0; dim < NUMBER_OF_DIMENSIONS; dim++ )
                { 
                   out.writeInt( in.readInt());
                }

                //System.out.println();
                count ++;
            }
            in.close();
            out.close();
            return count;
        }
        catch (EOFException e)
        {
            // end of the file
            in.close();
            out.close();
            return count;
        }
    }

    public int readBinaryAndWriteText(String readFile, String writeFile, int maxNumberOfFeatures) throws IOException
    {
        int count = 0;

        DataInputStream in = new DataInputStream(new
                BufferedInputStream(new FileInputStream(new File(readFile))));
        FileWriter out = new FileWriter( new File(writeFile) );

            while (true)
            {
                try {

                // read int for id
            	int featureIDBinary = in.readInt();
                int featureID = EndianUtils.swapInteger( featureIDBinary );


                
                if (!debug && featureID % 10000 == 1 )
                {
                    System.out.println("processing " + featureID );
                } else if (debug)
                {
                    System.out.println("processing " + featureID );
                }

                if ( featureID >= maxNumberOfFeatures )
                {
                    break;
                }
                out.write(featureID+" ");

                // System.out.println("featureID:" + featureID);
                for ( int dim = 0; dim < NUMBER_OF_DIMENSIONS; dim++ )
                { 
                    int value = EndianUtils.swapInteger( in.readInt() );
                   out.write(value+" ");
                   if(featureID == 8638){
                   		System.out.print(value+" ");
                   }
                }
                out.write("\n");
                
                //System.out.println();
                count ++;
                }
                catch (EOFException e)
                {
                    // end of the file
                    in.close();
                    out.close();
                    return count;
                }
            }
            in.close();
            out.close();
            return count;
        

    }
    
    
    /**
     * write the inverted index to the dataFile
     *
     * @param dataFile
     * @throws IOException
     */
    public void writeBinaryFileWithEndianConvertion ( File dataFile, int totalNumberOfFeatures ) throws IOException
    {
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(dataFile)));


        // write the number of features;
        out.writeInt( EndianUtils.swapInteger( totalNumberOfFeatures ) );


        // write the number of dimensions;
        out.writeInt( EndianUtils.swapInteger( NUMBER_OF_DIMENSIONS ) );


        // write the corresponding inverted index in the file
        for ( int i = 0; i < MAX_NUMBER_OF_LISTS; i++ )
        {
            ArrayList<Integer> invertedList = invertedIndex.get(i);

            // write key
            out.writeInt( EndianUtils.swapInteger( i ) );

            // write number of features containing this key
            int numberOfFeatures = invertedList.size();
            out.writeInt(EndianUtils.swapInteger( numberOfFeatures) );

            // write the feature ids containing the key
            if ( numberOfFeatures > 0 )
            {
                for ( int featureID : invertedList )
                {
                    out.writeInt( EndianUtils.swapInteger( featureID ) );
                }
            }
        }

        // close the file
        out.close();
    }


    public void writeBinaryFile ( File dataFile ) throws IOException
    {
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(dataFile)));

        // write the corresponding inverted index in the file
        for ( int i = 0; i < MAX_NUMBER_OF_LISTS; i++ )
        {
            ArrayList<Integer> invertedList = invertedIndex.get(i);

            // write key
            out.writeInt( i );

            // write number of features containing this key
            int numberOfFeatures = invertedList.size();
            out.writeInt( numberOfFeatures );

            // write the feature ids containing the key
            if ( numberOfFeatures > 0 )
            {
                for ( int featureID : invertedList )
                {
                    out.writeInt( featureID );
                }
            }
        }

        // close the file
        out.close();
    }


    public void writeTextFile ( File dataFile ) throws IOException
    {
        FileWriter fileWriter = new FileWriter( dataFile );

        // write the corresponding inverted index in the file
        for ( int i = 0; i < MAX_NUMBER_OF_LISTS; i++ )
        {
            String writeLine = "";
            ArrayList<Integer> invertedList = invertedIndex.get(i);
            int numberOfFeatures = invertedList.size();

            // write key
            writeLine += i + " ";
            // write number of features containing this key
            writeLine += numberOfFeatures + " ";

            // write the feature ids containing the key
            if ( numberOfFeatures > 0 )
            {
                for ( int featureID : invertedList )
                {
                    writeLine += featureID + " ";
                }
            }
            writeLine += "\n";

            fileWriter.write(writeLine);
        }

        // close the file
        fileWriter.close();
    }


    /**
     * read the inverted file
     * @param invertedFile
     * @throws IOException
     */
    public void readBinaryInvertedFile( File invertedFile ) throws IOException
    {

        DataInputStream in = new DataInputStream(new
                BufferedInputStream(new FileInputStream(invertedFile)));


        int totalNumberOfFeatures = EndianUtils.swapInteger( in.readInt() );
        int numberOfDimensions = EndianUtils.swapInteger( in.readInt() );


        // write the corresponding inverted index in the file
        for ( int i = 0; i < MAX_NUMBER_OF_LISTS; i++ )
        {
            // read key
            int key = EndianUtils.swapInteger( in.readInt() );

            // read the number of features containing the key
            int numberOfFeatures =  EndianUtils.swapInteger( in.readInt() );

            if ( numberOfFeatures > 0 )
            {
                for ( int j = 0; j < numberOfFeatures; j++ )
                {
                    int featureID = EndianUtils.swapInteger( in.readInt() );
                }
            }
        }

        in.close();
    }


    public static void main ( String[] args )
    {
        if ( args.length < 2 )
        {
            System.out.println("USAGE: java -jar test.jar 0.readFile 1.writeFile ");
        }


        String readFile = args[0];
        String writeFile = args[1];
        int maxNumberOfElements = Integer.MAX_VALUE;
        
        
        
        readFile = "data/datafile.bin";
        writeFile ="LSHfile_1_100000.txt";
        // read binary files
        InvertedIndex invertedIndex = new InvertedIndex();
        try{
        	invertedIndex.readBinaryAndWriteText(readFile, writeFile, 100000);
        }catch(Exception e){
        	e.printStackTrace();
        }

//        invertedIndex.readAFeature( readFile, 101 );


//        int numberOfFeatures = invertedIndex.addFeaturesOfBinaryFile( new File( readFile ),100000 );

//        System.out.println("# of features: " + numberOfFeatures);
        
//        invertedIndex.writeTextFile(new File("LSHfile_1_100000.txt"));

        // write the inverted files in binary format
        //invertedIndex.writeBinaryFile( new File("data/datafile2.bin"));
        //invertedIndex.writeBinaryFileWithEndianConvertion( new File("data/datafile2.bin"), numberOfFeatures );
        


     }

}
