package lsh;

import java.util.List;

import lucene.ReturnValue;

public class LshManager {
		
	private int numberOfHashTables;
	private int numberOfHashes;
	private int numberOfNeighbours;
	private double radius;
	
	private List<Vector> dataset;
	private List<Vector> queries;
	
	private int dimensions;
	private DistanceMeasure measure;
	private int timeout = 40; //seconds timeout for radius search.
	private HashFamily family;
	
	private boolean benchmark;
	private boolean printHelp;
	
	public LshManager(String datasetFile, String queryFile) {
		if(datasetFile !=null){
			dataset = LSH.readDataset(datasetFile,Integer.MAX_VALUE);
			dimensions = dataset.get(0).getDimensions();
		}
		if(queryFile != null){
			queries = LSH.readDataset(queryFile,Integer.MAX_VALUE);
			dimensions = queries.get(0).getDimensions();
		}
		//determine the radius for hash bins automatically
		System.out.println("Radius for Euclidean distance.");
		int radiusEuclidean = (int) LSH.determineRadius(dataset, new EuclideanDistance(), 20);
		family = new EuclidianHashFamily(radiusEuclidean,dimensions);
	}
	
	
	
	public ReturnValue startLSH(){
		LSH lsh = new LSH(dataset, family);
		lsh.buildIndex(numberOfHashes,numberOfHashTables);		
		ReturnValue revalue = new ReturnValue();
		if(queries != null){
			for(Vector query:queries){
				List<Vector> neighbours = lsh.query(query, numberOfNeighbours);
				System.out.println(query.getKey());
				for(Vector neighbour:neighbours){
					float count_dis[] = new float[2];
					count_dis[0] = 128;
					count_dis[1] = (float)family.createDistanceMeasure().distance(query, neighbour);				
					revalue.table.put(neighbour.getKey(), count_dis);
					System.out.print(neighbour.getKey() + ";");
				}
				System.out.print("\n");
			}
		}
	}
	
}
