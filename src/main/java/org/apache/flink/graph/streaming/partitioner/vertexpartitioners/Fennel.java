package org.apache.flink.graph.streaming.partitioner.vertexpartitioners;

/**
 * Created by zainababbas on 27/04/16.
 */

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.streaming.partitioner.object.StoredObject;
import org.apache.flink.graph.streaming.partitioner.object.StoredState;
import org.apache.flink.graph.streaming.partitioner.vertexpartitioners.keyselector.CustomKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class Fennel {

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Tuple2<Long, List<Long>>> vertices = getVertices(env);


		vertices.partitionCustom(new FennelPartitioner<>(new CustomKeySelector<>(0), k, vertexCount, edgeCount), new CustomKeySelector<>(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);


		JobExecutionResult result = env.execute("My Flink Job");

		try {
			FileWriter fw = new FileWriter(log, true); //the true will append the new data
			fw.write("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute"+"\n");//appends the string to the file
			fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute"+"\n");
			fw.close();
		} catch (IOException ioe) {
			System.err.println("IOException: " + ioe.getMessage());
		}

		System.out.println("abc");
	}




	//private static DataStream<Tuple3<Long, List<Long>,Long>> getGraphStream(StreamExecutionEnvironment env) throws IOException {

	//	return env.fromCollection(getVertices(env));
//	}


	private static String InputPath = null;
	private static String outputPath = null;
	private static String log = null;
	private static int k = 0;
	private static int vertexCount = 0;
	private static int edgeCount = 0;


	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			System.out.println(args.length);
			if(args.length != 6) {
				System.err.println("Usage: FennelLog <input edges path> <output path> <log> <partitions> <vertex count> <edge count>");
				return false;
			}

			InputPath = args[0];
			outputPath = args[1];
			log = args[2];
			k = (int) Long.parseLong(args[3]);
			vertexCount = Integer.parseInt(args[4]);
			edgeCount = Integer.parseInt(args[5]);
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  Usage: FennelLog <input edges path> <output path> <log> <partitions> <vertex count> <edge count>");
		}
		return true;
	}

	public static DataStream<Tuple2<Long, List<Long>>> getVertices(StreamExecutionEnvironment env) throws IOException {

		List<Tuple2<Long, List<Long>>> vertices = new ArrayList<>();

		return env.readTextFile(InputPath)
					   .map(new MapFunction<String, Tuple2<Long, List<Long>>>() {
						   @Override
						   public Tuple2<Long, List<Long>> map(String s) throws Exception {
							   String[] fields = s.split("\\[");
							   String src = fields[0];
							   int h=src.indexOf(':');
							   src=src.substring(0,h);
							   Long source = Long.parseLong(src);
							   String trg= fields[1];

							   long n = trg.indexOf("]");
							   String j = trg.substring(0, (int) n);
							   String fg =j.replaceAll("\\s","");
							   String[] ne = fg.split("\\,");
							   int f = ne.length;
							   List<Long> neg = new ArrayList<Long>();
							   neg.add(Long.parseLong(ne[0]));
							   for (int k = 1; k < f; k++) {
								   neg.add(Long.parseLong(String.valueOf(ne[k])));

							   }
							   return new Tuple2<Long, List<Long>>(source, neg);
						   }
					   });

	}


	///////code for partitioner/////////

	private static class FennelPartitioner<T> implements Serializable, Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;
		private double k;  //no. of partitions
		private double alpha = 0.0;  //parameters for formula
		private double gamma = 0.0;
		private double loadlimit = 0.0;     //k*v+n/n
		private double n = 0.0;        // no of vertices
		private double m = 0.0;        //no. of edges
		StoredState currentState;

		public FennelPartitioner(CustomKeySelector keySelector, int k, int n, int m) {
			this.keySelector = keySelector;
			this.k = (double) k;
			this.n = (double) n;
			this.m = (double) m;
			this.alpha = ((Math.pow(k, 0.5) * Math.pow(n, 1.5)) + m) / Math.pow(n, 1.5);
			this.gamma = 1.5;
			this.loadlimit = (k * 1.1 + n) / k;
			this.currentState = new StoredState(k);
		}

		@Override
		public int partition(Object key, int numPartitions) {

			List neighbours = new ArrayList<>();

			try {
				neighbours = (List) keySelector.getValue(key);
			} catch (Exception e) {
				e.printStackTrace();
			}

			long source = (long) key;

			int machine_id = -1;


			StoredObject first_vertex = currentState.getRecord(source);
			StoredObject[] n_vertices = new StoredObject[neighbours.size()];

			for(int i=0;i<neighbours.size();i++)

			{
				n_vertices[i]= currentState.getRecord((Long) neighbours.get(i));

			}

			LinkedList<Integer> candidates = new LinkedList<Integer>();
			double MAX_SCORE =  Double.NEGATIVE_INFINITY;

			for (int p = 0; p < numPartitions; p++) {

				int occurences=0;
				for(int i=0;i<neighbours.size();i++)

				{

					if(n_vertices[i].hasReplicaInPartition(p))
					{occurences++;}

				}
				double SCORE_m = -1;
				if(currentState.getMachineVerticesLoad(p) <= loadlimit) {
					SCORE_m = (double) occurences - alpha * gamma * Math.pow((double) currentState.getMachineVerticesLoad(p), gamma - 1);
				}

				else if(currentState.getMachineVerticesLoad(p) > loadlimit) {
					SCORE_m = Double.NaN;
				}

				if (SCORE_m > MAX_SCORE) {
					MAX_SCORE = SCORE_m;
					candidates.clear();
					candidates.add(p);
				} else if (SCORE_m == MAX_SCORE) {
					candidates.add(p);
				}

			}


			if (candidates.isEmpty()) {
				System.out.println("ERROR: GreedyObjectiveFunction.performStep -> candidates.isEmpty()");
				System.out.println("MAX_SCORE: " + MAX_SCORE);
				System.exit(-1);
			}

			//*** PICK A RANDOM ELEMENT FROM CANDIDATES
			Random r = new Random();
			int choice = r.nextInt(candidates.size());
			machine_id = candidates.get(choice);


			if (currentState.getClass() == StoredState.class) {
				StoredState cord_state = (StoredState) currentState;
				//NEW UPDATE RECORDS RULE TO UPDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
				if (!first_vertex.hasReplicaInPartition(machine_id)) {
					first_vertex.addPartition(machine_id);
					cord_state.incrementMachineLoadVertices(machine_id);
				}

			} else {
				//1-UPDATE RECORDS
				if (!first_vertex.hasReplicaInPartition(machine_id)) {
					first_vertex.addPartition(machine_id);
				}

			}


			//System.out.print("source" + source);
			//System.out.println(machine_id);

			return machine_id;

		}


	}

}

