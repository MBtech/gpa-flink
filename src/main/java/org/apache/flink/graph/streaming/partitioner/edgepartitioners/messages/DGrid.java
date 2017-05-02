package org.apache.flink.graph.streaming.partitioner.edgepartitioners.messages;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.library.DegreeCheck;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;
import org.apache.flink.graph.streaming.partitioner.object.StoredObject;
import org.apache.flink.graph.streaming.partitioner.object.StoredState;
import org.apache.flink.graph.streaming.partitioner.tests.DumSink5;
import org.apache.flink.graph.streaming.summaries.HMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;

/**
 * Created by zainababbas on 24/02/2017.
 */
public class DGrid {
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		env.setParallelism(k);

		DataStream<Edge<Long, NullValue>> partitionesedges = edges.partitionCustom(new GridPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0));
		//partitionesedges.addSink(new DumSink2());
		// 1. emit (vertexID, 1) or (vertexID, -1) for addition or deletion
		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(partitionesedges,env);
		DataStream<HMap> D=graph.aggregate(new DegreeCheck<Long, NullValue>(5000));
		D.addSink(new DumSink5());

		/*D.flatMap(new FlattenSet()).keyBy(0)
				.timeWindow(Time.of(50, TimeUnit.MILLISECONDS))
				.fold(new Tuple2<Long, Integer>(0l, 0), new IdentityFold()).print();*/

		// group by vertex ID and maintain degree per vertex

		// group by degree and emit current count
		//.keyBy(0).map(new DegreeDistributionMap())

		// .writeAsText(resultPath);

		env.execute("Streaming Degree Distribution");
	}


	private static final class EmitVerticesWithChange implements
			FlatMapFunction<Edge<Long, NullValue>, Tuple2<Long, Integer>> {

		public void flatMap(Edge<Long, NullValue> t, Collector<Tuple2<Long, Integer>> c) {
			// output <vertexID, degreeChange>
			int change = 1 ;
			c.collect(new Tuple2<>(t.f0, change));
			c.collect(new Tuple2<>(t.f1, change));
		}
	}

	public static final class FlattenSet implements FlatMapFunction<HMap, Tuple2<Long, Integer>> {

		private Tuple2<Long, Integer> t = new Tuple2<>();

		@Override
		public void flatMap(HMap set, Collector<Tuple2<Long, Integer>> out) {
			for (Long vertex : set.getmap().keySet()) {
				Integer degree = set.getmap().get(vertex);
				t.setField(vertex, 0);
				t.setField(degree, 1);
				out.collect(t);
			}
		}
	}


	public static final class IdentityFold implements FoldFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>> {
		public Tuple2<Long, Integer> fold(Tuple2<Long, Integer> accumulator, Tuple2<Long, Integer> value) throws Exception {
			return value;
		}
	}


	/**
	 * Maintains a hash map of vertex ID -> degree and emits changes in the form of (degree, change).
	 */
	private static final class VertexDegreeCounts implements FlatMapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>> {

		Map<Long, Integer> verticesWithDegrees = new HashMap<>();
		private  static int count = 0;

		public void flatMap(Tuple2<Long, Integer> t, Collector<Tuple2<Long, Integer>> c) {
			// output <degree, localCount>
			if (verticesWithDegrees.containsKey(t.f0)) {
				// update existing vertex
				int oldDegree = verticesWithDegrees.get(t.f0);
				int newDegree = oldDegree + t.f1;
				if (newDegree > 0) {
					verticesWithDegrees.put(t.f0, newDegree);
					c.collect(new Tuple2<>(t.f0, newDegree));
				} else {
					// if the current degree is <= 0: remove the vertex
					verticesWithDegrees.remove(t.f0);
				}
				c.collect(new Tuple2<>(t.f0, newDegree));
			} else {
				count++;
				System.out.println("count: "+count);
				// first time we see this vertex
				if (t.f1 > 0) {
					verticesWithDegrees.put(t.f0, 1);
					c.collect(new Tuple2<>(t.f0, 1));
				}
			}
		}
	}


	private static String InputPath = null;
	private static String outputPath = null;
	private static String log = null;
	private static int k = 0;
	private static int count = 0;
	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 4) {
				System.err.println("Usage: Dbh <input edges path> <output path> <log> <partitions> ");
				return false;
			}

			InputPath = args[0];
			outputPath = args[1];
			log = args[2];
			k = (int) Long.parseLong(args[3]);
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println(" Usage: Dbh <input edges path> <output path> <log> <partitions>");
		}
		return true;
	}

	public static  DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) throws IOException {

		return env.readTextFile(InputPath)
					   .map(new MapFunction<String, Edge<Long, NullValue>>() {
						   @Override
						   public Edge<Long, NullValue> map(String s) throws Exception {
							   String[] fields = s.split("\\ ");
							   long src = Long.parseLong(fields[0]);
							   long trg = Long.parseLong(fields[1]);
							   return new Edge<>(src, trg, NullValue.getInstance());
						   }
					   });

		/*return env.fromElements(
				new Edge<>(1L, 2L, NullValue.getInstance()),
				new Edge<>(12L, 3L, NullValue.getInstance()),
				new Edge<>(1L, 4L, NullValue.getInstance()),
				new Edge<>(1L, 6L, NullValue.getInstance()),
				new Edge<>(1L, 7L, NullValue.getInstance()),
				new Edge<>(1L, 8L, NullValue.getInstance()),
				new Edge<>(1L, 9L, NullValue.getInstance()),
				new Edge<>(1L, 10L, NullValue.getInstance()),
				new Edge<>(1L, 11L, NullValue.getInstance()),
				new Edge<>(1L, 12L, NullValue.getInstance()),
				new Edge<>(1L, 13L, NullValue.getInstance()),

				//		new Tuple3<>(2, 3, EventType.EDGE_DELETION),
				//		new Tuple3<>(3, 4, EventType.EDGE_ADDITION),
				new Edge<>(1L, 5L, NullValue.getInstance()));*/
	}

	private static class GridPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;

		private static final int MAX_SHRINK = 100;
		private double seed;
		private int shrink;
		private int k;
		private int nrows, ncols;
		LinkedList<Integer>[] constraint_graph;
		StoredState currentState;

		public GridPartitioner(CustomKeySelector keySelector, int k)
		{
			this.keySelector = keySelector;
			this.k= k;
			this.seed = Math.random();
			Random r = new Random();
			shrink = r.nextInt(MAX_SHRINK);
			this.constraint_graph = new LinkedList[k];
			this.currentState = new StoredState(k);

		}

		@Override
		public int partition(Object key, int numPartitions) {

			long target = 0L;
			try {
				target = (long) keySelector.getValue(key);
			} catch (Exception e) {
				e.printStackTrace();
			}

			long source = (long) key;


			make_grid_constraint();

			int machine_id = -1;

			StoredObject first_vertex = currentState.getRecord(source);
			StoredObject second_vertex = currentState.getRecord(target);


			int shard_u = Math.abs((int) ( (int) source*seed*shrink) % k);
			int shard_v = Math.abs((int) ( (int) target*seed*shrink) % k);

			LinkedList<Integer> costrained_set = (LinkedList<Integer>) constraint_graph[shard_u].clone();
			costrained_set.retainAll(constraint_graph[shard_v]);

			//CASE 1: GREEDY ASSIGNMENT
			LinkedList<Integer> candidates = new LinkedList<Integer>();
			int min_load = Integer.MAX_VALUE;
			for (int m : costrained_set){
				int load = currentState.getMachineLoad(m);
				if (load<min_load){
					candidates.clear();
					min_load = load;
					candidates.add(m);
				}
				if (load == min_load){
					candidates.add(m);
				}
			}
			//*** PICK A RANDOM ELEMENT FROM CANDIDATES
			Random r = new Random();
			int choice = r.nextInt(candidates.size());
			machine_id = candidates.get(choice);

			//UPDATE EDGES
			Edge e = new Edge<>(source, target, NullValue.getInstance());
			currentState.incrementMachineLoad(machine_id,e);

			//UPDATE RECORDS
			if (currentState.getClass() == StoredState.class){
				StoredState cord_state = (StoredState) currentState;
				//NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
				if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
				if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
			}
			else{
				//1-UPDATE RECORDS
				if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id);}
				if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id);}
			}



			//System.out.print("source"+source);
			//System.out.println("target"+target);
			//System.out.println("machineid"+machine_id);

			return machine_id;
		}

		private void make_grid_constraint() {
			initializeRowColGrid();
			for (int i = 0; i < k; i++) {
				LinkedList<Integer> adjlist = new LinkedList<Integer>();
				// add self
				adjlist.add(i);
				// add the row of i
				int rowbegin = (i/ncols) * ncols;
				for (int j = rowbegin; j < rowbegin + ncols; ++j)
					if (i != j) adjlist.add(j);
				// add the col of i
				for (int j = i % ncols; j < k; j+=ncols){
					if (i != j) adjlist.add(j);
				}
				Collections.sort(adjlist);
				constraint_graph[i]=adjlist;
			}

		}

		private void initializeRowColGrid() {
			double approx_sqrt = Math.sqrt(k);
			nrows = (int) approx_sqrt;
			for (ncols = nrows; ncols <= nrows + 2; ++ncols) {
				if (ncols * nrows == k) {
					return;
				}
			}
			System.out.println("ERRORE Num partitions "+k+" cannot be used for grid ingress.");
			System.exit(-1);
		}





	}


}