package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.shaded.guava18.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava18.com.google.common.collect.Table;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 22/06/16.
 */
public class DegreeConnectedp {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges.partitionCustom(new DegreeBasedPartitioner(new CustomKeySelector(0), vertexCount, edgeCount), new CustomKeySelector(0)),env);

		DataStream<DisjointSet<Long>> cc2 = graph.aggregate(new ConnectedComponentss<Long, NullValue>(5000000));

		cc2.map(new MapFunction<DisjointSet<Long>, Long>() {

			@Override
			public Long map(DisjointSet<Long> longDisjointSet) throws Exception {
				return ((long) longDisjointSet.getMatches().size());
			}

		}).writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

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

	private static String InputPath = null;
	private static String outputPath = null;
	private static String log = null;
	private static int vertexCount = 100;
	private static int edgeCount = 1000;
	private static String file1 = null;
	private static String file2 = null;
	private static int count = 0;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 7) {
				System.err.println("Usage: DegreeBasedAdvacned <input edges path> <output path> <log> <vertex count> <edge count>");
				return false;
			}

			InputPath = args[0];
			outputPath = args[1];
			log = args[2];
			vertexCount = Integer.parseInt(args[3]);
			edgeCount = Integer.parseInt(args[4]);
			file1 = args[5];
			file2 = args[6];

		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println(" Usage: DegreeBasedAdvacned <input edges path> <output path> <log> <vertex count> <edge count>");
		}
		return true;
	}

	private static class CustomKeySelector<K, EV> implements KeySelector<Edge<K, EV>, K> {
		private final int key1;
		private EV key2;
		private static final HashMap<Object, Object> KeyMap = new HashMap<>();

		public CustomKeySelector(int k) {
			this.key1 = k;
		}

		public K getKey(Edge<K, EV> edge) throws Exception {
			KeyMap.put(edge.getField(key1), edge.getField(key1 + 1));
			return edge.getField(key1);
		}

		public EV getValue(Object k) throws Exception {
			key2 = (EV) KeyMap.get(k);
			KeyMap.clear();
			return key2;

		}
	}


	///////code for partitioner/////////
	private static class DegreeBasedPartitioner<K, EV, T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector<T, ?> keySelector;
		private final Table<Long, Long, Long> Degree = HashBasedTable.create();   //for <partition.no, vertexId, Degree>
		private final HashMap<Long, List<Tuple2<Long, Long>>> Result = new HashMap<>();
		private final List<Double> load = new ArrayList<>(); //for load of each partiton
		private final List<Long> subset = new ArrayList<>();
		private Long k;   //no. of partitions
		private Double loadlimit = 0.0;
		private int m = 0;  // no. of edges
		private int n = 0;
		private double alpha = 0;  //parameters for formula
		private double gamma = 0;

		public DegreeBasedPartitioner(CustomKeySelector<T, ?> keySelector, int n, int m) {
			this.keySelector = keySelector;
			this.k = (long) 4;
			this.m = m;
			this.n = n;
			this.alpha = (((Math.pow(k, 0.5)) * Math.pow(n, 1.5)) + m) / Math.pow(n, 1.5);
			this.gamma = 1.5;
			this.loadlimit = (k * 1.1 + m) / k;
			;
		}

		@Override
		public int partition(Object key, int numPartitions) {

			Long target = 0L;
			try {
				target = (Long) keySelector.getValue(key);
			} catch (Exception e) {
				e.printStackTrace();
			}

			long source = (long) key;


			int h = 0;


			if (Degree.isEmpty()) {
				for (int j = 0; j < k; j++) {
					load.add(j, 0.0);
				}
				load.set(0, 1.0);
				Degree.put((long) 0, source, (long) 0);
				Degree.put((long) 0, target, (long) 1);
				List<Tuple2<Long, Long>> L = new ArrayList<>();
				L.add(new Tuple2<>(source, target));
				Result.put((long) 0, L);
				h = 0;

			} else {
				//condition 1 both vertices in same partition
				for (int i = 0; i < k; i++) {

					int value = 0;
					value = getValue(source, target, i);
					subset.add(i, (long) value);

					//get the value of S(k) from each partition using return
				}

				h = Cost(source, target);

				Long d1 = Degree.get((long) h, source);
				Long d2 = Degree.get((long) h, target);
				if (d1 == null) {
					d1 = (long) 0;
				}
				if (d2 == null) {
					d2 = (long) 0;
				}
				//d1++;
				d2++;
				Double l = load.get(h);
				l++;
				load.set(h, l);
				Degree.put((long) h, source, d1);
				Degree.put((long) h, target, d2);
				if (Result.get((long) h) != null) {
					List<Tuple2<Long, Long>> L = Result.get((long) h);
					L.add(new Tuple2<>(source, target));
					Result.put((long) h, L);
				} else {
					List<Tuple2<Long, Long>> L = new ArrayList<>();
					L.add(new Tuple2<>(source, target));
					Result.put((long) h, L);
				}
				subset.clear();
			}

			return h;
		}

		public int Cost(Long source, Long target) {
			Long max = subset.get(0);
			int sub = 0;
			for (int j = 1; j < k; j++) {

				if (max.compareTo( subset.get(j)) < 0 && load.get(j).compareTo(loadlimit) < 0) {
					max = subset.get(j);
					sub = j;
				} else if (max.equals( subset.get(j)) && load.get(j).compareTo(loadlimit) < 0 && subset.get(j).equals((long)1) ) {
					if (Degree.get((long) j, source) != null && Degree.get((long) sub, target) != null) {
						Double cost1 = Degree.get((long) j, source) + alpha * gamma * Math.pow(load.get(j), gamma - 1);
						Double cost2 = Degree.get((long) sub, target) + alpha * gamma * Math.pow(load.get(sub), gamma - 1);
						if (cost1.compareTo(cost2) < 0) {
							max = subset.get(j);
							sub = j;
						} else if (load.get(j).compareTo(load.get(sub)) < 0) {
							max = subset.get(j);
							sub = j;
						}

					} else if (Degree.get((long) j, target) != null && Degree.get((long) sub, source) != null) {
						Double cost1 = Degree.get((long) j, target) + alpha * gamma * Math.pow(load.get(j), gamma - 1);
						Double cost2 = Degree.get((long) sub, source) + alpha * gamma * Math.pow(load.get(sub), gamma - 1);
						if (cost1.compareTo(cost2) < 0) {
							max = subset.get(j);
							sub = j;
						} else if (load.get(j).compareTo(load.get(sub)) < 0) {
							max = subset.get(j);
							sub = j;
						}

					} else {
						if (load.get(j).compareTo(load.get(sub)) < 0) {
							max = subset.get(j);
							sub = j;
						}
					}

				} else if (max.equals( subset.get(j)) && subset.get(j).equals((long)0) && load.get(j).compareTo(load.get(sub)) < 0) {
					max = subset.get(j);
					sub = j;
				} else if (load.get(sub).compareTo(loadlimit) > 0) {

					int min = j;
					Double min_load = subset.get(min) + alpha * gamma * Math.pow(load.get(min), gamma - 1);
					for (int i = min+1; i < 4; i++) {
						if (min_load.compareTo(subset.get(i) + alpha * gamma * Math.pow(load.get(i), gamma - 1)) < 0) {
							min_load = subset.get(i) + alpha * gamma * Math.pow(load.get(i), gamma - 1);
							min = i;
						}

					}
					sub = min;
				}
			}

			return sub;


		}

		public int getValue(Long source, Long target, int p) {
			{
				int i = 0;
				if (Degree.contains((long) p, source) && Degree.contains((long) p, target)) {
					i = 2;
				} else if (Degree.contains((long) p, source) && !Degree.contains((long) p, target)) {
					i = 1;
				} else if (!Degree.contains((long) p, source) && Degree.contains((long) p, target)) {
					i = 1;
				} else {
					i = 0;
				}
				return i;
			}

		}
	}

	private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) throws IOException {
		return env.fromCollection(getEdges());
	}

	public static final List<Edge<Long, NullValue>> getEdges() throws IOException {
		List<Edge<Long, NullValue>> edges = new ArrayList<>();

		FileReader inputFile = new FileReader(InputPath);
		//Instantiate the BufferedReader Class

		BufferedReader bufferReader = new BufferedReader(inputFile);

		String line;
		// Read file line by line and print on the console
		while ((line = bufferReader.readLine()) != null) {
			String[] fields = line.split("\\,");
			long src = Long.parseLong(fields[0]);
			long trg = Long.parseLong(fields[1]);

			edges.add(new Edge<>(src, trg, NullValue.getInstance()));
		}


		return edges;
	}

	private static class ConnectedComponentss<K extends Serializable, EV> extends WindowGraphAggregation<K, EV, DisjointSet<K>, DisjointSet<K>> implements Serializable {

		private long mergeWindowTime;

		public ConnectedComponentss(long mergeWindowTime) {

			super(new UpdateCC(), new CombineCC(), new DisjointSet<K>(), mergeWindowTime, false);
		}


		public final static class UpdateCC<K extends Serializable> implements EdgesFold<K, NullValue, DisjointSet<K>> {

			@Override
			public DisjointSet<K> foldEdges(DisjointSet<K> ds, K vertex, K vertex2, NullValue edgeValue) throws Exception {
				ds.union(vertex, vertex2);
				return ds;
			}
		}

		public static class CombineCC<K extends Serializable> implements ReduceFunction<DisjointSet<K>> {

			@Override
			public DisjointSet<K> reduce(DisjointSet<K> s1, DisjointSet<K> s2) throws Exception {

				count++;
				int count1 = s1.getMatches().size();
				int count2 = s2.getMatches().size();

				try {

					FileWriter fw1 = new FileWriter(file1, true); //the true will append the new data
					fw1.write(count1 + "\n");//appends the string to the file
					fw1.close();

				} catch (IOException ioe) {
					System.err.println("IOException: " + ioe.getMessage());
				}

				if (count1 > count2) {
					try {

						FileWriter fw2 = new FileWriter(file2, true); //the true will append the new data
						fw2.write(count1 + "\n");//appends the string to the file
						fw2.close();

					} catch (IOException ioe) {
						System.err.println("IOException: " + ioe.getMessage());
					}
				}
				if (count1 <= count2) {
					try {
						FileWriter fw2 = new FileWriter(file2, true); //the true will append the new data
						fw2.write(count2 + "\n");//appends the string to the file
						fw2.close();
					}
					catch (IOException ioe) {
						System.err.println("IOException: " + ioe.getMessage());}
					s2.merge(s1);
					return s2;
				}
				s1.merge(s2);
				return s1;
			}
		}
	}

}
