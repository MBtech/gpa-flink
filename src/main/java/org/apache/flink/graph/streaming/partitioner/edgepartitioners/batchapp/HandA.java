package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.linkanalysis.HITS;
import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.HDRFPartitioner;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.HashPartitioner;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;

public class HandA implements App{

    @Override
    public ExecutionEnvironment exec(ExecutionEnvironment env, String[] args) throws Exception {
        if(!parseParameters(args)) {
            return null;
        }
        env.setParallelism(k);
        // read vertex and edge data
        DataSet<Edge<Long, Double>> data = env.readTextFile(edgesInputPath).map(new MapFunction<String, Edge<Long, Double>>() {

            @Override
            public Edge<Long, Double> map(String s) {
                String[] fields = s.split("\\t");
                long src = Long.parseLong(fields[0]);
                long trg = Long.parseLong(fields[1]);
                return new Edge<>(src, trg, 1.0);
            }
        });
        Partitioner partitioner;
        if (pStrategy.equals("hdrf")){
            partitioner = new HDRFPartitioner<>(new CustomKeySelector(0), k, 1);
        }else{
            partitioner = new HashPartitioner<>(new CustomKeySelector(0));
        }


        data.partitionCustom(partitioner, new CustomKeySelector<>(0)).writeAsCsv(partitionPath, FileSystem.WriteMode.OVERWRITE);

        Graph<Long, Double, Double> graph = Graph.fromDataSet(data.partitionCustom(partitioner, new CustomKeySelector<>(0)),new InitVertices(1.0), env);

        DataSet<HITS.Result<Long>> result = graph.run(new HITS<Long, Double, Double>(maxIterations));

        result.writeAsCsv((outputPath), "\n", " ");

        return env;
    }

    @SuppressWarnings("serial")
    private static final class InitVertices implements MapFunction<Long, Double> {

        private double initValue;

        public InitVertices(double val) {
            this.initValue = val;
        }

        public Double map(Long id) {
            return this.initValue;
        }
    }

    public boolean fileOutput = false;
    public String edgesInputPath = null;
    public String partitionPath = "";
    public String outputPath = null;
    public int maxIterations = 5;
    public int k = 4;
    public String pStrategy = "hash";

    public boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if(args.length != 6) {
                System.err.println("Usage: PageRank" +
                        " <input edges path> <partition path> <output path>  <num iterations> <number of partitions> <strategy>");
                return false;
            }
            edgesInputPath = args[0];
            partitionPath = args[1];
            outputPath = args[2];
            maxIterations = Integer.parseInt(args[3]);
            k = Integer.parseInt(args[4]);
            pStrategy = args[5];

        }
        return true;
    }

}
