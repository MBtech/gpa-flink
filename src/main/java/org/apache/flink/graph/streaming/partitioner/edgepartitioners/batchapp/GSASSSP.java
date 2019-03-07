package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

/**
 * Created by zainababbas on 18/04/2017.
 */

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.HashPartitioner;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.Hdrf;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector2;
import org.apache.flink.graph.streaming.partitioner.object.StoredObject;
import org.apache.flink.graph.streaming.partitioner.object.StoredState;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.HDRFPartitioner;
import org.apache.flink.types.NullValue;

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GSASSSP implements App{

    public boolean fileOutput = false;

    public Long srcVertexId = 1l;

    public String edgesInputPath = null;
    public String partitionPath = "";

    public String outputPath = null;

    public String logPath = null;

    public int maxIterations = 5;

    public int k = 4;
    public String pStrategy = "hash";

    public ExecutionEnvironment exec(ExecutionEnvironment env, String[] args) throws Exception {

        if (!parseParameters(args)) {
            return null;
        }
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env.setParallelism(k);
        DataSet<Edge<Long, NullValue>> data = env.readTextFile(edgesInputPath).setParallelism(1).map(new MapFunction<String, Edge<Long, NullValue>>() {

            @Override
            public Edge<Long, NullValue> map(String s) {
                String[] fields = s.split("\\t");
                long src = Long.parseLong(fields[0]);
                long trg = Long.parseLong(fields[1]);
                return new Edge<>(src, trg, NullValue.getInstance());
            }
        }).setParallelism(1);

//        env.setParallelism(1);
        //DataSet<Edge<Long, NullValue>> partitionedData =
        //			data.partitionCustom(new GreedyPartitioner<>(new CustomKeySelector2(0),k), new CustomKeySelector2<>(0));
        Partitioner partitioner;
        if (pStrategy == "hdrf"){
            partitioner = new Hdrf.HDRF<>(new CustomKeySelector(0), k, 1);
//            partitioner = new HDRFPartitioner<>(new CustomKeySelector(0), k, 1);
        }else{
            partitioner = new HashPartitioner<>(new CustomKeySelector(0));
        }
        System.out.println(pStrategy);
        data.partitionCustom(partitioner, new CustomKeySelector<>(0)).setParallelism(1).writeAsCsv(partitionPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);

        Graph<Long, Double, NullValue> graph = Graph.fromDataSet(data.partitionCustom(partitioner, new CustomKeySelector<>(0)).setParallelism(1), new InitVertices(srcVertexId), env);
        //Graph<Long, Double, NullValue> graph = Graph.fromDataSet(data, new InitVertices(srcVertexId), env);

//        env.setParallelism(k);
        // Execute the GSA iteration
        Graph<Long, Double, NullValue> result = graph.runGatherSumApplyIteration(
                new CalculateDistances(), new ChooseMinDistance(), new UpdateDistance(), maxIterations);

        // Extract the vertices as the result
        DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();

        if (fileOutput) {
            singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",");

            // since file sinks are lazy, we trigger the execution explicitly
        } else {
            singleSourceShortestPaths.print();
        }

        return env;
//        JobExecutionResult result1 = env.execute("My Flink Job1");
//
//        try {
//            FileWriter fw = new FileWriter(logPath, true); //the true will append the new data
//            //fw.write("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
//            //fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
//            fw.write("The job1 took " + result1.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
//            fw.write("The job1 took " + result1.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
//            fw.close();
//        } catch (IOException ioe) {
//            System.err.println("IOException: " + ioe.getMessage());
//        }


    }

    // --------------------------------------------------------------------------------------------
    //  Single Source Shortest Path UDFs
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("serial")
    private static final class InitVertices implements MapFunction<Long, Double> {

        private long srcId;

        public InitVertices(long srcId) {
            this.srcId = srcId;
        }

        public Double map(Long id) {
            if (id.equals(srcId)) {
                return 0.0;
            } else {
                return Double.POSITIVE_INFINITY;
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class CalculateDistances extends GatherFunction<Double, NullValue, Double> {

        public Double gather(Neighbor<Double, NullValue> neighbor) {
            return neighbor.getNeighborValue() + 1;
        }

    }

    @SuppressWarnings("serial")
    private static final class ChooseMinDistance extends SumFunction<Double, NullValue, Double> {

        public Double sum(Double newValue, Double currentValue) {
            return Math.min(newValue, currentValue);
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateDistance extends ApplyFunction<Long, Double, Double> {

        public void apply(Double newDistance, Double oldDistance) {
            if (newDistance < oldDistance) {
                setResult(newDistance);
            }
        }

    }

    public boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 8) {
                System.err.println("Usage: GSASSSP <source vertex id>" +
                        " <input edges path> <partition output path> <output path> <log>  <num iterations> <no. of partitions> <partitioning algorithm>");
                return false;
            }

            fileOutput = true;
            srcVertexId = Long.parseLong(args[0]);
            edgesInputPath = args[1];
            partitionPath = args[2];
            outputPath = args[3];
            logPath = args[4];
            maxIterations = Integer.parseInt(args[5]);
            k = Integer.parseInt(args[6]);
            pStrategy = args[7];
        } else {
            System.out.println("Executing GSASingle Source Shortest Paths example "
                    + "with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("Usage: GSASSSPHash <source vertex id>" +
                    " <input edges path> <output path> <log path><num iterations> <no. of partitions>");
        }
        return true;
    }


}

