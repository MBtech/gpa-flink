package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class Runner {
    public static void main(String[] args) throws Exception {
        String[] remainingArgs = Arrays.copyOfRange(args, 1, args.length);
        String algo = args[0];
        System.out.println(algo);
        App app = new GSASSSP();
        if (algo.equals("GSASSSP")){
            app = new GSASSSP();
        }else if (algo.equals("CC")){
            app = new ConnectedComponents();
        }else if (algo.equals("PageRank")){
            app = new PR();
        }else if (algo.equals("CD")){
            app = new CD();
        }else if (algo.equals("HandA")){
            app = new HandA();
        }else{
            System.err.println("Wrong algorithm Selection "+
                    "Please select: GSASSSP, CC, PageRank, CD or HandA");
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env = app.exec(env, remainingArgs);
        JobExecutionResult result1 = env.execute("My Flink Job1");

        try {
            FileWriter fw = new FileWriter(app.logPath, true); //the true will append the new data
            fw.write("The job1 took " + result1.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
            fw.write("The job1 took " + result1.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
            fw.close();
        } catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getMessage());
        }
    }
}
