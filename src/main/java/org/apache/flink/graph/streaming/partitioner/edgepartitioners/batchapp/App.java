package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

import org.apache.flink.api.java.ExecutionEnvironment;

public interface App {
    public String logPath = "log";
    public abstract ExecutionEnvironment exec(ExecutionEnvironment env, String[] args) throws Exception;
}
