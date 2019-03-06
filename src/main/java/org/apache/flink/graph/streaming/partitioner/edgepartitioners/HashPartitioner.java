package org.apache.flink.graph.streaming.partitioner.edgepartitioners;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector2;
import org.apache.flink.util.MathUtils;

public class HashPartitioner<T> implements Partitioner<T> {
    private static final long serialVersionUID = 1L;
    CustomKeySelector2 keySelector;
    /*	private double seed;
        private int shrink;
        private static final int MAX_SHRINK = 100;
        private int k;*/
    public HashPartitioner(CustomKeySelector2 keySelector)
    {
        this.keySelector = keySelector;
        //	this.seed = Math.random();
        //	Random r = new Random();
        //	shrink = r.nextInt(MAX_SHRINK);
        //	this.k=k;
    }

    @Override
    public int partition(Object key, int numPartitions) {
        //long source = (long) key;
        //return Math.abs((int) ( (int) (source)*seed*shrink) % k);
        return MathUtils.murmurHash(key.hashCode()) % numPartitions;


    }

}