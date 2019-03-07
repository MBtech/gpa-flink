package org.apache.flink.graph.streaming.partitioner.edgepartitioners;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;
import org.apache.flink.graph.streaming.partitioner.object.StoredObject;
import org.apache.flink.graph.streaming.partitioner.object.StoredState;
import org.apache.flink.types.NullValue;

import java.util.LinkedList;
import java.util.Random;

///////code for partitioner/////////
public class HDRFPartitioner<T> implements Partitioner<T> {
    private static final long serialVersionUID = 1L;
    CustomKeySelector keySelector;
    private int epsilon = 1;
    private double lamda;
    private StoredState currentState;
    private int k = 0;

    public HDRFPartitioner(CustomKeySelector keySelector, int k , double lamda) {
        this.keySelector = keySelector;
        this.currentState = new StoredState(k);
        this.lamda = lamda;
        this.k=k;
    }

    @Override
    public int partition(Object key,  int numPartitions) {

        long target = 0L;
        try {
            target = (long) keySelector.getValue(key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        long source = (long) key;

        int machine_id = -1;

        StoredObject first_vertex = currentState.getRecord(source);
        StoredObject second_vertex = currentState.getRecord(target);

        int min_load = currentState.getMinLoad();
        int max_load = currentState.getMaxLoad();

        LinkedList<Integer> candidates = new LinkedList<Integer>();
        double MAX_SCORE = 0;

        for (int m = 0; m < k; m++) {

            int degree_u = first_vertex.getDegree() + 1;
            int degree_v = second_vertex.getDegree() + 1;
            int SUM = degree_u + degree_v;
            double fu = 0;
            double fv = 0;
            if (first_vertex.hasReplicaInPartition(m)) {
                fu = degree_u;
                fu /= SUM;
                fu = 1 + (1 - fu);
            }
            if (second_vertex.hasReplicaInPartition(m)) {
                fv = degree_v;
                fv /= SUM;
                fv = 1 + (1 - fv);
            }
            int load = currentState.getMachineLoad(m);
            double bal = (max_load - load);
            bal /= (epsilon + max_load - min_load);
            if (bal < 0) {
                bal = 0;
            }
            double SCORE_m = fu + fv + lamda * bal;
//					System.out.printf("factors fu=%.2f, fv=%.2f, bal=%.2f\n", fu, fv, bal);
//					System.out.printf("For partition %d Score is: %.2f\n", m, SCORE_m);
            if (SCORE_m < 0) {
                System.out.println("ERRORE: SCORE_m<0");
                System.out.println("fu: " + fu);
                System.out.println("fv: " + fv);
                System.out.println("GLOBALS.LAMBDA: " + lamda);
                System.out.println("bal: " + bal);
                System.exit(-1);
            }
            if (SCORE_m > MAX_SCORE) {
                MAX_SCORE = SCORE_m;
                candidates.clear();
                candidates.add(m);
            } else if (SCORE_m == MAX_SCORE) {
                candidates.add(m);
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


//				System.out.printf("Selected Partition %d\n", machine_id);

        if (currentState.getClass() == StoredState.class) {
            StoredState cord_state = (StoredState) currentState;
            //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
            if (!first_vertex.hasReplicaInPartition(machine_id)) {
                first_vertex.addPartition(machine_id);
                cord_state.incrementMachineLoadVertices(machine_id);
            }
            if (!second_vertex.hasReplicaInPartition(machine_id)) {
                second_vertex.addPartition(machine_id);
                cord_state.incrementMachineLoadVertices(machine_id);
            }
        } else {
            //1-UPDATE RECORDS
            if (!first_vertex.hasReplicaInPartition(machine_id)) {
                first_vertex.addPartition(machine_id);
            }
            if (!second_vertex.hasReplicaInPartition(machine_id)) {
                second_vertex.addPartition(machine_id);
            }
        }

        Edge e = new Edge<>(source, target, NullValue.getInstance());
        //2-UPDATE EDGES
        currentState.incrementMachineLoad(machine_id, e);

        //3-UPDATE DEGREES
        first_vertex.incrementDegree();
        second_vertex.incrementDegree();
        //System.out.print("source" + source);
        //System.out.print(target);
        //System.out.println(machine_id);
				/*System.out.print("source"+source);
				System.out.println("target"+target);
				System.out.println("machineid"+machine_id);*/

        return machine_id;

    }
}
