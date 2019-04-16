package tutorial.util;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.HeronTopology;

import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;


public class HelperRunner {

    private static final String MODE_OF_OPERATION_CLUSTER = "Cluster";
    private static final String MODE_OF_OPERATION_LOCAL = "Local";
    private static final int NUMBER_OF_WORKERS = 3;  //default value
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;

    private static final long MILLIS_IN_SEC = 1000;

    private HelperRunner() {}

    public static void runTopology(String[] args,
                                   HeronTopology topology,
                                   Config conf) throws Exception {

        if (args != null) {
            if (args.length < 2) {
                throw new IllegalArgumentException("Illegal number of command line arguments supplied." +
                        "\nPlease provide the topologyName as the first argument and either " +
                        "'Cluster' or 'Local' as the second argument.");
            }

            if (!args[1].equals(MODE_OF_OPERATION_CLUSTER) && !args[1].equals(MODE_OF_OPERATION_LOCAL)) {
                throw new IllegalArgumentException("The allowed values for the second argument is either" +
                        " 'Cluster' or 'Local'. Please provide a valid value for the second argument.");

            }

            String topologyName = args[0];

            if (args[1].equals(MODE_OF_OPERATION_CLUSTER)) {
                HelperRunner.runTopologyRemotely(topology, topologyName, conf);
            } else {
                conf.setComponentParallelism(NUMBER_OF_WORKERS);
                HelperRunner.runTopologyLocally(topology, topologyName, conf, DEFAULT_RUNTIME_IN_SECONDS);
            }

        }
        else {
            throw new IllegalArgumentException("There are no arguments provided. Please resubmit the job with command line args");
        }

    }

    private static void runTopologyRemotely(HeronTopology topology, String topologyName, Config conf)
            throws com.twitter.heron.api.exception.AlreadyAliveException, com.twitter.heron.api.exception.InvalidTopologyException {
        HeronSubmitter.submitTopology(topologyName,conf,topology);
    }

    private static void runTopologyLocally(HeronTopology topology,
                                           String topologyName,
                                           Config conf,
                                           int runtimeInSeconds)
            throws InterruptedException, NotAliveException, AlreadyAliveException, InvalidTopologyException {

        LocalCluster cluster = new LocalCluster();
        StormTopology stormTopology = new StormTopology(topology);
        cluster.submitTopology(topologyName, conf, stormTopology);
        Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }
}
