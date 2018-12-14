package tutorial;


import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * This spout randomly emits sentences
 */
public class RandomSentenceSpout extends BaseRichSpout {
    //Collector used to emit output
    SpoutOutputCollector collector;
    //Used to generate a random number
    Random rand;

    //Open is called when an instance of the class is created
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        //Set the instance collector to the one passed in
        this.collector = collector;
        //For randomness
        this.rand = new Random();

    }

    //Emit data to the stream
    @Override
    public void nextTuple() {
        //Sleep for a bit to prevent annoyance in o/p terminal
        Utils.sleep(50);
        //The sentences that are randomly emitted
        String[] sentences = new String[]{
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"
        };
        //Randomly pick a sentence
        String sentence = sentences[rand.nextInt(sentences.length)];
        //Emit the sentence
        collector.emit(new Values(sentence));
    }

    //Declare the output fields. In this case, an sentence
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

}