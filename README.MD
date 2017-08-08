---
title: Heron example Java topology
description: Learn how to create Heron topologies in Java.
services: hdinsight
documentationcenter: ''
author: Ali Ahmed
tags: streamlio
keywords: heron,Heron example,heron java,heron topology example

---
# Create an Heron topology in Java

Here we create a Heron topology that implements a word-count application. We will use Maven to build and package the project.

## Prerequisites

* [Java Developer Kit (JDK) version 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

* [Maven (https://maven.apache.org/download.cgi)](https://maven.apache.org/download.cgi): Maven is a project build system for Java projects.

* A text editor or IDE.


## Clone the project

From the command line, use the following git command to clone this project

```bash
git clone https://github.com/streamlio/heron-java-tutorial.git
```

This command creates a directory named `heron-java-tutorial` at the current location, which contains a basic heron maven project.
The `heron-java-tutorial` directory contains the following items:

* `pom.xml`: Contains settings for the Maven project.
* `src\main\java\tutorial`: Contains your application code.
* `src\main\resources`: Contains the log4j configuration.


## Understating the maven pom.xml


### properties

Maven allows you to define project-level values called properties. In the __pom.xml__, the following text after the `</repositories>` add global properties:

```xml
<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <heron.version>0.14.6</heron.version>
</properties>
```

These values are used in different sections of the `pom.xml`. For example, when specifying the version of Heron components, we can use `${heron.version}` instead of hard coding a value.

### dependencies

Below are the dependencies for Heron components in the `pom.xml` 

```xml
<dependency>
    <groupId>com.twitter.heron</groupId>
    <artifactId>heron-api</artifactId>
    <version>${heron.version}</version>
</dependency>

<dependency>
    <groupId>com.twitter.heron</groupId>
    <artifactId>heron-storm</artifactId>
    <version>${heron.version}</version>
</dependency>
```


### Build configuration

Maven plug-ins allow you to customize the build stages of the project. For example, how the project is compiled or how to package it into a JAR file. General pom files follow this convention

```xml
<build>
    <plugins>
    </plugins>
    <resources>
    </resources>
</build>
```

This section is used to add plug-ins, resources, and other build configuration options. For a full reference of the **pom.xml** file, see [http://maven.apache.org/pom.html](http://maven.apache.org/pom.html).

### plug-ins

For Heron topologies implemented in Java, the [Exec Maven Plugin](http://www.mojohaus.org/exec-maven-plugin/) is useful because it allows you to easily run the topology locally in your development environment. The Exec Maven plugin is included in the`<plugins>` section of the `pom.xml` as such

```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <version>1.4.0</version>
    <executions>
    <execution>
    <goals>
        <goal>exec</goal>
    </goals>
    </execution>
    </executions>
    <configuration>
    <executable>java</executable>
    <includeProjectDependencies>true</includeProjectDependencies>
    <includePluginDependencies>false</includePluginDependencies>
    <classpathScope>compile</classpathScope>
    <mainClass>${heron.topology}</mainClass>
    <cleanupDaemonThreads>false</cleanupDaemonThreads> 
    </configuration>
</plugin>
```

Another useful plug-in is the [Apache Maven Compiler Plugin](http://maven.apache.org/plugins/maven-compiler-plugin/), which is used to change compilation options. The changes the Java version that Maven uses for the source and target for your application.

* For Heron we set the source and target Java version to __1.8__.

The compiler configuration is in the `<plugins>` section of the `pom.xml` as such:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>3.6.2</version>
    <configuration>
    <source>1.8</source>
    <target>1.8</target>
    </configuration>
</plugin>
```

## Create the topology

A Java-based Apache Storm topology consists of three components that you must author (or reference) as a dependency.

* **Spouts**: Reads data from external sources and emits streams of data into the topology.

* **Bolts**: Performs processing on streams emitted by spouts or other bolts, and emits one or more streams.

* **Topology**: Defines how the spouts and bolts are arranged, and provides the entry point for the topology.

### Create the spout

The Spount is implemented `RandomSentenceSpout.java` in the `src\main\java\tutorial` directory.

```java
package tutorial;

import ...

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
        //Sleep for a bit
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
```

> [!NOTE]
> Although this topology uses only one spout, others may have several that feed data from different sources into the topology.

### Create the bolts

Bolts handle the data processing. This topology uses two bolts:

* **SplitSentenceBolt**: Splits the sentences emitted by **RandomSentenceSpout** into individual words.

* **WordCountBolt**: Counts how many times each word has occurred.

> [!NOTE]
> Bolts can do anything, for example, computation, persistence, or talking to external components.

The Bolts are implmented in `SplitSentenceBolt.java` and `WordCountBolt.java` in the `src\main\java\tutorial` directory.

#### SplitSentenceBolt

```java
package tutorial;

import ...

/**
 * This Bolt splits a sentence into words
 */
public class SplitSentenceBolt extends BaseBasicBolt {

    //Execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //Get the sentence content from the tuple
        String sentence = tuple.getString(0);
        //An iterator to get each word
        BreakIterator boundary = BreakIterator.getWordInstance();
        //Give the iterator the sentence
        boundary.setText(sentence);
        //Find the beginning first word
        int start = boundary.first();
        //Iterate over each word and emit it to the output stream
        for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
            //get the word
            String word = sentence.substring(start, end);
            //If a word is whitespace characters, replace it with empty
            word = word.replaceAll("\\s+", "");
            //if it's an actual word, emit it
            if (!word.equals("")) {
                collector.emit(new Values(word));
            }
        }
    }

    //Declare that emitted tuples contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
```

#### WordCountBolt

```java
package tutorial;

import ...

/**
 * This Bolt emits word count pairs in periodic intervals
 */
public class WordCountBolt extends BaseBasicBolt {
    //Create logger for this class
    private static final Logger logger = LogManager.getLogger(WordCountBolt.class);

    private static final int EMIT_FREQUENCY = 3;
    //For holding words and counts
    private Map<String, Integer> counts = new HashMap<>();
    //How often to emit a count of words
    private Integer emitFrequency;

    public WordCountBolt(){
        emitFrequency = EMIT_FREQUENCY;
    }

    public WordCountBolt(int emitFrequency){
        this.emitFrequency = emitFrequency;
    }

    //Configure frequency of tick tuples for this bolt
    //This delivers a 'tick' tuple on a specific interval,
    //which is used to trigger certain actions
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    //execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //If it's a tick tuple, emit all words and counts
        if(TupleHelpers.isTickTuple(tuple)) {
            for(String word : counts.keySet()) {
                Integer count = counts.get(word);
                collector.emit(new Values(word, count));
                logger.info(String.format("Emitting a count of (%d) for word (%s)", count, word));
            }
        } else {
            //Get the word contents from the tuple
            String word = tuple.getString(0);
            //Have we counted any already?
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            //Increment the count and store it
            count++;
            counts.put(word, count);
        }
    }


    //Declare that this emits a tuple containing two fields; word and count
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
```

### Define the topology

The topology ties the spouts and bolts together into a graph, which defines how data flows between the components. It also provides parallelism hints that Heron uses when creating instances of the components within the cluster.

The following image is a basic diagram of the graph of components for this topology.

```
+----------------+          +-------------------+             +-------------+      ("A",2)
|                |          |                   |             |             |
| RandomSentence |          |                   |             |             |      ("B",1)
|     Spout      +---------->  SenteceSplitter  +------------>+  WordCount  +---->
|                |          |       Bolt        |             |    Bolt     |      ("C",1)
|                |          |                   |             |             |
+----------------+          +-------------------+   ("A")     +-------------+      ("D",2)
                                                    ("D")
         ("A D B C A D")                            ("B")
                                                    ("C")
                                                    ("A")
                                                    ("D")

```


The topology is implemented in the a file named `WordCountTopology.java` in the `src\main\java\tutorial` directory. Below is the code.

```java
package tutorial;

import ...

/**
 * This is driver as well the topology graph generator
 */
public class WordCountTopology {

    private WordCountTopology() { }

    //Entry point for the topology
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //Add the spout, with a name of 'sentence'
        //and parallelism hint of 3 executors
        builder.setSpout("sentence", new RandomSentenceSpout(),3);

        //Add the SplitSentence bolt, with a name of 'split'
        //and parallelism hint of 8 executors
        //shufflegrouping subscribes to the spout, and equally distributes
        //tuples (sentences) across instances of the SplitSentence bolt
        builder.setBolt("split", new SplitSentenceBolt(),8).shuffleGrouping("sentence");

        //Add the counter, with a name of 'count'
        //and parallelism hint of 12 executors
        //fieldsGrouping subscribes to the split bolt, and
        //ensures that the same word is sent to the same instance (group by field 'word')
        builder.setBolt("count", new WordCountBolt(),12).fieldsGrouping("split", new Fields("word"));
        Config conf = new Config();

        // Resource Configs
        com.twitter.heron.api.Config.setComponentRam(conf, "sentence", ByteAmount.fromGigabytes(1));
        com.twitter.heron.api.Config.setComponentRam(conf, "split", ByteAmount.fromGigabytes(1));
        com.twitter.heron.api.Config.setComponentRam(conf, "count", ByteAmount.fromGigabytes(1));
        com.twitter.heron.api.Config.setContainerCpuRequested(conf, 3);

        //submit the topology
        HelperRunner.runTopology(args, builder.createTopology(), conf);

    }

}
```

### Configure logging

Heron uses java util logging to log information. 
Topologies can psecifiy there own logging framework , in this example we are using log4j

```xml
    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-api</artifactId>
      <version>2.6.2</version>
    </dependency>

    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-core</artifactId>
      <version>2.6.2</version>
    </dependency>
```    

## Test the topology locally

We can use the following command to test the topology locally.

```bash
mvn compile exec:java -Dheron.topology=tutorial.WordCountTopology -Dexec.args="WordCount Local"
```

As it runs, the topology displays startup information. The following text is an example of the word count output:

	11:56:48 [count_count_2] INFO  tutorial.WordCountBolt - Emitting a count of (152) for word (keeps)
	11:56:48 [count_count_1] INFO  tutorial.WordCountBolt - Emitting a count of (161) for word (two)
	11:56:48 [count_count_2] INFO  tutorial.WordCountBolt - Emitting a count of (119) for word (years)
	11:56:48 [count_count_3] INFO  tutorial.WordCountBolt - Emitting a count of (525) for word (the)
	11:56:48 [count_count_3] INFO  tutorial.WordCountBolt - Emitting a count of (119) for word (score)
	11:56:48 [count_count_3] INFO  tutorial.WordCountBolt - Emitting a count of (152) for word (away)
	11:56:48 [count_count_3] INFO  tutorial.WordCountBolt - Emitting a count of (125) for word (snow)
	11:56:48 [count_count_3] INFO  tutorial.WordCountBolt - Emitting a count of (152) for word (day)
	11:56:48 [count_count_3] INFO  tutorial.WordCountBolt - Emitting a count of (125) for word (dwarfs)

This example log indicates that the word 'keeps' has been emitted 152 times. The count continues to go up as long as the topology runs because the spout continuously emits the same sentences.

There is a 3-second interval between emission of words and counts. The **WordCount** component is configured to only emit information when a tick tuple arrives. It requests that tick tuples are only delivered every five seconds.

## Test on the cluster

TODO