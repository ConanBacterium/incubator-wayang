/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.wordcount;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.time.Duration;
import java.time.Instant;

public class Main {

    private static long runExperiment1(String filePath, boolean isParquet, boolean isWordCount, WayangContext wayangContext)
    {
        /*
         * Java always allocates objects on heap, so putting this out of loop and into function shouldn't help with memory leak.
         */
        Instant start = Instant.now();

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount")
                .withUdfJarOf(Main.class);  

        Collection<Tuple2<String, Integer>> wordCounts;  

        if(isParquet)
        {
            if(isWordCount)
            {
                wordCounts = planBuilder 
                    // .readTextFile(args[1]).withName("Load file")
                    .readParquetFile(filePath).withName("Load file")
                    .flatMap(line -> Arrays.asList(line.split("\\W+")))
                    .withSelectivity(1, 100, 0.9)
                    .withName("Split words")
                    .filter(token -> !token.isEmpty())
                    .withName("Filter empty words")
                    .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")
                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                    )
                    .withName("Add counters")
                    .collect();
                // wordcountsParquet.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
            } 
            else 
            {
                planBuilder 
                    // .readTextFile(args[1]).withName("Load file")
                    .readParquetFile(filePath).withName("Load file")
                    .collect(); 
            }
        }
        else
        {
            if(isWordCount)
            {
                wordCounts = planBuilder 
                    .readTextFile(filePath).withName("Load file")
                    .flatMap(line -> Arrays.asList(line.split("\n")))
                    .flatMap(line -> Arrays.asList(line.split(",")))
                    .flatMap(val -> Arrays.asList(val.split("\\W+")))
                    .withSelectivity(1, 100, 0.9)
                    .withName("Split words")
                    .filter(token -> !token.isEmpty())
                    .withName("Filter empty words")
                    .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")
                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                    )
                    .withName("Add counters")
                    .collect();
                    // wordcountsParquet.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
            } 
            else 
            {
                planBuilder 
                    .readTextFile(filePath).withName("Load file")
                    .flatMap(line -> Arrays.asList(line.split("\n")))
                    .map(line -> line.split(","))
                    .collect(); 
            }

        }

        Instant end = Instant.now();
        long timeElapsed = Duration.between(start, end).toMillis();

        planBuilder = null; 
        wordCounts = null; 

        return timeElapsed; 
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        try {
            if (args.length == 0) {
                System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
                System.exit(1);
            }

            WayangContext wayangContext = new WayangContext();
            for (String platform : args[0].split(",")) {
                switch (platform) {
                    case "java":
                        wayangContext.register(Java.basicPlugin());
                        break;
                    case "spark":
                        wayangContext.register(Spark.basicPlugin());
                        break;
                    default:
                        System.err.format("Unknown platform: \"%s\"\n", platform);
                        System.exit(3);
                        return;
                }
            }

            int nReps = Integer.parseInt(args[2]); 
            boolean isWordCount = Boolean.parseBoolean(args[3]); 
            boolean isParquet = Boolean.parseBoolean(args[4]); 

            if(isWordCount){
                System.out.println("wordcount");
            } else {
                System.out.println("file read only");
            } 
            if(isParquet) {
                System.out.println("Parquet"); 
            } else {
                System.out.println("CSV"); 
            }
            

            List<Long> executionTimes = new ArrayList<>();

            // Do it once outside of repetition test... This way stuff should be hot in cache or smth 
            JavaPlanBuilder planBuilderColdRun = new JavaPlanBuilder(wayangContext)
                    .withJobName("WordCount")
                    .withUdfJarOf(Main.class);

            Collection<Tuple2<String, Integer>> wordcountsColdRun = planBuilderColdRun
                    // .readTextFile(args[1]).withName("Load file")
                    .readParquetFile(args[1]).withName("Load file")
                    .flatMap(line -> Arrays.asList(line.split("\\W+")))
                    .withSelectivity(1, 100, 0.9)
                    .withName("Split words")
                    .filter(token -> !token.isEmpty())
                    .withName("Filter empty words")
                    .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")
                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                    )
                    .withName("Add counters")
                    .collect();

            planBuilderColdRun = null; 
            wordcountsColdRun = null; 

            for(int i = 0; i < nReps; i++)
            {
            /* 
             *  THIS HAS MEMORY LEAK SOMEHOW !!!
             * 
                Instant start = Instant.now();

                JavaPlanBuilder planBuilderParquet = new JavaPlanBuilder(wayangContext)
                        .withJobName("WordCount")
                        .withUdfJarOf(Main.class);  

                Collection<Tuple2<String, Integer>> wordcountsParquet = planBuilderParquet
                        // .readTextFile(args[1]).withName("Load file")
                        .readParquetFile(args[1]).withName("Load file")
                        .flatMap(line -> Arrays.asList(line.split("\\W+")))
                        .withSelectivity(1, 100, 0.9)
                        .withName("Split words")
                        .filter(token -> !token.isEmpty())
                        .withName("Filter empty words")
                        .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")
                        .reduceByKey(
                                Tuple2::getField0,
                                (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                        )
                        .withName("Add counters")
                        .collect();
                // wordcountsParquet.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));

                Instant end = Instant.now();
                long timeElapsed = Duration.between(start, end).toMillis();
                executionTimes.add(timeElapsed);
                System.out.printf("Run %d: %d ms%n", i + 1, timeElapsed);
                planBuilderParquet = null; 
                wordcountsParquet = null; 
                System.gc();
                Thread.sleep(1000);
             */

                long time = runExperiment1(args[1], isParquet, isWordCount, wayangContext);
                System.out.println(time); 
                executionTimes.add(time); 
                System.gc();
                Thread.sleep(1000);
            }

            // Calculate statistics
            double mean = executionTimes.stream()
                    .mapToLong(Long::valueOf)
                    .average()
                    .orElse(0.0);

            double variance = executionTimes.stream()
                    .mapToDouble(time -> Math.pow(time - mean, 2))
                    .average()
                    .orElse(0.0);

            double stdDev = Math.sqrt(variance);
            double coefficientOfVariation= (stdDev / mean) * 100;

            // Print summary statistics
            System.out.println("\nBenchmark Results:");
            System.out.printf("Mean execution time: %.2f ms%n", mean);
            System.out.printf("Standard deviation: %.2f ms%n", stdDev);
            System.out.printf("Coefficient of variation: %.2f%%%n", coefficientOfVariation);


                // Collection<Tuple2<String, Integer>> wordcountsCsv = planBuilder
                //         .readTextFile(args[1]).withName("Load file")
                //         .flatMap(line -> Arrays.asList(line.split("\\W+")))
                //         .withSelectivity(1, 100, 0.9)
                //         .withName("Split words")
                //         .filter(token -> !token.isEmpty())
                //         .withName("Filter empty words")
                //         .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")
                //         .reduceByKey(
                //                 Tuple2::getField0,
                //                 (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                //         )
                //         .withName("Add counters")
                //         .collect();
                // // wordcountsParquet.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));

            // System.out.printf("Found %d words:\n", wordcounts.size());
            // wordcounts.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
                    /* Read the text file */
                    // .readParquetFile(fileUrl).withName("Load file")
                    // .flatMap(line -> Arrays.asList(line.split("\\s+")))
                    // .withSelectivity(1, 100, 0.9)
                    // .withName("Split words")

                    // /* Filter empty tokens */
                    // .filter(token -> !token.isEmpty())
                    // .withName("Filter empty words")

                    // /* Attach counter to each word */
                    // .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

                    // // // Sum up counters for every word.
                    // .reduceByKey(
                    //         Tuple2::getField0,
                    //         (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                    // )
                    // .withName("Add counters")

                    // /* Execute the plan and collect the results */
                    // .collect();



            // Collection<Tuple2<String, Integer>> wordcounts = planBuilder
                    // .flatMap((String line) -> Arrays.asList(line.split("\\s+"))) 
                    // .map(String::toLowerCase)                          
                    // .collect(Collectors.groupingBy(
                    //     word -> word,                                  
                    //     Collectors.counting()                          
                    // ));


                    // wordcounts.forEach((word, count) -> 
                    // System.out.println(word + ": " + count));


            //         // .readTextFile(args[1]).withName("Load file")

            //         /* Split each line by non-word characters */
            //         .flatMap(line -> Arrays.asList(line.split("\\W+")))
            //         .withSelectivity(1, 100, 0.9)
            //         .withName("Split words")

            //         /* Filter empty tokens */
            //         .filter(token -> !token.isEmpty())
            //         .withName("Filter empty words")

            //         /* Attach counter to each word */
            //         .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

            //         // Sum up counters for every word.
            //         .reduceByKey(
            //                 Tuple2::getField0,
            //                 (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
            //         )
            //         .withName("Add counters")

            //         /* Execute the plan and collect the results */
            //         .collect();


            // System.out.printf("Found %d words:\n", wordcounts.size());
            // wordcounts.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }
}

