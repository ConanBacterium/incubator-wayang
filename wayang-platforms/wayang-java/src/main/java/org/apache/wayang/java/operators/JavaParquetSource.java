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

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// JARONOTE: 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;

import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Iterator;

/**
 * This is execution operator implements the {@link ParquetSource}.
 */
public class JavaParquetSource extends ParquetSource implements JavaExecutionOperator 
{

    // public static Stream<ArrayList<String>> getParquetStream(String filePath) throws Exception {
    public static Stream<String> getParquetStream(String filePath) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(filePath);
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));
        
        Iterator<String> iterator = new Iterator<String>() 
        {
            private PageReadStore currentPages = reader.readNextRowGroup();
            private RecordReader<Group> recordReader = currentPages != null ? 
                new ColumnIOFactory().getColumnIO(reader.getFooter().getFileMetaData().getSchema())
                    .getRecordReader(currentPages, new GroupRecordConverter(reader.getFooter().getFileMetaData().getSchema())) : null;
            private long currentRow = 0;
            private long currentRowCount = currentPages != null ? currentPages.getRowCount() : 0;

            @Override
            public boolean hasNext() {
                if (currentRow >= currentRowCount) {
                    try {
                        currentPages = reader.readNextRowGroup();
                        if (currentPages == null) {
                            reader.close();
                            return false;
                        }
                        recordReader = new ColumnIOFactory().getColumnIO(reader.getFooter().getFileMetaData().getSchema())
                            .getRecordReader(currentPages, new GroupRecordConverter(reader.getFooter().getFileMetaData().getSchema()));
                        currentRow = 0;
                        currentRowCount = currentPages.getRowCount();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return true;
            }

            @Override
            // public ArrayList<String> next() {
            public String next() {
                Group group = recordReader.read();
                currentRow++;
                // ArrayList<String> record = new ArrayList<>();
                // Convert Group to ArrayList - modify this according to your schema
                // record.add(group.toString());
                // return record;
                return group.toString(); 
            }
        };

        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
            true); // TODO: try to make it parallel to see how much faster it gets?? 
    }


    private static final Logger logger = LoggerFactory.getLogger(JavaParquetSource.class);

    public JavaParquetSource(String inputUrl) {
        super(inputUrl);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaParquetSource(ParquetSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        String urlStr = this.getInputUrl().trim(); // JARONOTE: from wayang operator TextFileSource
        try {
            // Stream<ArrayList<String>> records = getParquetStream(urlStr);  

            // // TEST START
            // Map<String, Long> wordCount = getParquetStream(urlStr)
            // .flatMap((String line) -> Arrays.stream(line.split("\\s+"))) 
            // .map(String::toLowerCase)                          
            // .collect(Collectors.groupingBy(
            //     word -> word,                                  
            //     Collectors.counting()                          
            // ));
            // wordCount.forEach((word, count) -> 
            // // TEST END 

            Stream<String> records = getParquetStream(urlStr);  
            ((StreamChannel.Instance) outputs[0]).accept(records); 


        } catch(Exception e)
        {
            throw new WayangException(String.format("Reading from URL: %s failed.", urlStr), e);
        }

        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.parquetsource.load.prepare", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.parquetsource.load.main", javaExecutor.getConfiguration()
        ));

        outputs[0].getLineage().addPredecessor(mainLineageNode); // JARONOTE: set the predecessor node of the next node... 

        return prepareLineageNode.collectAndMark();
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.parquetsource.load.prepare", "wayang.java.textfilesource.load.main");
    }

    @Override
    public JavaParquetSource copy() {
        return new JavaParquetSource(this.getInputUrl());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
