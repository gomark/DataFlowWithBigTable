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
package com.putti.dataflow3;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful.Fn.Context;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StartBundle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.shaded.org.mortbay.log.Log;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.log4j.BasicConfigurator;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.transforms.Contextful.Fn.Context;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipeline {
  private static final Logger log = Logger.getLogger(StarterPipeline.class.getName());
  
  private static final byte[] FAMILY = Bytes.toBytes("cf1");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");

  public static void main(String[] args) {
	  
	  //BasicConfigurator.configure();	  	  
	  PipelineOptionsFactory.register(MyStreamOptions.class);
	  MyStreamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyStreamOptions.class);
	  
	  	  
	  //Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
	  Pipeline p = Pipeline.create(options);
	  
	  String PROJECT_ID = options.getBigtableProjectId().get();
	  String INSTANCE_ID = options.getBigtableInstanceId().get();
	  String TABLE_ID = options.getBigtableTableId().get();
	  
	  log.info("PROJECT_ID=" + PROJECT_ID);
	  log.info("INSTANCE_ID=" + INSTANCE_ID);
	  log.info("TABLE_ID=" + TABLE_ID);
	  
	  CloudBigtableTableConfiguration config = new CloudBigtableTableConfiguration.Builder()
			  .withProjectId(PROJECT_ID)
			  .withInstanceId(INSTANCE_ID)
			  .withTableId(TABLE_ID)
			  .build();	  
	  
	  
	  Connection conn = BigtableConfiguration.connect(PROJECT_ID, INSTANCE_ID);
	  	  	  
	  PCollection<PubsubMessage> streamData = p.apply("Read From PubSub", PubsubIO.readMessages().fromTopic(options.getPubSubTopic()));
	  	streamData.apply("Test1", ParDo.of(new DoFn<PubsubMessage, Mutation>() {
	  		/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			
			@StartBundle
			public void startBundle(DoFn<PubsubMessage, Mutation>.StartBundleContext c) {

				
			}
			
			private void testRest() {
				String urlStr = "https://www.google.com";
				try {
					URL url = new URL(urlStr);
					HttpURLConnection connection = (HttpURLConnection) url.openConnection();
					connection.setDoOutput(true);
					connection.setRequestMethod("GET");
					connection.addRequestProperty("Content-Type", "application/json");
					
					if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {

						log.info("HTTP 200 OK");
						InputStream s = connection.getInputStream();
						InputStreamReader r = new InputStreamReader(s, StandardCharsets.UTF_8);
						
					} else {
						log.info("HTTP Response: " + String.valueOf(connection.getResponseCode()));
					}
				} catch (Exception e) {
					System.out.println(e.toString());
				}
			}

			@ProcessElement
	  		@Description("Test1 - Description")
	  		public void processElement(ProcessContext c) throws Exception {
				
	  			PubsubMessage m = (PubsubMessage) c.element();
	  			log.info("Test1:" + c.element());
	  			String payload = new String(m.getPayload(), "UTF-8");
	  			log.info("detail:" + payload);
	  			
	  			JsonParser jsonParser = new JsonParser();
			
	  			try {
	  				JsonElement jsonTree = jsonParser.parse(payload);
	  				String id = jsonTree.getAsJsonObject().get("id").getAsString();
					log.info("id=" + id);
					
					this.testRest();
										
					/*
		        	String rowKey = id;
		        	Table table = conn.getTable(TableName.valueOf("my-table2"));
		            Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
		            if (getResult.isEmpty() == false) {
		            	String data = Bytes.toString(getResult.getValue(FAMILY, QUALIFIER));
		            	log.info("lookup-data=" + data);
		            }
	  				*/
					
	  				Put put = new Put(Bytes.toBytes(id));
	  				put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(payload));
	  				c.output(put);
	  					  					  				
	  				log.info("record -- done");
	  			} catch (Exception e) {
	  				log.severe(e.toString());
	  				e.printStackTrace();
	  			}
	  		}
	  	})).apply(CloudBigtableIO.writeToTable(config));
	  	
	  	
	  	
	
	  	p.run();
	  	log.info("DONE");
		

  }
}
