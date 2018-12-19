package com.putti.dataflow3;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface MyStreamOptions extends StreamingOptions {
	@Description("PubSub Topic")
	@Default.String("projects/putti-project2/topics/my-topic")
	public ValueProvider<String> getPubSubTopic();	
	public void setPubSubTopic(ValueProvider<String> pubSubTopic);
	
	@Description("The Google Cloud project ID for the Cloud Bigtable instance.")
	@Default.String("putti-project2")
	public ValueProvider<String> getBigtableProjectId();
	public void setBigtableProjectId(ValueProvider<String> bigtableProjectId);
	
	@Description("The Google Cloud Bigtable instance ID .")
	@Default.String("putti-bigtable1")
	public ValueProvider<String> getBigtableInstanceId();
	public void setBigtableInstanceId(ValueProvider<String> bigtableInstanceId);
	
	@Description("The Cloud Bigtable table ID in the instance." )
	@Default.String("my-table2")
	public ValueProvider<String> getBigtableTableId();
	public void setBigtableTableId(ValueProvider<String> bigtableTableId);	
}
