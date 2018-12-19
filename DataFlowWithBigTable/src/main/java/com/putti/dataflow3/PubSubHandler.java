package com.putti.dataflow3;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.transforms.Contextful.Fn.Context;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.hadoop.hbase.client.Mutation;

public class PubSubHandler extends DoFn<PubsubMessage, Mutation> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7063609193891850158L;
	
	@StartBundle
	public void startBundle(Context c) {
		
	}

	@ProcessElement
	@Description("Test1 - Description")
	public void processElement(ProcessContext c) throws Exception {
		
	}
	
}
