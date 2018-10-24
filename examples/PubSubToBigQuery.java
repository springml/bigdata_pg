package com.google.cloud.training.dataanalyst.javahelp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.training.dataanalyst.javahelp.WindowExample.FormatText;
import com.google.cloud.training.dataanalyst.javahelp.WindowExample.MyOptions;
import com.google.cloud.training.dataanalyst.javahelp.WindowExample.OutputToString;
//import com.google.pubsub.v1.ProjectSubscriptionName;
//import com.google.cloud.ServiceOptions;
//import com.google.cloud.pubsub.v1.AckReplyConsumer;
//import com.google.cloud.pubsub.v1.MessageReceiver;
//import com.google.cloud.pubsub.v1.Subscriber;
//import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubToBigQuery {

	public static interface MyOptions extends DataflowPipelineOptions {
		
		@Description("Output BigQuery table <project_id>:<dataset_id>.<table_id>")
		@Default.String("cloud-training-demos:demos.streamdemo")
		String getOutput();
		void setOutput(String s);

		@Description("Input topic")
		@Default.String("projects/cloud-training-demos/topics/streamdemo")
		String getInput();
		void setInput(String s);
		
	}

	@SuppressWarnings("serial")
	public static class FormatText extends DoFn<String, String>{
		
		@ProcessElement
		public void procesElement(@Element String word, OutputReceiver<String> out) {
//			String[] elements = word.split(",");
			
			System.out.println("Message from pubsub: " + word);
//			
//			if(word.contains("50")) {
//				throw new NullPointerException("Destroyer");
//			}
			
			out.output(word);
		}
	}
	
	
	@SuppressWarnings("serial")
	public static class OutputToString extends DoFn<KV<String, Long>, String> {
		
		@ProcessElement
		public void processElement(@Element KV<String, Long> word, OutputReceiver<String> out) {
			String temp = word.getKey();
			Long value = word.getValue();
			
			out.output(temp + value.toString());
		}
	}
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

		options.setStreaming(true); // turn or streaming mode

		Pipeline p = Pipeline.create(options);

		String topic = options.getInput(); // pub sub topic
		String output = options.getOutput(); // location of where the output will be stored

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("event_timestamp").setType("STRING"));
		fields.add(new TableFieldSchema().setName("processing_timestamp").setType("STRING"));
		fields.add(new TableFieldSchema().setName("start_window_timestamp").setType("STRING"));
		fields.add(new TableFieldSchema().setName("end_window_timestamp").setType("STRING"));
		// fields.add(new TableFieldSchema().setName("num_words").setType("INT64"));
		fields.add(new TableFieldSchema().setName("message").setType("STRING"));
		
		TableSchema schema = new TableSchema().setFields(fields);

		String projectId = options.getProject();
		String subscriptionId = "projects/dataflowtesting-218212/subscriptions/PubSubTesting";
		
		PCollection<String> text = p.apply("GetMessages", PubsubIO.readStrings().fromSubscription(subscriptionId));
		PCollection<String> moreText = text.apply(ParDo.of(new FormatText()));
		

		moreText.apply("ToBQRow", ParDo.of(new DoFn<String, TableRow>() {
			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
				TableRow row = new TableRow();
				
				row.set("event_timestamp", "PubSubTesting");
				row.set("processing_timestamp", "PubSubTesting");
				
//				IntervalWindow w = (IntervalWindow) window;
				
				row.set("start_window_timestamp", "PubSubTesting");
				row.set("end_window_timestamp", "PubSubTesting");
//				row.set("window_timestamp", window.maxTimestamp().toString());
				

				// row.set("num_words", 1);
				row.set("message", c.element());
				c.output(row);
			}
		})) 
		
		
		.apply(BigQueryIO.writeTableRows().to(output)
				.withSchema(schema)//
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
System.out.println("Write Completed");
//
//
p.run();
	}

}
