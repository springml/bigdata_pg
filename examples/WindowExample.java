// cd ~/training-data-analyst/courses/data_analysis/lab2/javahelp
// ./run_oncloud4.sh dataflowtesting-218212 dataflow-results-ini/stream-demo-consumer

/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import org.apache.beam.sdk.transforms.windowing.AfterWatermark;

import org.joda.time.Duration;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * A dataflow pipeline that listens to a PubSub topic and writes out aggregates
 * on windows to BigQuery
 * 
 * @author vlakshmanan
 *
 */
public class WindowExample {

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
			
			String[] elements = word.split(",");
			out.output(elements[0].trim());
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

		String subscriptionId = "projects/dataflowtesting-218212/subscriptions/PubSubTesting";
		PCollection<String> text = p.apply("GetMessages", PubsubIO.readStrings().fromSubscription(subscriptionId));
//		p 
//				.apply("GetMessages", PubsubIO.readStrings().fromTopic(topic)) // read the data from pubsub
	
				text.apply(
						Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
							.triggering(AfterWatermark.pastEndOfWindow())
							.withAllowedLateness(Duration.ZERO)
							.discardingFiredPanes())
				
				.apply(ParDo.of(new FormatText()))
				
				.apply(Count.perElement())
				
				.apply(ParDo.of(new OutputToString()))

				
//				.apply(
//						Window.<String>into(SlidingWindows.of(Duration.standardSeconds(60)).every(Duration.standardSeconds(30)))
//								.triggering(AfterWatermark.pastEndOfWindow())
//								.withAllowedLateness(Duration.ZERO)
//								.discardingFiredPanes())
				
//				 .apply("window",
//				 		Window.into(SlidingWindows // window moves every 2 min
//				 				.of(Duration.standardMinutes(2)) // window is set to 2 min
//				 				.every(Duration.standardSeconds(30)))) // slides every 30 sec

//				 .apply("WordsPerLine", ParDo.of(new DoFn<String, String>() {
//				 	@ProcessElement
//				 	public void processElement(ProcessContext c) throws Exception {
//				 		String line = c.element();
//				 		System.out.println("line");
//				 		// c.output(line.split(" ").length);
//				 		c.output(line);
//				 	}
//				 }))

				.apply("ToBQRow", ParDo.of(new DoFn<String, TableRow>() {
					@ProcessElement
					public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
						TableRow row = new TableRow();
						
						row.set("event_timestamp", c.timestamp().toString());
						row.set("processing_timestamp", Instant.now().toString());
						
						IntervalWindow w = (IntervalWindow) window;
						
						row.set("start_window_timestamp", w.start().toString());
						row.set("end_window_timestamp", w.end().toString());
//						row.set("window_timestamp", window.maxTimestamp().toString());
						

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
		

		p.run();
	}
}
