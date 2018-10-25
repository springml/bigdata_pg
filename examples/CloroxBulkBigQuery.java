package com.google.cloud.training.dataanalyst.javahelp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class CloroxBulkBigQuery {

	public interface Options extends PipelineOptions{
	
		@Description("The QPS which the benchmark should output to Pub/Sub.")
	    @Required
	    Long getQps();
	    void setQps(Long value);

	    @Description("The path to the schema to generate.")
	    String getSchemaLocation();
	    void setSchemaLocation(String value);
	    
		@Description("Set Dataset")
		String getDataset();
		void setDataset(String value);
		
	}

	@SuppressWarnings("serial")
	static class FormatTextFn extends DoFn<Long, String> {
		
		@ProcessElement
		public void processElement(@Element Long word, OutputReceiver<String> out) {
			
			String result = Long.toString(word) + " " + Instant.now().toString();
			
			out.output(result);
		}
	}
	
	@SuppressWarnings("serial")
	static class FormatBigQueryFn extends DoFn<String, TableRow> {
		
		@ProcessElement
		public void processElement(@Element String word, OutputReceiver<TableRow> out) {
			
			TableRow row = new TableRow();
			
			row.set("message", word);
			
			out.output(row);
		}
	}
	
	public static void main(String[] args) {
		
		Options options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(Options.class);
		
		Pipeline pipeline = Pipeline.create(options);
		
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("message").setType("STRING"));
		TableSchema schema = new TableSchema().setFields(fields);
		
		PCollection<Long> numbers = pipeline.apply("Trigger",
				GenerateSequence.from(0L).withRate(options.getQps(), Duration.standardSeconds(1L)));
				
//				GenerateSequence
//					.from(0L).to(1000));//withRate(options.getQps(), Duration.standardSeconds(1L)));
//					.withMaxReadTime(Duration.standardSeconds(1L)));
		
		PCollection<String> formatted = numbers.apply("Format", ParDo.of(new FormatTextFn()));
		
        PCollection<TableRow>rows = formatted.apply("FormatToBigQuery", ParDo.of(new FormatBigQueryFn()));

        rows.apply("WriteToBigQuery", BigQueryIO.writeTableRows().to(options.getDataset())
        	.withSchema(schema)
        	.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
        	.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        
		pipeline.run();
		

	}

}
