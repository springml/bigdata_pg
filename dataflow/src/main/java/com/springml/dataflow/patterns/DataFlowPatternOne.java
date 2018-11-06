
package com.springml.dataflow.patterns;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.springml.dataflow.patterns.util.PatternOneOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sreeni kompelli
 * Pattern One implementation of
 * Pattern: Pushing data to multiple storage locations

 * <p>Concepts: Reading  from GCS, writing TO BigQuery and BigTable counting, user-defined PTransforms
 *
 * <p>Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 * <p>To execute this pipeline locally, specify the BigQuery table for the output with the form:
 * <pre>{@code
 * --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * }</pre>
 * <p>To change the runner, specify:
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 * <p>
 * See examples/java/README.md for instructions about how to configure different runners.
 *
--project=myspringml
--input=gs://dataflow-dev-myspringml/sample-email-data.csv
--output=myspringml:DataflowJavaSetup.patterns_multiple_data_sink
--tempLocation=gs://dataflow-dev-myspringml/temp
--bigtableProjectId=myspringml
--bigtableInstanceId=sj-sml
--bigtableTableId=Dataflow_test
--stagingLocation=gs://dataflow-dev-myspringml/staging
(local)
--runner=DirectRunner
(submit to cloud)
--runner=DataFlowRunner

 *
 *
 */
public class DataFlowPatternOne {

    private static final byte[] FAMILY = Bytes.toBytes("cf");
    private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");

    // This is a random value so that there will be some changes in the table
    // each time the job runs.
    private static final byte[] VALUE = Bytes.toBytes("value_" + (60 * Math.random()));
    // [START bigtable_dataflow_connector_process_element]
    static final DoFn<String, Mutation> BT_TRANSFORM = new DoFn<String, Mutation>() {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            c.output(new Put(c.element().getBytes()).addColumn(FAMILY, QUALIFIER, VALUE));
        }
    };
    // [END bigtable_dataflow_connector_process_element]


    private static TableSchema createBQSchema() {
        // Build the table schema for the output table.
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("email").setType("STRING"));
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
        return new TableSchema().setFields(fields);
    }

    private static CloudBigtableTableConfiguration getCloudBigtableTableConfiguration(PatternOneOptions options) {
        String PROJECT_ID = options.getBigtableProjectId();
        String INSTANCE_ID = options.getBigtableInstanceId();
        String TABLE_ID = options.getBigtableTableId();
        // [START bigtable_dataflow_connector_config]
        // [END bigtable_dataflow_connector_config]
        return new CloudBigtableTableConfiguration.Builder()
                .withProjectId(PROJECT_ID)
                .withInstanceId(INSTANCE_ID)
                .withTableId(TABLE_ID)
                .build();
    }

    //p
    public static class  StringToRowConverter extends DoFn<String, TableRow> {
        private static String[] columnNames = {"name", "email", "count"};

        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = new TableRow();
            String[] parts = c.element().split(",");
            for (int i = 0; i < parts.length; i++) {
                row.set(columnNames[i], parts[i]);
            }
            c.output(row);
        }

    }

    static void runPatternOne(PatternOneOptions options) {

        //Create Pipeline
        Pipeline pipeline = Pipeline.create(options);

        //START PCollection read from CloudStorage
        PCollection<String> csv = pipeline.apply("ReadLinesCSV", TextIO.read().from(options.getInput()));
        //END PCollection




        //START BigTableWrite
        CloudBigtableTableConfiguration config = getCloudBigtableTableConfiguration(options);
        // [START bigtable_dataflow_connector_write]
        csv.apply("BigTableTransform", ParDo.of(BT_TRANSFORM))
                .apply("WriteToBigTable", CloudBigtableIO.writeToTable(config));
        //END BigTableWrite

        //START BigQuerry
        //TableSchema schema = createBQSchema();
        csv.apply("BigQueryTransform", ParDo.of(new StringToRowConverter()))
                .apply("WriteToBigQuery", BigQueryIO.<TableRow>writeTableRows()
                        .to(options.getOutput())
                        // .withSchema(schema).withoutValidation()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        pipeline.run().waitUntilFinish();


    }

    public static void main(String[] args) {
        PatternOneOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PatternOneOptions.class);

        runPatternOne(options);
    }
}
