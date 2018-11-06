package com.springml.dataflow.patterns.util;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface PatternOneOptions extends DataflowPipelineOptions {
  // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
  String WEATHER_SAMPLES_TABLE =
          "clouddataflow-readonly:samples.weather_stations";
  @Description("The Google Cloud project ID for the Cloud Bigtable instance.")
  String getBigtableProjectId();

  void setBigtableProjectId(String bigtableProjectId);

  @Description("The Google Cloud Bigtable instance ID .")
  String getBigtableInstanceId();

  void setBigtableInstanceId(String bigtableInstanceId);

  @Description("The Cloud Bigtable table ID in the instance." )
  String getBigtableTableId();

  void setBigtableTableId(String bigtableTableId);

  @Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
  @Default.String(WEATHER_SAMPLES_TABLE)
  String getInput();

  void setInput(String value);

  @Description(
          "BigQuery table to write to, specified as "
                  + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
  @Validation.Required
  String getOutput();

  void setOutput(String value);

}
