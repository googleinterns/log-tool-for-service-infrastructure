// Copyright 2016-2020 Google LLC. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// reference: https://github.com/GoogleCloudPlatform/processing-logs-using-dataflow

package com.google.cloud.solutions;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
//
import org.apache.beam.sdk.options.ValueProvider;

@SuppressWarnings("unused")
public interface LogAnalyticsPipelineOptions extends PipelineOptions {
    @Description("Location of logs, Cloud Storage path or Cloud Pub/Sub subscription")
    @Default.String("input/Sample_log_*.csv")
    // @Default.String("gs://alchemy-logdata/Sample_log_*.csv")
    String getLogSource();
    void setLogSource(String logSource);

    // @Description("Regular expression pattern used to parse embedded log messages inside Cloud Logging entries")
    // @Default.String("\\[GIN\\]\\s+(?<timestamp>\\d{4}/\\d{2}/\\d{2} \\- \\d{2}\\:\\d{2}\\:\\d{2})\\s\\|\\s+(?<httpStatusCode>\\d{3})\\s+\\|\\s+(?<responseTime>\\d+\\.?\\d*)(?<resolution>\\S{1,})\\s+\\|\\s+(?<source>[0-9\\.:]+?)\\s+\\|\\s+(?<httpMethod>\\w+?)\\s+(?<destination>[a-z0-9/]+)")

    @Description("BigQueryIO.Write needs a GCS temp location to store temp files")
    @Default.String("gs://alchemy-logdata/temp")
    String getBQTempLocation();
    void setBQTempLocation(String tempLocation);
    // ValueProvider<String> getBQTempLocation(); //
    // void setBQTempLocation(ValueProvider<String> bqTempLocation); //

    @Description("Time interval as \"windowing\"")
    @Default.Long(1) // in second
    long getTimeInterval();
    void setTimeInterval(long TimeInterval);

    @Description("Counts of time intervals")
    @Default.Long(10) // in second
    long getTimeIntervalCount();
    void setTimeIntervalCount(long TimeIntervalCount);

    @Description("Entity type")
    @Default.String("service") // "consumer"
    String getEntityType();
    void setEntityType(String entityType);

    @Description("field name")
    @Default.String("query") // "status", "check", "quota"
    String getFieldName();
    void setFieldName(String fieldName);

    // @Description("BigQuery table name for service table")
    // @Default.String("dataflow_log_analytics.service_table")
    // String getServiceTableName();
    // void setServiceTableName(String serviceTableName);

    // @Description("BigQuery table schema for service table, comma-separated values of [field-name]:[TYPE]")
    // @Default.String("serviceName:STRING,seconds:STRING,nanos:STRING,statusMessage:STRING")
    // String getServiceTableSchema();
    // void setServiceTableSchema(String serviceTableSchema);

    // @Description("BigQuery table name for operation table")
    // @Default.String("dataflow_log_analytics.operation_table")
    // String getOperationTableName();
    // void setOperationTableName(String operationTableName);

    // @Description("BigQuery table schema for operation table, comma-separated values of [field-name]:[TYPE]")
    // @Default.String("operationId:STRING,operationName:STRING,associatedServiceName:STRING,consumerProjectId:STRING,startSeconds:STRING,startNanos:STRING,endSeconds:STRING,endNanos:STRING")
    // String getOperationTableSchema();
    // void setOperationTableSchema(String operationTableSchema);

    @Description("BigQuery table name for timestamp_entity_field table")
    @Default.String("dataflow_log_analytics.timestamp_entity_field_table")
    String getTimestampEntityFieldTableName();
    void setTimestampEntityFieldTableName(String timestampEntityFieldTableName);

    @Description("BigQuery table schema for timestamp_entity_field table, comma-separated values of [field-name]:[TYPE]")
    @Default.String("seconds:STRING,entityType:STRING,entityName:STRING,fieldName:String,fieldValue:STRING")
    String getTimestampEntityFieldTableSchema();
    void setTimestampEntityFieldTableSchema(String timestampEntityFieldTableSchema);

    @Description("BigQuery table name for service_field_stats table")
    @Default.String("dataflow_log_analytics.service_field_stats_table")
    String getServiceFieldStatsTableName();
    void setServiceFieldStatsTableName(String serviceFieldStatsTableName);

    @Description("BigQuery table schema for service_field_stats table, comma-separated values of [field-name]:[TYPE]")
    @Default.String("service:STRING,querySum:STRING,checkSum:STRING,quotaSum:STRING,statusSum:STRING,queryPerInterval:STRING,queryDev:STRING,checkRatio:STRING,quotaRatio:STRING,statusRatio:STRING")
    String getServiceFieldStatsTableSchema();
    void setServiceFieldStatsTableSchema(String serviceFieldStatsTableSchema);
}
