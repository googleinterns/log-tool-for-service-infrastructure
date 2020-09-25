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

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.logging.v2.model.LogEntry;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.api.servicecontrol.log.ServiceExtensionProtos;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.ServiceControlLogEntry;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.Timestamp;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.ServiceControlExtension;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.OperationInfo;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.StatusInfo;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.ExtensionRegistry;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.joda.time.DateTimeZone;
import org.joda.time.DateTime;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Map;
import java.util.HashMap;
import java.lang.StackTraceElement;
import java.util.Random;



// https://github.com/GoogleCloudPlatform/processing-logs-using-dataflow
public class LogAnalyticsPipeline {

    protected static final Logger LOG = LoggerFactory.getLogger(LogAnalyticsPipeline.class);
    protected static Random rand = new Random(); 

    /**
     * ParseStringToProtobufFn is a custom DoFn that parses a ProtoBuf string
     * The input format can be either JSON or text
     * The output is an instance of ServiceControlLogEntry defined in ServiceExtensionProtos.java
     */
    protected static class ParseStringToProtobufFn extends DoFn<String, ServiceControlLogEntry> {
        private String format; 

        public ParseStringToProtobufFn(String format) {
            this.format = format;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String s = c.element();
            // DEBUG:
            // System.out.println("Sampled String's length: \n" + s.length() + "\n");
            ServiceControlLogEntry.Builder builder = ServiceControlLogEntry.newBuilder();

            if (this.format.equals("json")) {
                try {
                    // Java: JSON -> Protobuf & back conversion
                    // https://stackoverflow.com/questions/28545401/java-json-protobuf-back-conversion/28555016
                    JsonFormat.Parser parser = JsonFormat.parser();
                    parser.ignoringUnknownFields().merge(s, builder);
                }
                catch(com.google.protobuf.InvalidProtocolBufferException e) {
                    LOG.error(e.getMessage());
                    System.out.println(e.getMessage());

                    StackTraceElement[] stktrace = e.getStackTrace();
                    for (int i = 0; i < stktrace.length; i++) { 
                        LOG.error("Index " + i 
                                           + " of stack trace"
                                           + " array conatins = "
                                           + stktrace[i].toString()); 
                    } 
                }
            } else {
                try {
                    // What does the protobuf text format look like?
                    // https://stackoverflow.com/questions/18873924/what-does-the-protobuf-text-format-look-like
                    TextFormat.getParser().merge(s, ExtensionRegistry.getEmptyRegistry(), builder);
                }
                catch(com.google.protobuf.TextFormat.ParseException e) {
                    LOG.error(e.getMessage());
                    System.out.println(e.getMessage());
                }
            }

            ServiceControlLogEntry scle = builder.build();
            // DEBUG:
            // System.out.println("Sampled ServiceControlLogEntry has timestamp: " + scle.hasTimestamp());
            // System.out.println("Sampled ServiceControlLogEntry has service_control_extension: " + scle.hasServiceControlExtension() + "\n");
            c.output(scle);
        }
    }

    /**
     * EmitLogMessageFn is a custom DoFn that transforms ServiceControlLogEntry to LogMessage
     * time_usec (as timestamp) is extracted from ServiceControlLogEntry and becomes a filed of LogMessage
     */
    protected static class EmitLogMessageFn extends DoFn<ServiceControlLogEntry, LogMessage> {
        // private boolean outputWithTimestamp;

        public EmitLogMessageFn(boolean outputWithTimestamp) {
            // this.outputWithTimestamp = outputWithTimestamp;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            LogMessage logMessage = parseEntry(c.element());
            if(logMessage != null) {
                c.output(logMessage);
            }
        }

        private LogMessage parseEntry(ServiceControlLogEntry entry) {
            try {
                //extract a field from a protobuf message
                long timeUsec = entry.getTimeUsec();
                return new LogMessage(timeUsec, entry);
            }
            // catch (IOException e) {
            //     LOG.error("IOException parsing entry: " + e.getMessage());
            // }
            catch(NullPointerException e) {
                LOG.error("NullPointerException parsing entry: " + e.getMessage());
            }
            return null;
        }
    }

    /**
     * PrintKVStringDoubleFn is a custom DoFn that prints the contents of KV<String, Double> in PCollection
     * It can be used for debug
     */
    // protected static class PrintKVStringDoubleFn extends DoFn<KV<String, Double>, KV<String, Double>> {
    //     private String heading;

    //     public PrintKVStringDoubleFn(String heading) {
    //         this.heading = heading;
    //     }

    //     @ProcessElement
    //     public void processElement(ProcessContext c) throws Exception {
    //         KV<String, Double> kv = c.element();
    //         System.out.println(heading + ": " + kv.toString());
    //         LOG.info(heading + ": " + kv.toString());
    //         c.output(kv);
    //     }
    // }

    /**
     * RemoveKeyPrefixFn is a custom DoFn that removes the given prefix for a string as the key in a KV<String, Double>
     * It can be used for unifying keys of PColeection<KV<String, Double>> for different fields of a same entity
     */
    protected static class RemoveKeyPrefixFn extends DoFn<KV<String, Double>, KV<String, Double>> {
        private String prefix;

        public RemoveKeyPrefixFn(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, Double> kv = c.element();
            c.output(KV.of(kv.getKey().substring(prefix.length()), kv.getValue()));
        }
    }

    /**
     * TimestampAndEntityKeyedFieldValueFn is a custom DoFn that extracts timestamp/entity/field information from a LogMessage and creates multiple KV<String, Double>s
     * The following are the format of the key string, which includes entity type, field name, timestamp, entity name (i.e., "${entityType}-${fieldName}-${timestamp}-${entityName}")
     * Note that '_' is used to keep different values of a same field align for the convenience of future processing (e.g., removing)
     * - service query count:         "service_-query_-${timestamp}-${serviceName}"
     * - service check error count:   "service_-check_-${timestamp}-${serviceName}"
     * - service quota error count:   "service_-quota_-${timestamp}-${serviceName}"
     * - service not-OK status count: "service_-status-${timestamp}-${serviceName}"
     * - consumer query count:        "consumer-query_-${timestamp}-${consumerName}"
     * timestamp, a 10-digit number standing for epoch seconds, is the start seconds of the time interval that a piece of information belongs to 
     * And it is not necessarily the same to its own epoch seconds
     * Note that it can be confusing that "timestamp" actually refers to "time interval"
     */
    protected static class TimestampAndEntityKeyedFieldValueFn extends DoFn<LogMessage, KV<String, Double>> {
        private long interval;

        public TimestampAndEntityKeyedFieldValueFn(long interval) {
            this.interval = interval;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LogMessage l = c.element();
            // ServiceControlExtension.Timestamp ts = l.getTimestamp(); 
            // ServiceControlExtension sce = l.getServiceControlLogEntry().getServiceControlExtension();
            long ts = l.getTimeUsec(); 
            ServiceControlExtension sce = l.getServiceControlLogEntry().getProtoContent();

            long seconds = ts / 1000000; // tricky
            long remainder = seconds % interval; // tricky
            String timestamp = (seconds - remainder) + "";

            String serviceName = sce.getServiceName();
                c.output(KV.of("service_-query_-" + timestamp + "-" + sce.getServiceName(), sce.getOperationsCount() + 0.0)); // 1.0?
                c.output(KV.of("service_-check_-" + timestamp + "-" + sce.getServiceName(), sce.getCheckErrorsCount() + 0.0));
                c.output(KV.of("service_-quota_-" + timestamp + "-" + sce.getServiceName(), sce.getQuotaErrorsCount() + 0.0));
                c.output(KV.of("service_-status-" + timestamp + "-" + sce.getServiceName(), (sce.getStatus().getCode() == 0) ? 0.0 : 1.0));

            for (OperationInfo op : sce.getOperationsList()) {
                String consumerProjectId = op.getConsumerProjectId();
                c.output(KV.of("consumer-query_-" + timestamp + "-" + op.getConsumerProjectId(), 1.0));
            }
        }
    }

    /**
     * EntityKeyedFieldValueFn is a custom DoFn that removes timestamp from a key string of KV<String, Double>
     * e.g., "${entityType}-${fieldName}-${timestamp}-${entityName}" becomes "${entityType}-${fieldName}-${entityName}"
     */
    protected static class EntityKeyedFieldValueFn extends DoFn<KV<String, Double>, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, Double> kv = c.element();
            String timestampEntity = kv.getKey();
            Double fieldValue = kv.getValue();

            String keyWithoutTimestamp = timestampEntity.substring(0, 16) + timestampEntity.substring(27);

            // String key0 = entityName;
            String key0 = keyWithoutTimestamp;
            // String key1 = timestampSeconds;
            Double value1 = fieldValue;
            // c.output(KV.of(key0, KV.of(key1, value1)));
            c.output(KV.of(key0, value1));
        }
    }

    /**
     * CalculateMeanPerIntervalFn is a custom DoFn that calculates mean per interval of a field for an entity
     * e.g., query per second for a service
     */
    protected static class CalculateMeanPerIntervalFn extends DoFn<KV<String, Double>, KV<String, Double>> {
        private long timeIntervalCount;

        public CalculateMeanPerIntervalFn(long timeIntervalCount) {
            this.timeIntervalCount = timeIntervalCount;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, Double> kv = c.element();
            String key = kv.getKey();
            Double value = kv.getValue() / this.timeIntervalCount;
            c.output(KV.of(key, value));
        }
    }

    /**
     * CalculateDeviationFn is a custom DoFn that calculates deviation of a field for an entity
     * (maximum count among all intervals - minimum count among all intervals) / all count among all intervals
     */
    protected static class CalculateDeviationFn extends DoFn<KV<String, CoGbkResult>, KV<String, Double>> {
        private TupleTag<Double> minTag;
        private TupleTag<Double> maxTag;
        private TupleTag<Double> sumTag;

        public CalculateDeviationFn(TupleTag<Double> minTag, TupleTag<Double> maxTag, TupleTag<Double> sumTag) {
            this.minTag = minTag;
            this.maxTag = maxTag;
            this.sumTag = sumTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, CoGbkResult> kv = c.element();
            String entityName = kv.getKey();
            CoGbkResult coGbkResult = kv.getValue();

            Double min = coGbkResult.getOnly(minTag);
            Double max = coGbkResult.getOnly(maxTag);
            Double sum = coGbkResult.getOnly(sumTag);

            String key = entityName;
            Double value = (sum == 0.0) ? 0.0 : (max - min) / sum; //TODO: reasonable corner case treament?
            c.output(KV.of(key, value));
        }
    }

    /**
     * CalculateRatioFn is a custom DoFn that calculates the ratio of the total count of a field to that of queries for an entity
     * all field count among all intervals / all queries among all intervals
     */
    protected static class CalculateRatioFn extends DoFn<KV<String, CoGbkResult>, KV<String, Double>> {
        private TupleTag<Double> querySumTag;
        private TupleTag<Double> fieldSumTag;

        public CalculateRatioFn(TupleTag<Double> querySumTag, TupleTag<Double> fieldSumTag) {
            this.querySumTag = querySumTag;
            this.fieldSumTag = fieldSumTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, CoGbkResult> kv = c.element();
            String entityName = kv.getKey();
            CoGbkResult coGbkResult = kv.getValue();

            Double querySum = coGbkResult.getOnly(querySumTag);
            Double fieldSum = coGbkResult.getOnly(fieldSumTag);

            String key = entityName;
            Double value = fieldSum / querySum;
            c.output(KV.of(key, value));
        }
    }

    /**
     * TimestampEntityFieldTableRowFn is a custom DoFn that transforms a KV<String, Double> of timestamp-entity-field to a TableRow for BigQuery storage
     * The format of a key string: ${entityType}-${fieldName}-${timestamp}-${entityName}"
     * The value is a Double of field value
     */
    protected static class TimestampEntityFieldTableRowFn extends DoFn<KV<String, Double>, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, Double> kv = c.element();
            String timestampEntity = kv.getKey();
            Double fieldValue = kv.getValue();

            String entityType = timestampEntity.substring(0, 8);
            String fieldName = timestampEntity.substring(9, 15);
            String timestampSeconds = timestampEntity.substring(16, 26);
            String entityName = timestampEntity.substring(27);

            TableRow row = new TableRow()
                .set("seconds",    timestampSeconds)
                .set("entityType", entityType)
                .set("entityName", entityName)
                .set("fieldName",  fieldName)
                .set("fieldValue", fieldValue + ""); //
            c.output(row);
        }
    }

    /**
     * TimestampServiceFieldListTableRowFn is a custom DoFn that transforms a KV<String, CoGbkResult> of service-field-list to a TableRow for BigQuery storage
     * The key string is the name of a service
     * The value, as a result of CoGroupByKey operations, contains multiple tagged field list (e.g., check error count, not-OK status count) for the keyed service 
     */
    protected static class TimestampServiceFieldListTableRowFn extends DoFn<KV<String, CoGbkResult>, TableRow> {
        private TupleTag<Double> queryTag;
        private TupleTag<Double> checkErrorTag;
        private TupleTag<Double> quotaErrorTag;
        private TupleTag<Double> stautsErrorTag;

        public TimestampServiceFieldListTableRowFn(TupleTag<Double> queryTag, TupleTag<Double> checkErrorTag, TupleTag<Double> quotaErrorTag, TupleTag<Double> stautsErrorTag) {
            this.queryTag   = queryTag;
            this.checkErrorTag   = checkErrorTag;
            this.quotaErrorTag   = quotaErrorTag;
            this.stautsErrorTag  = stautsErrorTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, CoGbkResult> kv = c.element();
            String key = kv.getKey();
            CoGbkResult coGbkResult = kv.getValue();

            String timestamp = key.substring(0, 10); //TODO: adjust hard-coded timestamp length (10)
            String serviceName = key.substring(10+1); // 

            Double query   = coGbkResult.getOnly(queryTag);
            Double check   = coGbkResult.getOnly(checkErrorTag);
            Double quota   = coGbkResult.getOnly(quotaErrorTag);
            Double status  = coGbkResult.getOnly(stautsErrorTag);

            TableRow row = new TableRow()
                .set("seconds", timestamp)
                .set("service", serviceName)
                .set("query",   query  + "")
                .set("check",   check  + "")
                .set("quota",   quota  + "")
                .set("status",  status + "");
            c.output(row);
        }
    }

    /**
     * ServiceFieldStatTableRowFn is a custom DoFn that transforms a KV<String, CoGbkResult> of service-fiels-stats to a TableRow for BigQuery storage
     * The key string is the name of a service
     * The value, as a result of CoGroupByKey operations, contains multiple tagged field stats (e.g., min check errors, ratio of not-OK status) for the keyed service 
     */
    protected static class ServiceFieldStatTableRowFn extends DoFn<KV<String, CoGbkResult>, TableRow> {
        private TupleTag<Double> querySumTag;
        private TupleTag<Double> checkErrorSumTag;
        private TupleTag<Double> quotaErrorSumTag;
        private TupleTag<Double> statusErrorSumTag;
        private TupleTag<Double> queryPerIntervalTag;
        private TupleTag<Double> queryDevTag;
        private TupleTag<Double> checkErrorRatioTag;
        private TupleTag<Double> quotaErrorRatioTag;
        private TupleTag<Double> statusErrorRatioTag;

        public ServiceFieldStatTableRowFn(TupleTag<Double> querySumTag, TupleTag<Double> checkErrorSumTag, TupleTag<Double> quotaErrorSumTag, TupleTag<Double> statusErrorSumTag, 
                                            TupleTag<Double> queryPerIntervalTag, TupleTag<Double> queryDevTag, TupleTag<Double> checkErrorRatioTag, TupleTag<Double> quotaErrorRatioTag, TupleTag<Double> statusErrorRatioTag) {
            this.querySumTag               = querySumTag;
            this.checkErrorSumTag          = checkErrorSumTag;
            this.quotaErrorSumTag          = quotaErrorSumTag;
            this.statusErrorSumTag         = statusErrorSumTag;
            this.queryPerIntervalTag       = queryPerIntervalTag;
            this.queryDevTag               = queryDevTag;
            this.checkErrorRatioTag        = checkErrorRatioTag;
            this.quotaErrorRatioTag        = quotaErrorRatioTag;
            this.statusErrorRatioTag       = statusErrorRatioTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, CoGbkResult> kv = c.element();
            String serviceName = kv.getKey();
            CoGbkResult coGbkResult = kv.getValue();

            Double querySum              = coGbkResult.getOnly(querySumTag);
            Double checkErrorSum         = coGbkResult.getOnly(checkErrorSumTag);
            Double quotaErrorSum         = coGbkResult.getOnly(quotaErrorSumTag);
            Double statusErrorSum        = coGbkResult.getOnly(statusErrorSumTag);
            Double queryPerInterval      = coGbkResult.getOnly(queryPerIntervalTag);
            Double queryDev              = coGbkResult.getOnly(queryDevTag);
            Double checkErrorRatio       = coGbkResult.getOnly(checkErrorRatioTag);
            Double quotaErrorRatio       = coGbkResult.getOnly(quotaErrorRatioTag);
            Double statusErrorRatio      = coGbkResult.getOnly(statusErrorRatioTag);

            TableRow row = new TableRow()
                .set("service",               serviceName)
                .set("querySum",              querySum          + "")
                .set("checkErrorSum",         checkErrorSum          + "")
                .set("quotaErrorSum",         quotaErrorSum          + "")
                .set("statusErrorSum",        statusErrorSum         + "")
                .set("queryPerInterval",      queryPerInterval  + "")
                .set("queryDev",              queryDev          + "")
                .set("checkErrorRatio",       checkErrorRatio        + "")
                .set("quotaErrorRatio",       quotaErrorRatio        + "")
                .set("statusErrorRatio",      statusErrorRatio       + "");
            c.output(row);
        }
    }

    /**
     * ConsumerFieldStatTableRowFn is a custom DoFn that transforms a KV<String, CoGbkResult> of consumer-field-stats to a TableRow for BigQuery storage
     * The key string is the name of a consumer
     * The value, as a result of CoGroupByKey operations, contains multiple tagged field (so far query only) stats for the keyed consumer 
     */
    protected static class ConsumerFieldStatTableRowFn extends DoFn<KV<String, CoGbkResult>, TableRow> {
        private TupleTag<Double> querySumTag;
        // private TupleTag<Double> checkErrorSumTag;
        // private TupleTag<Double> quotaErrorSumTag;
        // private TupleTag<Double> statusErrorSumTag;
        private TupleTag<Double> queryPerIntervalTag;
        private TupleTag<Double> queryDevTag;
        // private TupleTag<Double> checkErrorRatioTag;
        // private TupleTag<Double> quotaErrorRatioTag;
        // private TupleTag<Double> statusErrorRatioTag;

        public ConsumerFieldStatTableRowFn(TupleTag<Double> querySumTag, 
                                            TupleTag<Double> queryPerIntervalTag, TupleTag<Double> queryDevTag) {
            this.querySumTag          = querySumTag;
            this.queryPerIntervalTag  = queryPerIntervalTag;
            this.queryDevTag          = queryDevTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, CoGbkResult> kv = c.element();
            String consumerName = kv.getKey();
            CoGbkResult coGbkResult = kv.getValue();

            Double querySum         = coGbkResult.getOnly(querySumTag);
            Double queryPerInterval = coGbkResult.getOnly(queryPerIntervalTag);
            Double queryDev         = coGbkResult.getOnly(queryDevTag);

            TableRow row = new TableRow()
                .set("consumer",         consumerName)
                .set("querySum",         querySum          + "")
                .set("queryPerInterval", queryPerInterval  + "")
                .set("queryDev",         queryDev          + "");
            c.output(row);
        }
    }

    /**
     * TableRowOutputTransform is a custom DoFn that outputs TableRow to BigQuery 
     * Accoding to given table name and table schema
     */
    protected static class TableRowOutputTransform extends PTransform<PCollection<KV<String,Double>>,PCollection<TableRow>> {
        private String tableSchema;
        private String tableName;

        public TableRowOutputTransform(String tableSchema, String tableName) {
            this.tableSchema = tableSchema;
            this.tableName = tableName;
        }

        public static TableSchema createTableSchema(String schema) {
            String[] fieldTypePairs = schema.split(",");
            List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();

            for(String entry : fieldTypePairs) {
                String[] fieldAndType = entry.split(":");
                fields.add(new TableFieldSchema().setName(fieldAndType[0]).setType(fieldAndType[1]));
            }

            return new TableSchema().setFields(fields);
        }

        public PCollection<TableRow> expand(PCollection<KV<String,Double>> input) {
            PCollection<TableRow> output = input.
              apply( "aggregateToTableRow", ParDo.of(new DoFn<KV<String, Double>, TableRow>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                      KV<String, Double> e = c.element();

                      TableRow row = new TableRow()
                        .set("destination", e.getKey())
                        .set("aggResponseTime", e.getValue());

                      c.output(row);
                  }
              }));

            output.apply("tableRowToBigQuery", BigQueryIO.writeTableRows()
              .to(this.tableName)
              .withSchema(createTableSchema(this.tableSchema))
              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

            return output;
        }
    }

    /**
     * getTimestampEntityFieldsCombinedWithinEachInterval is a custom function that
     * - Transforms LogMessage to KV<String, Double> ("${entityType}-${fieldName}-${timestamp}-${entityName}", ${fieldValue})
     * - Sums up field values with same key (same time interval, same entity name, same field name)
     */
    protected static PCollection<KV<String, Double>> getTimestampEntityFieldsCombinedWithinEachInterval(PCollection<LogMessage> allLogMessages, long interval) {
        PCollection<KV<String, Double>> res = allLogMessages
            .apply("ApplyInterval", ParDo.of(new TimestampAndEntityKeyedFieldValueFn(interval)))
            .apply("CombineWithinInterval", Combine.<String, Double, Double>perKey(Sum.ofDoubles())) // .apply(Sum.<String>doublesPerKey())
            ;//.apply("Print", ParDo.of(new PrintKVStringDoubleFn("TimestampEntityFields")));
        return res;
    }

    /**
     * doFilterAndRemoveKeyPrefix is a custom function that
     * - Filters KV<String, Double> by the prefix of key string
     * - Removes the prefix of filtered key string
     * It can be used for unifying keys of PColeection<KV<String, Double>> for different fields of a same entity
     */
    protected static PCollection<KV<String, Double>> doFilterAndRemoveKeyPrefix(PCollection<KV<String, Double>> pc, String prefix) {
        PCollection<KV<String, Double>> res = pc
            .apply(prefix + "Filter" + Integer.toString(rand.nextInt(1024)), Filter.by(new SerializableFunction<KV<String, Double>, Boolean>() {
                @Override
                public Boolean apply(KV<String, Double> input) {
                    return input.getKey().startsWith(prefix);
                }
            }))
            .apply(prefix + "Remove" + Integer.toString(rand.nextInt(1024)), ParDo.of(new RemoveKeyPrefixFn(prefix)));
        return res;
    }

    /**
     * getMinMaxSum is a custom function that processes a PCollection of KV<String, Double> as timestamp-entity-field ("${entityType}-${fieldName}-${timestamp}-${entityName}", ${fieldValue}):
     * - Gets min: KV<String, Double> ("${entityType}-${fieldName}-${entityName}", ${minimumFieldValueAmongAllTimeIntervals})
     * - Gets max: KV<String, Double> ("${entityType}-${fieldName}-${entityName}", ${maximumFieldValueAmongAllTimeIntervals})
     * - Gets sum: KV<String, Double> ("${entityType}-${fieldName}-${entityName}", ${totalFieldValueAmongAllTimeIntervals})
     * The results are put into a Map instance
     */
    protected static Map<String, PCollection<KV<String, Double>>> getMinMaxSum(PCollection<KV<String, Double>> timestampEntityFields) {
        PCollection<KV<String, Double>> entityField = timestampEntityFields
            .apply("GetEntityKeyedFieldValue", ParDo.of(new EntityKeyedFieldValueFn()));

        PCollection<KV<String, Double>> min = entityField
            .apply(Min.<String>doublesPerKey()) 
            ;//.apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Min)")));

        PCollection<KV<String, Double>> max = entityField
            .apply(Max.<String>doublesPerKey()) 
            ;//.apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Max)")));

        PCollection<KV<String, Double>> sum = entityField
            .apply("sum", Combine.<String, Double, Double>perKey(Sum.ofDoubles())) 
            ;//.apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Sum)")));

        Map<String, PCollection<KV<String, Double>>> res = new HashMap<String, PCollection<KV<String, Double>>>();
        res.put("min", min);
        res.put("max", max);
        res.put("sum", sum);
        return res;
    }

    /**
     * getMeanPerInterval is a custom function that processes a PCollection of KV<String, Double> as entity-field-sum ("${entityType}-${fieldName}-${entityName}", ${totalFieldValueAmongAllTimeIntervals}):
     * Gets mean per interval: KV<String, Double> ("${entityType}-${fieldName}-${entityName}", ${meanFieldValuePerTimeInterval})
     */
    protected static PCollection<KV<String, Double>> getMeanPerInterval(PCollection<KV<String, Double>> entityFieldSum, long timeIntervalCount) {
        PCollection<KV<String, Double>> res = entityFieldSum
            .apply("CalculateMeanPerInterval", ParDo.of(new CalculateMeanPerIntervalFn(timeIntervalCount)))
            ;//.apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (PerInterval)")));;

        return res;
    }

    /**
     * getDeviation is a custom function that processes a Map with values of PCollection of KV<String, Double> as entity-field-stats (stats: min/max/sum)
     * Gets deviation: KV<String, Double> ("${entityType}-${fieldName}-${entityName}", ${deviationFieldValueAmongAllTimeIntervals})
     * The way to calculate deviation is given in CalculateDeviationFn
     */
    protected static PCollection<KV<String, Double>> getDeviation(Map<String, PCollection<KV<String, Double>>> entityFieldMinMaxSum) {
        final TupleTag<Double> minTag = new TupleTag<Double>();
        final TupleTag<Double> maxTag = new TupleTag<Double>();
        final TupleTag<Double> sumTag = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(minTag,  entityFieldMinMaxSum.get("min"))
            .and(maxTag, entityFieldMinMaxSum.get("max"))
            .and(sumTag, entityFieldMinMaxSum.get("sum"))
            .apply("DevCGBK", CoGroupByKey.<String>create());

        PCollection<KV<String, Double>> res = joined
            .apply("CalculateDeviation", ParDo.of(new CalculateDeviationFn(minTag, maxTag, sumTag)))
            ;//.apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Dev)")));

        return res;
    }

    /**
     * getRatio is a custom function that processes a of PCollection of KV<String, Double> as entity-field-stats (field: query/other, stats: sum)
     * Gets ratio: KV<String, Double> ("${entityType}-${fieldName}-${entityName}", ${FieldValueOverQueriesAmongAllTimeIntervals})
     */
    protected static PCollection<KV<String, Double>> getRatio(PCollection<KV<String, Double>> entityFieldSum, String entityType, String fieldName) {
        // filter and rename key first
        PCollection<KV<String, Double>> specificEntityQuerySum = doFilterAndRemoveKeyPrefix(entityFieldSum, entityType + "-" + "query_"  + "-");
        PCollection<KV<String, Double>> specificEntityFieldSum = doFilterAndRemoveKeyPrefix(entityFieldSum, entityType + "-" + fieldName + "-");
        
        final TupleTag<Double> querySumTag = new TupleTag<Double>();
        final TupleTag<Double> fieldSumTag = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(querySumTag,  specificEntityQuerySum)
            .and(fieldSumTag, specificEntityFieldSum)
            .apply(entityType + fieldName + "RatioCGBK", CoGroupByKey.<String>create());

        PCollection<KV<String, Double>> res = joined
            .apply("CalculateRatio" + Integer.toString(rand.nextInt(1024)), ParDo.of(new CalculateRatioFn(querySumTag, fieldSumTag)))
            ;//.apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Ratio)")));

        return res;
    }

    /**
     * writeTimestampEntityFieldToBigQuery is a custom function that outputs PCollection<KV<String, Double>> as timestamp-entity-field to BigQuery
     */
    protected static boolean writeTimestampEntityFieldToBigQuery(PCollection<KV<String, Double>> timestampEntityFields, String bqTempLocation, String tableName, String tableSchema, BigQueryIO.Write.WriteDisposition writeDisposition) {
        timestampEntityFields.apply("", ParDo.of(new TimestampEntityFieldTableRowFn()))
            .apply("ToBigQuery", BigQueryIO.writeTableRows()
                .to(tableName)
                .withSchema(TableRowOutputTransform.createTableSchema(tableSchema))
                .withCustomGcsTempLocation(StaticValueProvider.of(bqTempLocation))
                .withWriteDisposition(writeDisposition) // WRITE_APPEND
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return true;
    }

    /**
     * writeTimestampServiceFieldListsToBigQuery is a custom function that outputs Map<String, PCollection<KV<String, Double>>> as timestamp-service-field-lists to BigQuery
     */
    protected static boolean writeTimestampServiceFieldListsToBigQuery(Map<String, PCollection<KV<String, Double>>> timestampServiceFieldLists, String bqTempLocation, String tableName, String tableSchema) {
        final TupleTag<Double> queryTag       = new TupleTag<Double>();
        final TupleTag<Double> checkErrorTag  = new TupleTag<Double>();
        final TupleTag<Double> quotaErrorTag  = new TupleTag<Double>();
        final TupleTag<Double> stautsErrorTag = new TupleTag<Double>();
        
        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(queryTag,        timestampServiceFieldLists.get("query"))
            .and(checkErrorTag,  timestampServiceFieldLists.get("check"))
            .and(quotaErrorTag,  timestampServiceFieldLists.get("quota"))
            .and(stautsErrorTag, timestampServiceFieldLists.get("status"))
            .apply("TimestampServiceFieldListsCGBK", CoGroupByKey.<String>create());

        joined.apply("", ParDo.of(new TimestampServiceFieldListTableRowFn(queryTag, checkErrorTag, quotaErrorTag, stautsErrorTag)))
            .apply("ToBigQuery", BigQueryIO.writeTableRows()
                .to(tableName)
                .withSchema(TableRowOutputTransform.createTableSchema(tableSchema))
                .withCustomGcsTempLocation(StaticValueProvider.of(bqTempLocation))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE) // WRITE_APPEND
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return true;

    }

    /**
     * writeServiceFieldStatsToBigQuery is a custom function that outputs Map<String, PCollection<KV<String, Double>>> as service-field-stats to BigQuery
     */
    protected static boolean writeServiceFieldStatsToBigQuery(Map<String, PCollection<KV<String, Double>>> serviceFieldStats, String bqTempLocation, String tableName, String tableSchema) {
        final TupleTag<Double> querySumTag               = new TupleTag<Double>();
        final TupleTag<Double> checkErrorSumTag          = new TupleTag<Double>();
        final TupleTag<Double> quotaErrorSumTag          = new TupleTag<Double>();
        final TupleTag<Double> statusErrorSumTag         = new TupleTag<Double>();
        final TupleTag<Double> queryPerIntervalTag       = new TupleTag<Double>();
        final TupleTag<Double> queryDevTag               = new TupleTag<Double>();
        final TupleTag<Double> checkErrorRatioTag        = new TupleTag<Double>();
        final TupleTag<Double> quotaErrorRatioTag        = new TupleTag<Double>();
        final TupleTag<Double> statusErrorRatioTag       = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(querySumTag,               serviceFieldStats.get("querySum"))
            .and(checkErrorSumTag,         serviceFieldStats.get("checkErrorSum"))
            .and(quotaErrorSumTag,         serviceFieldStats.get("quotaErrorSum"))
            .and(statusErrorSumTag,        serviceFieldStats.get("statusErrorSum"))
            .and(queryPerIntervalTag,      serviceFieldStats.get("queryPerInterval"))
            .and(queryDevTag,              serviceFieldStats.get("queryDev"))
            .and(checkErrorRatioTag,       serviceFieldStats.get("checkErrorRatio"))
            .and(quotaErrorRatioTag,       serviceFieldStats.get("quotaErrorRatio"))
            .and(statusErrorRatioTag,      serviceFieldStats.get("statusErrorRatio"))
            .apply("ServiceFieldStatsCGBK", CoGroupByKey.<String>create());

        joined.apply("", ParDo.of(new ServiceFieldStatTableRowFn(querySumTag, checkErrorSumTag, quotaErrorSumTag, statusErrorSumTag, queryPerIntervalTag, queryDevTag, checkErrorRatioTag, quotaErrorRatioTag, statusErrorRatioTag)))
            .apply("ToBigQuery", BigQueryIO.writeTableRows()
                .to(tableName)
                .withSchema(TableRowOutputTransform.createTableSchema(tableSchema))
                .withCustomGcsTempLocation(StaticValueProvider.of(bqTempLocation))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE) // WRITE_APPEND
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return true;

    }

    /**
     * writeConsumerFieldStatsToBigQuery is a custom function that outputs Map<String, PCollection<KV<String, Double>>> as consumer-field-stats to BigQuery
     */
    protected static boolean writeConsumerFieldStatsToBigQuery(Map<String, PCollection<KV<String, Double>>> stats, String bqTempLocation, String tableName, String tableSchema) {
        final TupleTag<Double> querySumTag               = new TupleTag<Double>();
        // final TupleTag<Double> checkErrorSumTag       = new TupleTag<Double>();
        // final TupleTag<Double> quotaErrorSumTag       = new TupleTag<Double>();
        // final TupleTag<Double> statusErrorSumTag      = new TupleTag<Double>();
        final TupleTag<Double> queryPerIntervalTag       = new TupleTag<Double>();
        final TupleTag<Double> queryDevTag               = new TupleTag<Double>();
        // final TupleTag<Double> checkErrorRatioTag     = new TupleTag<Double>();
        // final TupleTag<Double> quotaErrorRatioTag     = new TupleTag<Double>();
        // final TupleTag<Double> statusErrorRatioTag    = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(querySumTag,          stats.get("querySum"))
            .and(queryPerIntervalTag, stats.get("queryPerInterval"))
            .and(queryDevTag,         stats.get("queryDev"))
            .apply("ComsumerFieldStatsCGBK", CoGroupByKey.<String>create());

        joined.apply("", ParDo.of(new ConsumerFieldStatTableRowFn(querySumTag, queryPerIntervalTag, queryDevTag)))
            .apply("ToBigQuery", BigQueryIO.writeTableRows()
                .to(tableName)
                .withSchema(TableRowOutputTransform.createTableSchema(tableSchema))
                .withCustomGcsTempLocation(StaticValueProvider.of(bqTempLocation))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE) // WRITE_APPEND
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return true;

    }

    public static void main(String[] args) {
        LogAnalyticsPipelineOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(LogAnalyticsPipelineOptions.class);

        //TODO: use a config, e.g., protobuf file
        boolean outputWithTimestamp = true;
        String filepattern = options.getLogSource();
        String bqTempLocation = options.getBQTempLocation();
        long timeInterval = options.getTimeInterval();
        long timeIntervalCount = options.getTimeIntervalCount();
        String entityType = options.getEntityType();
        String fieldName = options.getFieldName();

        System.out.println("[0] Create a pipeline...\n");
        Pipeline p = Pipeline.create(options);

        /* (1) Read data (raw string?) as PCollection<MyMessage> from GCS */
        System.out.println("Input filepattern: " + filepattern + "\n");
        
        PCollection<ServiceControlLogEntry> allLogs = p.apply("logsTextRead", TextIO.read().from(filepattern))
            .apply("stringParsedToProtobuf", ParDo.of(new ParseStringToProtobufFn("json")));

        /* (2) Transform "allLogs" PCollection<String> to PCollection<LogMessage> by adding timestamp */
        PCollection<LogMessage> allLogMessages = allLogs
            .apply("allLogsToLogMessage", ParDo.of(new EmitLogMessageFn(outputWithTimestamp)));

        /* (3) Apply windowing scheme */
        /** Skip
         * It seems that there is no provided functions to aggregate over windows
         * TODO: Need more research later
         */

        /** (4) Aggregate interesting fields for entities within each time interval, 
         * and calculate statistical information among all time intervals
         */
        PCollection<KV<String, Double>> timestampEntityFields = getTimestampEntityFieldsCombinedWithinEachInterval(allLogMessages, timeInterval);

        Map<String, PCollection<KV<String, Double>>> entityFieldMinMaxSum  = getMinMaxSum(timestampEntityFields);
        PCollection<KV<String, Double>> entityFieldSum = entityFieldMinMaxSum.get("sum");

        PCollection<KV<String, Double>> entityFieldPerInteval = getMeanPerInterval(entityFieldSum, timeIntervalCount);

        PCollection<KV<String, Double>> entityFieldDev = getDeviation(entityFieldMinMaxSum);

        PCollection<KV<String, Double>> servicecheckErrorRatio  = getRatio(entityFieldSum, "service_", "check_");
        PCollection<KV<String, Double>> servicequotaErrorRatio  = getRatio(entityFieldSum, "service_", "quota_");
        PCollection<KV<String, Double>> servicestatusErrorRatio = getRatio(entityFieldSum, "service_", "status");

    
        /* (5) Store to BigQuery */
        System.out.println("GCS temp location to store temp files for BigQuery: " + bqTempLocation + "\n");

        writeTimestampEntityFieldToBigQuery(timestampEntityFields, 
                                            bqTempLocation, 
                                            options.getTimestampEntityFieldTableName(), 
                                            options.getTimestampEntityFieldTableSchema(),
                                            BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE); //

        Map<String, PCollection<KV<String, Double>>> timestampServiceFieldLists = new HashMap<String, PCollection<KV<String, Double>>>();
        timestampServiceFieldLists.put("query",  doFilterAndRemoveKeyPrefix(timestampEntityFields, "service_-query_-"));
        timestampServiceFieldLists.put("check",  doFilterAndRemoveKeyPrefix(timestampEntityFields, "service_-check_-"));
        timestampServiceFieldLists.put("quota",  doFilterAndRemoveKeyPrefix(timestampEntityFields, "service_-quota_-"));
        timestampServiceFieldLists.put("status", doFilterAndRemoveKeyPrefix(timestampEntityFields, "service_-status-"));
        writeTimestampServiceFieldListsToBigQuery(timestampServiceFieldLists, 
                                            bqTempLocation,
                                            options.getTimestampServiceFieldListTableName(), 
                                            options.getTimestampServiceFieldListTableSchema());

        Map<String, PCollection<KV<String, Double>>> serviceFieldStats = new HashMap<String, PCollection<KV<String, Double>>>();
        serviceFieldStats.put("querySum",         doFilterAndRemoveKeyPrefix(entityFieldSum,        "service_-query_-"));
        serviceFieldStats.put("checkErrorSum",         doFilterAndRemoveKeyPrefix(entityFieldSum,        "service_-check_-"));
        serviceFieldStats.put("quotaErrorSum",         doFilterAndRemoveKeyPrefix(entityFieldSum,        "service_-quota_-"));
        serviceFieldStats.put("statusErrorSum",        doFilterAndRemoveKeyPrefix(entityFieldSum,        "service_-status-"));
        serviceFieldStats.put("queryPerInterval", doFilterAndRemoveKeyPrefix(entityFieldPerInteval, "service_-query_-"));
        serviceFieldStats.put("queryDev",         doFilterAndRemoveKeyPrefix(entityFieldDev,        "service_-query_-"));
        serviceFieldStats.put("checkErrorRatio",       servicecheckErrorRatio);
        serviceFieldStats.put("quotaErrorRatio",       servicequotaErrorRatio);
        serviceFieldStats.put("statusErrorRatio",      servicestatusErrorRatio);
        writeServiceFieldStatsToBigQuery(serviceFieldStats, 
                                            bqTempLocation,
                                            options.getServiceFieldStatsTableName(), 
                                            options.getServiceFieldStatsTableSchema());

        Map<String, PCollection<KV<String, Double>>> consumerFieldStats = new HashMap<String, PCollection<KV<String, Double>>>();
        consumerFieldStats.put("querySum",         doFilterAndRemoveKeyPrefix(entityFieldSum,        "consumer-query_-"));
        consumerFieldStats.put("queryPerInterval", doFilterAndRemoveKeyPrefix(entityFieldPerInteval, "consumer-query_-"));
        consumerFieldStats.put("queryDev",         doFilterAndRemoveKeyPrefix(entityFieldDev,        "consumer-query_-"));
        writeConsumerFieldStatsToBigQuery(consumerFieldStats, 
                                            bqTempLocation,
                                            options.getConsumerFieldStatsTableName(), 
                                            options.getConsumerFieldStatsTableSchema());

        System.out.println("[1] Run the pipeline...\n");
        PipelineResult r = p.run();

        LOG.info(r.toString());
        System.out.println("\nPipeline result: \n" + r.toString() + '\n');
    }
}
