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



// https://github.com/GoogleCloudPlatform/processing-logs-using-dataflow
public class LogAnalyticsPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(LogAnalyticsPipeline.class);

    private static class ParseStringToProtobufFn extends DoFn<String, ServiceControlLogEntry> {
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

    private static class EmitLogMessageFn extends DoFn<ServiceControlLogEntry, LogMessage> {
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

    private static class PrintKVStringDoubleFn extends DoFn<KV<String, Double>, KV<String, Double>> {
        private String heading;

        public PrintKVStringDoubleFn(String heading) {
            this.heading = heading;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, Double> kv = c.element();
            System.out.println(heading + ": " + kv.toString());
            c.output(kv);
        }
    }

    private static class RemoveKeyPrefixFn extends DoFn<KV<String, Double>, KV<String, Double>> {
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

    private static class TimestampAndEntityKeyedFieldValueFn extends DoFn<LogMessage, KV<String, Double>> {
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

    private static class EntityKeyedFieldValueFn extends DoFn<KV<String, Double>, KV<String, Double>> {
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

    private static class CalculateMeanPerIntervalFn extends DoFn<KV<String, Double>, KV<String, Double>> {
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

    private static class CalculateDeviationFn extends DoFn<KV<String, CoGbkResult>, KV<String, Double>> {
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

    private static class CalculateRatioFn extends DoFn<KV<String, CoGbkResult>, KV<String, Double>> {
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

    private static class TimestampEntityFieldTableRowFn extends DoFn<KV<String, Double>, TableRow> {
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

    private static class ServiceFieldStatTableRowFn extends DoFn<KV<String, CoGbkResult>, TableRow> {
        private TupleTag<Double> querySumTag;
        private TupleTag<Double> checkSumTag;
        private TupleTag<Double> quotaSumTag;
        private TupleTag<Double> statusSumTag;
        private TupleTag<Double> queryPerIntervalTag;
        private TupleTag<Double> queryDevTag;
        private TupleTag<Double> checkRatioTag;
        private TupleTag<Double> quotaRatioTag;
        private TupleTag<Double> statusRatioTag;

        public ServiceFieldStatTableRowFn(TupleTag<Double> querySumTag, TupleTag<Double> checkSumTag, TupleTag<Double> quotaSumTag, TupleTag<Double> statusSumTag, 
                                            TupleTag<Double> queryPerIntervalTag, TupleTag<Double> queryDevTag, TupleTag<Double> checkRatioTag, TupleTag<Double> quotaRatioTag, TupleTag<Double> statusRatioTag) {
            this.querySumTag          = querySumTag;
            this.checkSumTag          = checkSumTag;
            this.quotaSumTag          = quotaSumTag;
            this.statusSumTag         = statusSumTag;
            this.queryPerIntervalTag  = queryPerIntervalTag;
            this.queryDevTag          = queryDevTag;
            this.checkRatioTag        = checkRatioTag;
            this.quotaRatioTag        = quotaRatioTag;
            this.statusRatioTag       = statusRatioTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, CoGbkResult> kv = c.element();
            String serviceName = kv.getKey();
            CoGbkResult coGbkResult = kv.getValue();

            Double querySum         = coGbkResult.getOnly(querySumTag);
            Double checkSum         = coGbkResult.getOnly(checkSumTag);
            Double quotaSum         = coGbkResult.getOnly(quotaSumTag);
            Double statusSum        = coGbkResult.getOnly(statusSumTag);
            Double queryPerInterval = coGbkResult.getOnly(queryPerIntervalTag);
            Double queryDev         = coGbkResult.getOnly(queryDevTag);
            Double checkRatio       = coGbkResult.getOnly(checkRatioTag);
            Double quotaRatio       = coGbkResult.getOnly(quotaRatioTag);
            Double statusRatio      = coGbkResult.getOnly(statusRatioTag);

            TableRow row = new TableRow()
                .set("service",          serviceName)
                .set("querySum",         querySum          + "")
                .set("checkSum",         checkSum          + "")
                .set("quotaSum",         quotaSum          + "")
                .set("statusSum",        statusSum         + "")
                .set("queryPerInterval", queryPerInterval  + "")
                .set("queryDev",         queryDev          + "")
                .set("checkRatio",       checkRatio        + "")
                .set("quotaRatio",       quotaRatio        + "")
                .set("statusRatio",      statusRatio       + "");
            c.output(row);
        }
    }

    private static class ConsumerFieldStatTableRowFn extends DoFn<KV<String, CoGbkResult>, TableRow> {
        private TupleTag<Double> querySumTag;
        // private TupleTag<Double> checkSumTag;
        // private TupleTag<Double> quotaSumTag;
        // private TupleTag<Double> statusSumTag;
        private TupleTag<Double> queryPerIntervalTag;
        private TupleTag<Double> queryDevTag;
        // private TupleTag<Double> checkRatioTag;
        // private TupleTag<Double> quotaRatioTag;
        // private TupleTag<Double> statusRatioTag;

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

    private static class TableRowOutputTransform extends PTransform<PCollection<KV<String,Double>>,PCollection<TableRow>> {
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

    private static PCollection<KV<String, Double>> getTimestampEntityFieldsCombinedWithinEachInterval(PCollection<LogMessage> allLogMessages, long interval) {
        PCollection<KV<String, Double>> res = allLogMessages
            .apply("", ParDo.of(new TimestampAndEntityKeyedFieldValueFn(interval)))
            .apply(Combine.<String, Double, Double>perKey(Sum.ofDoubles())) // .apply(Sum.<String>doublesPerKey())
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn("")));
        return res;
    }

    private static PCollection<KV<String, Double>> doFilterAndRemoveKeyPrefix(PCollection<KV<String, Double>> pc, String prefix) {
        PCollection<KV<String, Double>> res = pc
            .apply(Filter.by(new SerializableFunction<KV<String, Double>, Boolean>() {
                @Override
                public Boolean apply(KV<String, Double> input) {
                    return input.getKey().startsWith(prefix);
                }
            }))
            .apply("", ParDo.of(new RemoveKeyPrefixFn(prefix)));
        return res;
    }

    private static Map<String, PCollection<KV<String, Double>>> getMinMaxSum(PCollection<KV<String, Double>> timestampEntityFields) {
        PCollection<KV<String, Double>> entityField = timestampEntityFields
            .apply("", ParDo.of(new EntityKeyedFieldValueFn()));

        PCollection<KV<String, Double>> min = entityField
            .apply(Min.<String>doublesPerKey()) 
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Min)")));

        PCollection<KV<String, Double>> max = entityField
            .apply(Max.<String>doublesPerKey()) 
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Max)")));

        PCollection<KV<String, Double>> sum = entityField
            .apply(Combine.<String, Double, Double>perKey(Sum.ofDoubles())) 
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Sum)")));

        Map<String, PCollection<KV<String, Double>>> res = new HashMap<String, PCollection<KV<String, Double>>>();
        res.put("min", min);
        res.put("max", max);
        res.put("sum", sum);
        return res;
    }

    private static PCollection<KV<String, Double>> getMeanPerInterval(PCollection<KV<String, Double>> entityFieldSum, long timeIntervalCount) {
        PCollection<KV<String, Double>> res = entityFieldSum
            .apply("", ParDo.of(new CalculateMeanPerIntervalFn(timeIntervalCount)))
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (PerInterval)")));;

        return res;
    }

    private static PCollection<KV<String, Double>> getDeviation(Map<String, PCollection<KV<String, Double>>> entityFieldMinMaxSum) {
        final TupleTag<Double> minTag = new TupleTag<Double>();
        final TupleTag<Double> maxTag = new TupleTag<Double>();
        final TupleTag<Double> sumTag = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(minTag,  entityFieldMinMaxSum.get("min"))
            .and(maxTag, entityFieldMinMaxSum.get("max"))
            .and(sumTag, entityFieldMinMaxSum.get("sum"))
            .apply(CoGroupByKey.<String>create());

        PCollection<KV<String, Double>> res = joined
            .apply("", ParDo.of(new CalculateDeviationFn(minTag, maxTag, sumTag)))
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Dev)")));

        return res;
    }

    private static PCollection<KV<String, Double>> getRatio(PCollection<KV<String, Double>> entityFieldSum, String entityType, String fieldName) {
        // filter and rename key first
        PCollection<KV<String, Double>> specificEntityQuerySum = doFilterAndRemoveKeyPrefix(entityFieldSum, entityType + "-" + "query_"  + "-");
        PCollection<KV<String, Double>> specificEntityFieldSum = doFilterAndRemoveKeyPrefix(entityFieldSum, entityType + "-" + fieldName + "-");
        
        final TupleTag<Double> querySumTag = new TupleTag<Double>();
        final TupleTag<Double> fieldSumTag = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(querySumTag,  specificEntityQuerySum)
            .and(fieldSumTag, specificEntityFieldSum)
            .apply(CoGroupByKey.<String>create());

        PCollection<KV<String, Double>> res = joined
            .apply("", ParDo.of(new CalculateRatioFn(querySumTag, fieldSumTag)))
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(" (Ratio)")));

        return res;
    }

    private static boolean writeTimestampEntityFieldToBigQuery(PCollection<KV<String, Double>> timestampEntityFields, String bqTempLocation, String tableName, String tableSchema, BigQueryIO.Write.WriteDisposition writeDisposition) {
        timestampEntityFields.apply("", ParDo.of(new TimestampEntityFieldTableRowFn()))
            .apply("ToBigQuery", BigQueryIO.writeTableRows()
                .to(tableName)
                .withSchema(TableRowOutputTransform.createTableSchema(tableSchema))
                .withCustomGcsTempLocation(StaticValueProvider.of(bqTempLocation))
                .withWriteDisposition(writeDisposition) // WRITE_APPEND
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return true;
    }

    private static boolean writeServiceFieldStatsToBigQuery(Map<String, PCollection<KV<String, Double>>> serviceFieldStats, String bqTempLocation, String tableName, String tableSchema) {
        final TupleTag<Double> querySumTag          = new TupleTag<Double>();
        final TupleTag<Double> checkSumTag          = new TupleTag<Double>();
        final TupleTag<Double> quotaSumTag          = new TupleTag<Double>();
        final TupleTag<Double> statusSumTag         = new TupleTag<Double>();
        final TupleTag<Double> queryPerIntervalTag  = new TupleTag<Double>();
        final TupleTag<Double> queryDevTag          = new TupleTag<Double>();
        final TupleTag<Double> checkRatioTag        = new TupleTag<Double>();
        final TupleTag<Double> quotaRatioTag        = new TupleTag<Double>();
        final TupleTag<Double> statusRatioTag       = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(querySumTag,          serviceFieldStats.get("querySum"))
            .and(checkSumTag,         serviceFieldStats.get("checkSum"))
            .and(quotaSumTag,         serviceFieldStats.get("quotaSum"))
            .and(statusSumTag,        serviceFieldStats.get("statusSum"))
            .and(queryPerIntervalTag, serviceFieldStats.get("queryPerInterval"))
            .and(queryDevTag,         serviceFieldStats.get("queryDev"))
            .and(checkRatioTag,       serviceFieldStats.get("checkRatio"))
            .and(quotaRatioTag,       serviceFieldStats.get("quotaRatio"))
            .and(statusRatioTag,      serviceFieldStats.get("statusRatio"))
            .apply(CoGroupByKey.<String>create());

        joined.apply("", ParDo.of(new ServiceFieldStatTableRowFn(querySumTag, checkSumTag, quotaSumTag, statusSumTag, queryPerIntervalTag, queryDevTag, checkRatioTag, quotaRatioTag, statusRatioTag)))
            .apply("ToBigQuery", BigQueryIO.writeTableRows()
                .to(tableName)
                .withSchema(TableRowOutputTransform.createTableSchema(tableSchema))
                .withCustomGcsTempLocation(StaticValueProvider.of(bqTempLocation))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE) // WRITE_APPEND
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return true;

    }

    private static boolean writeConsumerFieldStatsToBigQuery(Map<String, PCollection<KV<String, Double>>> stats, String bqTempLocation, String tableName, String tableSchema) {
        final TupleTag<Double> querySumTag          = new TupleTag<Double>();
        // final TupleTag<Double> checkSumTag       = new TupleTag<Double>();
        // final TupleTag<Double> quotaSumTag       = new TupleTag<Double>();
        // final TupleTag<Double> statusSumTag      = new TupleTag<Double>();
        final TupleTag<Double> queryPerIntervalTag  = new TupleTag<Double>();
        final TupleTag<Double> queryDevTag          = new TupleTag<Double>();
        // final TupleTag<Double> checkRatioTag     = new TupleTag<Double>();
        // final TupleTag<Double> quotaRatioTag     = new TupleTag<Double>();
        // final TupleTag<Double> statusRatioTag    = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(querySumTag,          stats.get("querySum"))
            .and(queryPerIntervalTag, stats.get("queryPerInterval"))
            .and(queryDevTag,         stats.get("queryDev"))
            .apply(CoGroupByKey.<String>create());

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
        // skip

        /* (4) Aggregate interesting fields */
        PCollection<KV<String, Double>> timestampEntityFields = getTimestampEntityFieldsCombinedWithinEachInterval(allLogMessages, timeInterval);

        Map<String, PCollection<KV<String, Double>>> entityFieldMinMaxSum  = getMinMaxSum(timestampEntityFields);
        PCollection<KV<String, Double>> entityFieldSum = entityFieldMinMaxSum.get("sum");

        PCollection<KV<String, Double>> entityFieldPerInteval = getMeanPerInterval(entityFieldSum, timeIntervalCount);

        PCollection<KV<String, Double>> entityFieldDev = getDeviation(entityFieldMinMaxSum);

        PCollection<KV<String, Double>> serviceCheckRatio  = getRatio(entityFieldSum, "service_", "check_");
        PCollection<KV<String, Double>> serviceQuotaRatio  = getRatio(entityFieldSum, "service_", "quota_");
        PCollection<KV<String, Double>> serviceStatusRatio = getRatio(entityFieldSum, "service_", "status");

        /* (5) Store to BigQuery */
        System.out.println("GCS temp location to store temp files for BigQuery: " + bqTempLocation + "\n");

        writeTimestampEntityFieldToBigQuery(timestampEntityFields, 
                                            bqTempLocation, 
                                            options.getTimestampEntityFieldTableName(), 
                                            options.getTimestampEntityFieldTableSchema(),
                                            BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE); //

        Map<String, PCollection<KV<String, Double>>> serviceFieldStats = new HashMap<String, PCollection<KV<String, Double>>>();
        serviceFieldStats.put("querySum",         doFilterAndRemoveKeyPrefix(entityFieldSum,        "service_-query_-"));
        serviceFieldStats.put("checkSum",         doFilterAndRemoveKeyPrefix(entityFieldSum,        "service_-check_-"));
        serviceFieldStats.put("quotaSum",         doFilterAndRemoveKeyPrefix(entityFieldSum,        "service_-quota_-"));
        serviceFieldStats.put("statusSum",        doFilterAndRemoveKeyPrefix(entityFieldSum,        "service_-status-"));
        serviceFieldStats.put("queryPerInterval", doFilterAndRemoveKeyPrefix(entityFieldPerInteval, "service_-query_-"));
        serviceFieldStats.put("queryDev",         doFilterAndRemoveKeyPrefix(entityFieldDev,        "service_-query_-"));
        serviceFieldStats.put("checkRatio",       serviceCheckRatio);
        serviceFieldStats.put("quotaRatio",       serviceQuotaRatio);
        serviceFieldStats.put("statusRatio",      serviceStatusRatio);
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
