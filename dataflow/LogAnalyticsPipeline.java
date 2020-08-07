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

// ProtoCoder (Apache Beam)
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
// https://beam.apache.org/releases/javadoc/2.11.0/index.html?org/apache/beam/sdk/extensions/protobuf/ProtoCoder.html
// import MyProtoFile;
// import MyProtoFile.MyMessage;
import com.google.api.servicecontrol.log.ServiceExtensionProtos;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.ServiceControlLogEntry;
// Additional packages:
// import com.google.protobuf.timestamp;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.Timestamp;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.ServiceControlExtension;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.OperationInfo;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.StatusInfo;
//
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.ExtensionRegistry;
// https://cloud.google.com/dataflow/docs/guides/templates/creating-templates#java:-sdk-2.x
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
//
import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import org.joda.time.DateTimeZone;
import org.joda.time.DateTime;
//
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.transforms.View;
import java.util.Map;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Filter;
// 
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
//
import java.util.Map;
import java.util.HashMap;
//
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;

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
                // if(this.outputWithTimestamp) {
                //     c.outputWithTimestamp(logMessage, logMessage.getTimestamp());
                // }
                // else {
                    // DEBUG:
                    // System.out.println("Sampled LogMessage's timestamp: \n" + logMessage.getTimestamp());
                    // System.out.println("Sampled LogMessage's serviceControlLogEntry: \n" + logMessage.getServiceControlLogEntry());
                    // ServiceExtensionProtos.Timestamp timestamp = logMessage.getTimestamp();
                    // long seconds = timestamp.getSeconds();
                    // String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(seconds * 1000L));
                    // DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                    // Instant instant = Instant.parse(dateStr, dtf);
                    // DEBUG:
                    // System.out.println(instant.toString());
                    // c.outputWithTimestamp(logMessage, instant); //
                // }
                c.output(logMessage);
            }
        }

        private LogMessage parseEntry(ServiceControlLogEntry entry) {
            try {
                //extract a field from a protobuf message
                // Instant timestamp = (Instant) entry.getTimestamp();
                // return new LogMessage(timestamp, entry);
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

    public class SumFn extends CombineFn<Double, SumFn.Accum, Double> {
        public class Accum { // public static class Accum { //modifier static not allowed here
            double sum = 0;
            int count = 0;
        }
        public Accum createAccumulator() {
            return new Accum();
        }
        public Accum addInput(Accum accum, Double input) {
            accum.sum += input;
            accum.count++;
            return accum;
        }
        public Accum mergeAccumulators(Iterable<Accum> accums) {
            Accum merged = createAccumulator();
            for (Accum accum : accums) {
                merged.sum += accum.sum;
                merged.count += accum.count;
            }
            return merged;
        }
        public Double extractOutput(Accum accum) {
            // return ((double) accum.sum) / accum.count;
            return accum.sum;
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

    private static class serviceQueryCollectionFn extends DoFn<LogMessage, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LogMessage l = c.element();
            ServiceControlExtension sce = l.getServiceControlLogEntry().getProtoContent();
            String serviceName = sce.getServiceName();
            c.output(KV.of(serviceName, 1.0));
        }
    }

    private static class serviceErrorsCountCollectionFn extends DoFn<LogMessage, KV<String, Double>> {
        private String errorType;

        public serviceErrorsCountCollectionFn(String errorType) {
            this.errorType = errorType;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LogMessage l = c.element();
            ServiceControlExtension sce = l.getServiceControlLogEntry().getProtoContent();
            String serviceName = sce.getServiceName();
            Double errorsCount = 0.0;
            if(errorType.equals("check")) {
                errorsCount = sce.getCheckErrorsCount() + 0.0;
            } else if(errorType.equals("quota")) {
                errorsCount = sce.getQuotaErrorsCount() + 0.0;
            } else {
                // throw exception
            }
            c.output(KV.of(serviceName, errorsCount));
        }
    }

    private static class TimestampAndEntityKeyedFieldValueFn extends DoFn<LogMessage, KV<String, Double>> {
        private long interval;
        private String entity;
        private String field;

        public TimestampAndEntityKeyedFieldValueFn(long interval, String entity, String field) {
            this.interval = interval;
            this.entity   = entity;
            this.field    = field;
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

            if (entity.equals("service")) {
                String serviceName = sce.getServiceName();
                Double fieldValue = -1.0;
                if (field.equals("query")) {
                    fieldValue = 1.0;
                } else if (field.equals("check")) {
                    fieldValue = sce.getCheckErrorsCount() + 0.0;
                } else if (field.equals("quota")) {
                    fieldValue = sce.getQuotaErrorsCount() + 0.0;
                } else if (field.equals("status")) {
                    int code = sce.getStatus().getCode();
                    fieldValue = (code == 0) ? 0.0 : 1.0; // 1.0 for positive 
                } else {
                    //TODO: throw exception
                }
                // if (field.equals("status") && fieldValue == 0.0) { //
                //     // do not output
                // } else {
                    c.output(KV.of(timestamp + "-" + serviceName, fieldValue));
                // }
            } else if (entity.equals("consumer")) {
                if (!field.equals("query")) {
                    //TODO: throw exception
                }
                for (OperationInfo op : sce.getOperationsList()) {
                    String consumerProjectId = op.getConsumerProjectId();
                    c.output(KV.of(timestamp + "-" + consumerProjectId, 1.0));
                }
            } else { // accpet other types of entities?
                //TODO: throw exception
            }

        }
    }

    private static class EntityKeyedTimestampFieldValueFn extends DoFn<KV<String, Double>, KV<String, KV<String, Double>>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, Double> kv = c.element();
            String timestampEntity = kv.getKey();
            Double fieldValue = kv.getValue();

            String timestampSeconds = timestampEntity.substring(0, 10);
            String entityName = timestampEntity.substring(10+1);

            String key0 = entityName;
            String key1 = timestampSeconds;
            Double value1 = fieldValue;
            c.output(KV.of(key0, KV.of(key1, value1)));
        }
    }

    private static class EntityKeyedFieldValueFn extends DoFn<KV<String, Double>, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            KV<String, Double> kv = c.element();
            String timestampEntity = kv.getKey();
            Double fieldValue = kv.getValue();

            // String timestampSeconds = timestampEntity.substring(0, 10);
            String entityName = timestampEntity.substring(10+1);

            String key0 = entityName;
            // String key1 = timestampSeconds;
            Double value1 = fieldValue;
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
            Double value = (max - min) / sum;
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

    private static class ServicesTableRowFn extends DoFn<LogMessage, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LogMessage l = c.element();

            ServiceControlExtension sce = l.getServiceControlLogEntry().getProtoContent();
            long ts = l.getTimeUsec();

            TableRow row = new TableRow()
              .set("serviceName", sce.getServiceName())
              .set("timeUsec", ts + "")
              .set("statusMessage", sce.getStatus().getMessage());

            c.output(row);
        }
    }

    private static class OperationsTableRowFn extends DoFn<LogMessage, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            LogMessage l = c.element();

            ServiceControlExtension sce = l.getServiceControlLogEntry().getProtoContent();
            String serviceName = sce.getServiceName();
            List<OperationInfo> ops = sce.getOperationsList();
            for (OperationInfo op : ops) {
                TableRow row = new TableRow()
                    .set("operationId", op.getOperationId())
                    .set("operationName", op.getOperationName())
                    .set("associatedServiceName", serviceName)
                    .set("consumerProjectId", op.getConsumerProjectId())
                    .set("startSeconds", op.getStartTime().getSeconds() + "")
                    .set("startNanos", op.getStartTime().getNanos() + "")
                    .set("endSeconds", op.getEndTime().getSeconds() + "")
                    .set("endNanos", op.getEndTime().getNanos() + "");
                c.output(row);
            }

        }
    }

    private static class TimestampEntityFieldTableRowFn extends DoFn<KV<String, Double>, TableRow> {
        private String entityType;
        private String fieldName;

        public TimestampEntityFieldTableRowFn(String entityType, String fieldName) {
            this.entityType = entityType;
            this.fieldName  = fieldName;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, Double> kv = c.element();
            String timestampEntity = kv.getKey();
            Double fieldValue = kv.getValue();

            String timestampSeconds = timestampEntity.substring(0, 10);
            String entityName = timestampEntity.substring(10+1);

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

    private static PCollection<KV<String, Double>> getTimestampEntityFieldsCombinedWithinEachInterval(PCollection<LogMessage> allLogMessages, long interval, String entity, String field) {
        PCollection<KV<String, Double>> res = allLogMessages
            .apply("", ParDo.of(new TimestampAndEntityKeyedFieldValueFn(interval, entity, field)))
            .apply(Combine.<String, Double, Double>perKey(Sum.ofDoubles())) // .apply(Sum.<String>doublesPerKey())
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn("timestamp-" + entity + "-" + field)));
        return res;
    }

    private static Map<String, PCollection<KV<String, Double>>> getMinMaxSum(PCollection<KV<String, Double>> timestampEntityFields, String entity, String field) {
        PCollection<KV<String, Double>> entityField = timestampEntityFields
            .apply("", ParDo.of(new EntityKeyedFieldValueFn()));

        PCollection<KV<String, Double>> min = entityField
            .apply(Min.<String>doublesPerKey()) 
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(entity + "-" + field + " (Min)")));

        PCollection<KV<String, Double>> max = entityField
            .apply(Max.<String>doublesPerKey()) 
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(entity + "-" + field + " (Max)")));

        PCollection<KV<String, Double>> sum = entityField
            .apply(Combine.<String, Double, Double>perKey(Sum.ofDoubles())) 
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(entity + "-" + field + " (Sum)")));

        Map<String, PCollection<KV<String, Double>>> res = new HashMap<String, PCollection<KV<String, Double>>>();
        res.put("min", min);
        res.put("max", max);
        res.put("sum", sum);
        return res;
    }

    private static PCollection<KV<String, Double>> getDeviation(Map<String, PCollection<KV<String, Double>>> entityFieldMinMaxSum, String entity, String field) {
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
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(entity + "-" + field + " (Dev)")));

        return res;
    }

    private static PCollection<KV<String, Double>> getRatio(PCollection<KV<String, Double>> entityQuerySum, PCollection<KV<String, Double>> entityFieldSum, String entity, String field) {
        final TupleTag<Double> querySumTag = new TupleTag<Double>();
        final TupleTag<Double> fieldSumTag = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(querySumTag,  entityQuerySum)
            .and(fieldSumTag, entityFieldSum)
            .apply(CoGroupByKey.<String>create());

        PCollection<KV<String, Double>> res = joined
            .apply("", ParDo.of(new CalculateRatioFn(querySumTag, fieldSumTag)))
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn(entity + "-" + field + " (Ratio)")));

        return res;
    }

    private static boolean writeTimestampEntityFieldToBigQuery(PCollection<KV<String, Double>> timestampEntityFields, String entityType, String fieldName, String bqTempLocation, String tableName, String tableSchema, BigQueryIO.Write.WriteDisposition writeDisposition) {
        timestampEntityFields.apply("", ParDo.of(new TimestampEntityFieldTableRowFn(entityType, fieldName)))
            .apply("ToBigQuery", BigQueryIO.writeTableRows()
                .to(tableName)
                .withSchema(TableRowOutputTransform.createTableSchema(tableSchema))
                .withCustomGcsTempLocation(StaticValueProvider.of(bqTempLocation))
                .withWriteDisposition(writeDisposition) // WRITE_APPEND
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return true;
    }

    private static boolean writeServiceFieldStatsToBigQuery(Map<String, PCollection<KV<String, Double>>> serviceFieldStats, String bqTempLocation, String tableName, String tableSchema) {
        final TupleTag<Double> querySumTag  = new TupleTag<Double>();
        final TupleTag<Double> checkSumTag  = new TupleTag<Double>();
        final TupleTag<Double> quotaSumTag  = new TupleTag<Double>();
        final TupleTag<Double> statusSumTag = new TupleTag<Double>();
        final TupleTag<Double> queryPerIntervalTag  = new TupleTag<Double>();
        final TupleTag<Double> queryDevTag  = new TupleTag<Double>();
        final TupleTag<Double> checkRatioTag  = new TupleTag<Double>();
        final TupleTag<Double> quotaRatioTag  = new TupleTag<Double>();
        final TupleTag<Double> statusRatioTag = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
            .of(querySumTag,   serviceFieldStats.get("querySum"))
            .and(checkSumTag,  serviceFieldStats.get("checkSum"))
            .and(quotaSumTag,  serviceFieldStats.get("quotaSum"))
            .and(statusSumTag, serviceFieldStats.get("statusSum"))
            .and(queryPerIntervalTag,  serviceFieldStats.get("queryPerInterval"))
            .and(queryDevTag,  serviceFieldStats.get("queryDev"))
            .and(checkRatioTag,  serviceFieldStats.get("checkRatio"))
            .and(quotaRatioTag,  serviceFieldStats.get("quotaRatio"))
            .and(statusRatioTag, serviceFieldStats.get("statusRatio"))
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

        // ref: https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/extensions/protobuf/ProtoCoder.html
        ProtoCoder<ServiceControlLogEntry> coder = ProtoCoder.of(ServiceControlLogEntry.class).withExtensionsFrom(ServiceExtensionProtos.class);
        // if (options.isStreaming()) {
        //     outputWithTimestamp = false;
        //     allLogs = p.apply("logsPubSubRead", PubsubIO.readProtos(ServiceControlLogEntry.class).fromSubscription(options.getLogSource()))
        //                 .setCoder(coder);
        // } else {
        //     outputWithTimestamp = true;
        //     allLogs = p.apply("logsTextRead", TextIO.read().from(options.getLogSource()).withCoder(ProtoCoder.of(ServiceControlLogEntry.class)))
        //                 .setCoder(coder);
        // }
        // outputWithTimestamp = true;

        // filepattern = "input/sample_5l.txt"; // "gs://alchemy-logdata/sample_5line.txt"; 
        System.out.println("Input filepattern: " + filepattern + "\n");

        // Debug: java.security.InvalidAlgorithmParameterException: the trustAnchors parameter must be non-empty
        // https://stackoverflow.com/questions/6784463/error-trustanchors-parameter-must-be-non-empty
        // https://medium.com/@gustavocalcaterra/debugging-yet-another-ssl-tls-error-the-trustanchors-parameter-must-be-non-empty-7dd9cb300f43
        // https://askubuntu.com/questions/1004745/java-cant-find-cacerts
        // https://github.com/anapsix/docker-alpine-java/issues/27
        // https://stackoverflow.com/questions/17935619/what-is-difference-between-cacerts-and-keystore
        PCollection<ServiceControlLogEntry> allLogs = p.apply("logsTextRead", TextIO.read().from(filepattern))
            .apply("stringParsedToProtobuf", ParDo.of(new ParseStringToProtobufFn("json")));

        /* (2) Transform "allLogs" PCollection<String> to PCollection<LogMessage> by adding timestamp */
        PCollection<LogMessage> allLogMessages = allLogs
            .apply("allLogsToLogMessage", ParDo.of(new EmitLogMessageFn(outputWithTimestamp)));

        /* (3) Apply windowing scheme */
        // PCollection<LogMessage> allLogMessagesBySecond = allLogMessages
        //     .apply("allLogMessageToSecondly", Window.<LogMessage>into(FixedWindows.of(Duration.standardSeconds(1)))); // one-sec duration

        // PCollection<KV<String,Double>> serviceQueryCollection = allLogMessages
        //     .apply("logMessageToServiceQuery", ParDo.of(new serviceQueryCollectionFn()))
        //     .apply(Sum.<String>doublesPerKey()) // .apply(Combine.<String, Double, Double>perKey(Sum.ofDoubles()))
        //     .apply("Print", ParDo.of(new PrintKVStringDoubleFn("Service Query")));
        // PCollectionView<Map<String,Double>> output = serviceQueryCollection.apply(View.<String,Double>asMap());

        /* (4) Aggregate interesting fields */
        PCollection<KV<String, Double>> timestampServiceQueries = getTimestampEntityFieldsCombinedWithinEachInterval(allLogMessages, timeInterval, "service", "query");
        PCollection<KV<String, Double>> timestampServiceChecks  = getTimestampEntityFieldsCombinedWithinEachInterval(allLogMessages, timeInterval, "service", "check");
        PCollection<KV<String, Double>> timestampServiceQuotas  = getTimestampEntityFieldsCombinedWithinEachInterval(allLogMessages, timeInterval, "service", "quota");
        PCollection<KV<String, Double>> timestampServiceStatus  = getTimestampEntityFieldsCombinedWithinEachInterval(allLogMessages, timeInterval, "service", "status");

        Map<String, PCollection<KV<String, Double>>> serviceQueryMinMaxSum  = getMinMaxSum(timestampServiceQueries, "service", "query");
        Map<String, PCollection<KV<String, Double>>> serviceCheckMinMaxSum  = getMinMaxSum(timestampServiceChecks,  "service", "check");
        Map<String, PCollection<KV<String, Double>>> serviceQuotaMinMaxSum  = getMinMaxSum(timestampServiceQuotas,  "service", "quota");
        Map<String, PCollection<KV<String, Double>>> serviceStatusMinMaxSum = getMinMaxSum(timestampServiceStatus,  "service", "status");

        PCollection<KV<String, Double>> serviceQueryPerInterval = serviceQueryMinMaxSum.get("sum")
            .apply("", ParDo.of(new CalculateMeanPerIntervalFn(timeIntervalCount)))
            .apply("Print", ParDo.of(new PrintKVStringDoubleFn("service" + "-" + "query" + " (PerInterval)")));;

        PCollection<KV<String, Double>> serviceQueryDev = getDeviation(serviceQueryMinMaxSum, "service", "query");

        PCollection<KV<String, Double>> serviceCheckRatio  = getRatio(serviceQueryMinMaxSum.get("sum"), serviceCheckMinMaxSum.get("sum"),  "service", "check");
        PCollection<KV<String, Double>> serviceQuotaRatio  = getRatio(serviceQueryMinMaxSum.get("sum"), serviceQuotaMinMaxSum.get("sum"),  "service", "quota");
        PCollection<KV<String, Double>> serviceStatusRatio = getRatio(serviceQueryMinMaxSum.get("sum"), serviceStatusMinMaxSum.get("sum"), "service", "status");

        /* (5) Store to BigQuery */
        System.out.println("GCS temp location to store temp files for BigQuery: " + bqTempLocation + "\n");

        // allLogMessages.apply("servicesTableRow", ParDo.of(new ServicesTableRowFn()))
        //     .apply("servicesToBigQuery", BigQueryIO.writeTableRows()
        //         .to(options.getServicesTableName())
        //         .withSchema(TableRowOutputTransform.createTableSchema(options.getServicesTableSchema()))
        //         .withCustomGcsTempLocation(StaticValueProvider.of(options.getBQTempLocation()))
        //         .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
        //         .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        writeTimestampEntityFieldToBigQuery(timestampServiceQueries, "service", "query", 
                                            bqTempLocation, 
                                            options.getTimestampEntityFieldTableName(), 
                                            options.getTimestampEntityFieldTableSchema(),
                                            BigQueryIO.Write.WriteDisposition.WRITE_APPEND); //
        
        writeTimestampEntityFieldToBigQuery(timestampServiceChecks,  "service", "check", 
                                            bqTempLocation, 
                                            options.getTimestampEntityFieldTableName(), 
                                            options.getTimestampEntityFieldTableSchema(),
                                            BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
        
        writeTimestampEntityFieldToBigQuery(timestampServiceQuotas,  "service", "quota", 
                                            bqTempLocation, 
                                            options.getTimestampEntityFieldTableName(), 
                                            options.getTimestampEntityFieldTableSchema(),
                                            BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
        
        writeTimestampEntityFieldToBigQuery(timestampServiceStatus,  "service", "status", 
                                            bqTempLocation, 
                                            options.getTimestampEntityFieldTableName(), 
                                            options.getTimestampEntityFieldTableSchema(),
                                            BigQueryIO.Write.WriteDisposition.WRITE_APPEND);

        Map<String, PCollection<KV<String, Double>>> serviceFieldStats = new HashMap<String, PCollection<KV<String, Double>>>();
        serviceFieldStats.put("querySum",         serviceQueryMinMaxSum.get("sum"));
        serviceFieldStats.put("checkSum",         serviceCheckMinMaxSum.get("sum"));
        serviceFieldStats.put("quotaSum",         serviceQuotaMinMaxSum.get("sum"));
        serviceFieldStats.put("statusSum",        serviceStatusMinMaxSum.get("sum"));
        serviceFieldStats.put("queryPerInterval", serviceQueryPerInterval);
        serviceFieldStats.put("queryDev",         serviceQueryDev);
        serviceFieldStats.put("checkRatio",       serviceCheckRatio);
        serviceFieldStats.put("quotaRatio",       serviceQuotaRatio);
        serviceFieldStats.put("statusRatio",      serviceStatusRatio);
        writeServiceFieldStatsToBigQuery(serviceFieldStats, 
                                            bqTempLocation,
                                            options.getServiceFieldStatsTableName(), 
                                            options.getServiceFieldStatsTableSchema());

        System.out.println("[1] Run the pipeline...\n");
        PipelineResult r = p.run();

        LOG.info(r.toString());
        System.out.println("\nPipeline result: \n" + r.toString() + '\n');
    }
}