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
// TODO: Due to time limitation, this file is not complete, and can only be used as a reference.

package com.google.cloud.solutions;

import com.google.cloud.solutions.LogAnalyticsPipeline;

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
import org.apache.beam.sdk.options.PipelineOptions;
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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;


public class LogAnalyticsPipelineTestTemp {

    @Rule
    public final TestPipeline p = createPipeline();

    private static TestPipeline createPipeline() {
        final PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DataflowPipelineRunner.class);
        options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
        return TestPipeline.fromOptions(options);
    }


    @Test
    public void testLogAnalyticsPipeline() {
        // test case 
        String filepattern = "gs://alchemy-logdata/input/Sample_log_*";
        long timeInterval = 1;
        long timeIntervalCount = 10;

        /* (1) Read data (raw string?) as PCollection<MyMessage> from GCS */
        PCollection<ServiceControlLogEntry> allLogs = p.apply("logsTextRead", TextIO.read().from(filepattern))
            .apply("stringParsedToProtobuf", ParDo.of(new LogAnalyticsPipeline.ParseStringToProtobufFn("json")));

        /* (2) Transform "allLogs" PCollection<String> to PCollection<LogMessage> by adding timestamp */
        PCollection<LogMessage> allLogMessages = allLogs
            .apply("allLogsToLogMessage", ParDo.of(new LogAnalyticsPipeline.EmitLogMessageFn(true)));

        /* (3) Aggregate interesting fields for entities within each time interval, and calculate statistical information among all time intervals */
        PCollection<KV<String, Double>> timestampEntityFields = LogAnalyticsPipeline.getTimestampEntityFieldsCombinedWithinEachInterval(allLogMessages, timeInterval);

        Map<String, PCollection<KV<String, Double>>> entityFieldMinMaxSum  = LogAnalyticsPipeline.getMinMaxSum(timestampEntityFields);
        PCollection<KV<String, Double>> entityFieldSum = entityFieldMinMaxSum.get("sum");

        PCollection<KV<String, Double>> entityFieldPerInteval = LogAnalyticsPipeline.getMeanPerInterval(entityFieldSum, timeIntervalCount);

        PCollection<KV<String, Double>> entityFieldDev = LogAnalyticsPipeline.getDeviation(entityFieldMinMaxSum);

        PCollection<KV<String, Double>> serviceCheckRatio  = LogAnalyticsPipeline.getRatio(entityFieldSum, "service_", "check_");
        PCollection<KV<String, Double>> serviceQuotaRatio  = LogAnalyticsPipeline.getRatio(entityFieldSum, "service_", "quota_");
        PCollection<KV<String, Double>> serviceStatusRatio = LogAnalyticsPipeline.getRatio(entityFieldSum, "service_", "status");

        /* (4) Test with PAssert */
        List<KV<String, Double>> KV_ARRAY = new ArrayList<KV<String, Double>>();
        KV_ARRAY.add(KV.of("service_-query_-1593416510-staging-pubsub.sandbox.googleapis.com", 3.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416511-staging-pubsub.sandbox.googleapis.com", 2.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416512-staging-pubsub.sandbox.googleapis.com", 4.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416513-staging-pubsub.sandbox.googleapis.com", 13.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416514-staging-pubsub.sandbox.googleapis.com", 4.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416515-staging-pubsub.sandbox.googleapis.com", 4.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416516-staging-pubsub.sandbox.googleapis.com", 4.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416517-staging-pubsub.sandbox.googleapis.com", 3.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416518-staging-pubsub.sandbox.googleapis.com", 4.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416519-staging-pubsub.sandbox.googleapis.com", 3.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416510-staging-storage.sandbox.googleapis.com", 2.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416511-staging-storage.sandbox.googleapis.com", 3.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416512-staging-storage.sandbox.googleapis.com", 3.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416513-staging-storage.sandbox.googleapis.com", 31.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416514-staging-storage.sandbox.googleapis.com", 1.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416515-staging-storage.sandbox.googleapis.com", 3.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416516-staging-storage.sandbox.googleapis.com", 2.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416517-staging-storage.sandbox.googleapis.com", 4.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416518-staging-storage.sandbox.googleapis.com", 4.0));
        KV_ARRAY.add(KV.of("service_-query_-1593416519-staging-storage.sandbox.googleapis.com", 3.0));
        KV_ARRAY.add(KV.of("service_-status-1593416513-staging-pubsub.sandbox.googleapis.com", 2.0));
        KV_ARRAY.add(KV.of("service_-status-1593416514-staging-pubsub.sandbox.googleapis.com", 1.0));
        KV_ARRAY.add(KV.of("service_-status-1593416515-staging-pubsub.sandbox.googleapis.com", 1.0));
        KV_ARRAY.add(KV.of("service_-check_-1593416513-staging-pubsub.sandbox.googleapis.com", 3.0));
        KV_ARRAY.add(KV.of("service_-check_-1593416514-staging-pubsub.sandbox.googleapis.com", 2.0));
        KV_ARRAY.add(KV.of("service_-check_-1593416515-staging-pubsub.sandbox.googleapis.com", 3.0));
        //
        PAssert.that(timestampEntityFields).containsInAnyOrder(KV_ARRAY);

        //
        KV_ARRAY.clear();
        KV_ARRAY.add(KV.of("service_-query_-staging-pubsub.sandbox.googleapis.com", 2.0));
        KV_ARRAY.add(KV.of("service_-query_-staging-storage.sandbox.googleapis.com", 1.0));
        KV_ARRAY.add(KV.of("service_-status-staging-pubsub.sandbox.googleapis.com", 1.0));
        KV_ARRAY.add(KV.of("service_-check_-staging-pubsub.sandbox.googleapis.com", 2.0));
        //
        PAssert.that(entityFieldMinMaxSum.get("min")).containsInAnyOrder(KV_ARRAY);

        //
        KV_ARRAY.clear();
        KV_ARRAY.add(KV.of("service_-query_-staging-pubsub.sandbox.googleapis.com", 13.0));
        KV_ARRAY.add(KV.of("service_-query_-staging-storage.sandbox.googleapis.com", 31.0));
        KV_ARRAY.add(KV.of("service_-status-staging-pubsub.sandbox.googleapis.com", 2.0));
        KV_ARRAY.add(KV.of("service_-check_-staging-pubsub.sandbox.googleapis.com", 3.0));
        //
        PAssert.that(entityFieldMinMaxSum.get("max")).containsInAnyOrder(KV_ARRAY);

        //
        KV_ARRAY.clear();
        KV_ARRAY.add(KV.of("service_-query_-staging-pubsub.sandbox.googleapis.com", 44.0));
        KV_ARRAY.add(KV.of("service_-query_-staging-storage.sandbox.googleapis.com", 56.0));
        KV_ARRAY.add(KV.of("service_-status-staging-pubsub.sandbox.googleapis.com", 4.0));
        KV_ARRAY.add(KV.of("service_-check_-staging-pubsub.sandbox.googleapis.com", 8.0));
        //
        PAssert.that(entityFieldMinMaxSum.get("sum")).containsInAnyOrder(KV_ARRAY);

        //
        KV_ARRAY.clear();
        KV_ARRAY.add(KV.of("service_-query_-staging-pubsub.sandbox.googleapis.com", 4.4));
        KV_ARRAY.add(KV.of("service_-query_-staging-storage.sandbox.googleapis.com", 5.6));
        //
        PAssert.that(entityFieldPerInteval).containsInAnyOrder(KV_ARRAY);

        //
        KV_ARRAY.clear();
        KV_ARRAY.add(KV.of("service_-query_-staging-pubsub.sandbox.googleapis.com", 0.25));
        KV_ARRAY.add(KV.of("service_-query_-staging-storage.sandbox.googleapis.com", 0.54));
        //
        PAssert.that(entityFieldDev).containsInAnyOrder(KV_ARRAY);

        //
        KV_ARRAY.clear();
        KV_ARRAY.add(KV.of("service_-query_-staging-pubsub.sandbox.googleapis.com", 0.18));
        //
        PAssert.that(serviceCheckRatio).containsInAnyOrder(KV_ARRAY);

        //
        KV_ARRAY.clear();
        //
        PAssert.that(serviceQuotaRatio).containsInAnyOrder(KV_ARRAY);

        //
        KV_ARRAY.clear();
        KV_ARRAY.add(KV.of("service_-query_-staging-pubsub.sandbox.googleapis.com", 0.09));
        //
        PAssert.that(serviceStatusRatio).containsInAnyOrder(KV_ARRAY);

        p.run();

    }

    
}
