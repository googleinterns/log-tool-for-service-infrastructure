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

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;
import org.joda.time.Instant;

// import com.google.protobuf.timestamp;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import com.google.api.servicecontrol.log.ServiceExtensionProtos;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.ServiceControlLogEntry;
import com.google.api.servicecontrol.log.ServiceExtensionProtos.Timestamp;
import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
class LogMessage implements Serializable {
    @Nullable private long timeUsec;
    @Nullable private ServiceControlLogEntry serviceControlLogEntry;

    @SuppressWarnings("unused")
    public LogMessage() {}

    public LogMessage(long timeUsec,
                      ServiceControlLogEntry serviceControlLogEntry) {
        this.timeUsec = timeUsec;
        this.serviceControlLogEntry = serviceControlLogEntry;
    }

    public long getTimeUsec() {
        return this.timeUsec;
    }

    public ServiceControlLogEntry getServiceControlLogEntry() {
        return this.serviceControlLogEntry;
    }
}
