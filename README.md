# Log Analysis Tool For Service Infrastructure

This is not an officially supported Google product.

This repository contains code for summer intern project for Service Infrastructure log analysis tool.

project description:
ServiceControl is the central gateway for admission control and telemetry reporting. ServiceControl has 15M QPS and serving thousands of Google services as well as third party services.

This project is to build a framework to analyze ServiceControl log to detect patterns and analyze anomalies.

The specific tasks are as follows:
1) Build a framework so that we can mine ServiceControl log data.
2) Detect patterns to define the normal service behavior.
3) Detect anomalies so that we can notice abnormal behaviors.

This tool will be running against logs extracted from non-prod environment with all PII field masked.
