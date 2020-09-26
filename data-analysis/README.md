With Dataflow processing log data and extracting features, follow-up data analysis is performed in AI Platform:
- import data/features from BigQuery to Jupyter notebook instances;
- visualize data and select features;
- apply machine learning approaches to detect anomalies;
- deploy trained models;
- make predictions with deployed models.

Two examples are discussed in this part: 
- time range of log data is 1 second, aggregation interval is 1 second, and the model uses a point data view;
- time range of log data is about 1 week, aggregation interval is 5 minutes, and the model uses a time series data view.

****
The overview of doscussed models in our framework (not all of them are shown in the notebooks) is shown in the following figure:
![high-level flowchart](https://github.com/googleinterns/log-tool-for-service-infrastructure/blob/master/docs/images/Models.png?raw=true)
