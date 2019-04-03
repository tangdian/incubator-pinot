# Introduction to ThirdEye
[![Build Status](https://travis-ci.org/linkedin/pinot.svg?branch=master)](https://travis-ci.org/linkedin/pinot) [![license](https://img.shields.io/github/license/linkedin/pinot.svg)](LICENSE)

ThirdEye is an integrated tool for realtime monitoring of time series and interactive root-cause analysis. It enables anyone inside an organization to collaborate on effective identification and analysis of deviations in business and system metrics. ThirdEye supports the entire workflow from anomaly detection, over root-cause analysis, to issue resolution and post-mortem reporting.

## What is it for? (key features)

Online monitoring and analysis of business and system metrics from multiple data sources. ThirdEye comes batteries included for both detection and analysis use cases. It aims to minimize the Mean-Time-To-Detection (MTTD) and Mean-Time-To-Recovery (MTTR) of production issues. ThirdEye improves its detection and analysis performance over time from incremental user feedback.

**Detection**
* Detection toolkit based on business rules and exponential smoothing
* Realtime monitoring of high-dimensional time series
* Native support for seasonality and permanent change points in time series
* Email alerts with 1-click feedback for automated tuning of detection algorithms

**Root-Cause Analysis**
* Collaborative root-cause analysis dashboards
* Interactive slice-and-dice of data, correlation analysis, and event identification
* Reporting and archiving tools for anomalies and analyses
* Knowledge graph construction over time from user feedback

**Integration**
* Connectors for continuous time series data from Pinot, Presto, MySQL and CSV
* Connectors for discrete event data sources, such as holidays from Google calendar
* Plugin support for detection and analysis components

## What isn't it? (limitations)

ThirdEye maintains a dedicated meta-data store to capture data sources, anomalies, and relationships between entities but does not store raw time series data. It relies on systems such as Pinot, RocksDB, and Kafka to obtain both realtime and historic time series data.

ThirdEye does not replace your issue tracker - it integrates with it. ThirdEye supports collaboration but focuses on the data-integration aspect of anomaly detection and root-cause analysis. After all, your organization probably already has a well-oiled issue resolution process that we don't want to disrupt.

ThirdEye is not a generic dashboard builder toolkit. ThirdEye attempts to bring overview data from different sources into one single place on-demand. In-depth data about events, such as A/B experiments and deployments, should be kept in their respective systems. ThirdEye can link to these directly.

## Quick start

ThirdEye supports an interactive demo mode for the analysis dashboard. These steps will guide you to get started.

### 1: Prerequisites

You'll need Java 8+, Maven 3+, and NPM 3.10+


### 2: Build ThirdEye

```
git clone https://github.com/linkedin/pinot.git
cd pinot/thirdeye
chmod +x install.sh run-frontend.sh run-backend.sh reset.sh
./install.sh
```

Note: The build of thirdeye-frontend may take several minutes


### 3: Run ThirdEye frontend

```
./run-frontend.sh
```


### 4: Start an analysis

Point your favorite browser to

```
http://localhost:1426/app/#/rootcause?metricId=1
```

Note: ThirdEye in demo mode will accept any credentials


### 5: Have fun

Available metrics in demo mode are:
* business::puchases
* business::revenue
* tracking::adImpressions
* tracking::pageViews

Note: These metrics are regenerated randomly every time you launch ThirdEye in demo mode

We also have 2 real world metric with seasonality in H2 database, for detection experimentation:

* H2::daily
* H2::hourly


### 6: Shutdown

You can stop the ThirdEye dashboard server anytime by pressing **Ctrl + C** in the terminal

## Start ThirdEye with MySQL / Presto

ThirdEye now supports data from MySQL and Presto!

### 1. Set the config

#### MySQL Config

Add your MySQL database URL and credentials in `thirdeye-pinot/config/datasources/data-sources-config.yml`.
You will be able to add multiple databases with multiple credentials, as follows:

```
dataSourceConfigs:
  - className: org.apache.pinot.thirdeye.datasource.sql.SqlThirdEyeDataSource
    properties:
      MySQL:
        - db:
            <dbname1>: jdbc:mysql://<db url1>
            <dbname2>: jdbc:mysql://<db url2>
          user: <username>
          password: <password>
         - db:
            <dbname3>: jdbc:mysql://<db url3>
            <dbname4>: jdbc:mysql://<db url4>
          user: <username2>
          password: <password2>
```
Note: the `dbname` here is an arbitrary name that you want to name it. 
In `dburl`, you still need to include the specific database you are using. For example:
```
dataSourceConfigs:
  - className: org.apache.pinot.thirdeye.datasource.sql.SqlThirdEyeDataSource
    properties:
      MySQL:
        - db:
            te: jdbc:mysql://localhost:3306/thirdeye
          user: root
          password: ""
```

#### Presto Config

Similar to MySQL config, in in `thirdeye-pinot/config/datasources/data-sources-config.yml`.

```
dataSourceConfigs:
  - className: org.apache.pinot.thirdeye.datasource.sql.SqlThirdEyeDataSource
    properties:
      Presto:
        - db:
            <dbname1>: jdbc:presto://<db url1>
            <dbname2>: jdbc:presto://<db url2>
          user: <username>
          password: <password>
         - db:
            <dbname3>: jdbc:presto://<db url3>
            <dbname4>: jdbc:presto://<db url4>
          user: <username2>
          password: <password2>
```

### 2. Run ThirdEye frontend

```
./run-frontend.sh
```


### 3. Import metric from Presto/MySQL
No credential is needed for login. 
Click `Create Alert` on top right of the page, click `Import a Metric from SQL` link under `Define detection configuration`.
Fill in the form which includes the following fields, and click Import Metrics.
 
`Table Name`: For Presto, it is the Presto table name, including all schema prefixes. For MySQL it is just the table name.

`Time column`: Column name that contains the time.

`Timezone`: Timezone of the time column.

`Time Format`: Format of the time column.

`Time Granularity`: The granularity of your metric. For example, daily data should choose 1DAYS. 
Hourly data should choose 1HOURS.

`Dimensions`: Add dimensions and fill in the name of the dimension

`Metrics`: Add metrics and fill in the name and the aggregation method on the dimension when it is being aggregated by time.

For example:

![image](https://user-images.githubusercontent.com/11586489/56252038-cb974880-606a-11e9-9213-a06bfa533826.png)

### 4: Start an analysis

Point your favorite browser to

```
http://localhost:1426/app/#/rootcause
```

and type any data set or metric name (fragment) in the search box. Auto-complete will now list the names of matching metrics. Select any metric to start an investigation.


## Start ThirdEye with Pinot

### 0: Prerequisites

Run through the **Quick Start** guide and shut down the frontend server process.


### 1: Update the data sources configuration

Insert the connector configuration for Pinot in `thirdeye-pinot/config/data-sources/data-sources-config.yml`. Your config should look like this:

```
dataSourceConfigs:
  - className: com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSource
    properties:
        zookeeperUrl: 'myZkCluster.myDomain:12913/pinot-cluster'
        clusterName: 'myDemoCluster'
        controllerConnectionScheme: 'https'
        controllerHost: 'myPinotController.myDomain'
        controllerPort: 10611
        cacheLoaderClassName: com.linkedin.thirdeye.datasource.pinot.PinotControllerResponseCacheLoader
    metadataSourceConfigs:
      - className: com.linkedin.thirdeye.auto.onboard.AutoOnboardPinotMetadataSource

  - className: com.linkedin.thirdeye.datasource.mock.MockThirdEyeDataSource
    ...
```

Note: You'll have to change the host names and port numbers according to your setup


### 2: Enable Pinot auto-onboarding

Update the `thirdeye-pinot/config/detector.yml` file to enable auto onboarding of pinot data sets.

```
autoload: true
```


### 3: Run the backend worker to load all supported Pinot data sets

```
./run-backend.sh
```

Note: This process may take some time. The worker process will print log messages for each data set schema being processed. Schemas must contain a `timeFieldSpec` in order for ThirdEye to onboard it automatically


### 4: Stop the backend worker

By pressing **Ctrl-C** in the terminal


### 5: Run ThirdEye frontend

```
./run-frontend.sh
```


### 6: Start an analysis

Point your favorite browser to

```
http://localhost:1426/app/#/rootcause
```

and type any data set or metric name (fragment) in the search box. Auto-complete will now list the names of matching metrics. Select any metric to start an investigation.

## Setting up alert


**Welcome to ThirdEye**


## ThirdEye for production settings

ThirdEye relies on a central meta data store to coordinate its workers and frontend processes. The first step towards moving ThirdEye into production should therefore be the setup of a dedicated (MySQL) database instance. You can use the `thirdeye-pinot/src/resources/schema/create-schema.sql` script to create your tables. Then, update the `thirdeye-pinot/config/persistence.yml` file with path and credentials. Once you have a dedicated database instance, you can run backend and frontend servers in parallel. 

The next step could be the configuration of the holiday auto-loader. The holiday auto loader connects to the Google Calendar API. Once you obtain an API token, place it in `thirdeye-pinot/config/holiday-loader-key.json` and in `thirdeye-pinot/config/detector.yml` set `holidayEventsLoader: true`. Once the backend worker is restarted, it will periodically update the local cache of holiday events for ThirdEye's detection and Root-Cause Analysis components.


## More information

More information coming. In the meantime, use your favorite web search engine to search for 'Pinot ThirdEye' articles and blog posts.

