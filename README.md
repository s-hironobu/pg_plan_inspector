pg_plan_inspector
=================

`pg_plan_inspector` is being developed as a framework to monitor and improve the performance of PostgreSQL using Machine Learning methods.

One movie is worth a thousand words.
I demonstrate that a tool provided by this framework shows the progress of a running query.
Watch this: [query-progress-example01.mp4](https://user-images.githubusercontent.com/7246769/126768623-d02a3bea-edb1-4776-adaf-ce871ce526e5.mp4)



https://user-images.githubusercontent.com/7246769/126768623-d02a3bea-edb1-4776-adaf-ce871ce526e5.mp4




This is a POC model, and the primary purposes of this framework are:

1. To implement an external module that monitors the state of the running queries. (Refer to [[3](#references)].)
2. To show that the PostgreSQL optimizer can be improved from providing feedback on the analysis of the executed plans.


To achieve these purposes, I am currently developing two modules: `pg_query_plan` and `plan_analyzer`.

![alt text](./img/fig-over-view-pg_plan_inspector.png "overview of pg_plan_inspector system")


The development of this framework has just begun.
The movie shown above is the result of one of the by-products of developing these modules.


#### Terminology

+ Executed plan  
An executed plan is a plan that contains both estimated and actual values, i.e. it is the same as the result of the EXPLAIN command with ANALYZE option.
+ Query plan  
A query plan is a snapshot of the executed plan, which is taken by the pg_query_plan module. It also contains both estimated and actual values, and the actual values are the snapshot values when it is taken.
+ Machine Learning  
Many methods I learned in statistics as a student are now classified as Machine Learning methods. Thanks to the new AI era, I can use the word `Machine Learning` without hesitation.

## Contents

1. [Supported Versions](#1-supported-versions)  
2. [Installation and Usage](#2-installation-and-usage)  
3. [Tentative Conclusion](#3-tentative-conclusion)  
4. [Future work](#4-future-work)  
5. [Related works](#5-related-works)  
6. [Limitations and Warning](#6-limitations-and-warning)  
[Version](#version)  
[References](#references)  
[Change Log](#change-log)


## 1. Supported Versions


This framework supports PostgreSQL versions 13 and 14.

## 2. Installation and Usage

This framework is composed of two modules: `pg_query_plan` and `plan_analyzer`.

The installations and their usages are described in [README-pg_query_plan.md](./README-pg_query_plan.md) and [README-plan_analyzer.md](./README-plan_analyzer.md), respectively.


## 3. Tentative Conclusion

### pg_query_plan

As shown in [README-pg_query_plan.md](./README-pg_query_plan.md), the pg_query_plan module can be monitored through the state of running queries.
In other words, the first main purpose has been achieved by this module.


### plan_analyzer

In [README-tools.md](./README-tools.md),
I introduced an example that a tool, called analyze.py, detects the functional dependency between the attributes of the table by applying a simple heuristics rule to the executed plans.
This is an ad hoc; however, at least it shows that the second main purpose is not wrong and is possible.


As shown in [query-progress-example01.mp4](https://user-images.githubusercontent.com/7246769/126768623-d02a3bea-edb1-4776-adaf-ce871ce526e5.mp4) and [README-plan_analyzer.md](./README-plan_analyzer.md), I have implemented the tool that shows the progress of a running query with some accuracy.
More precisely, the tool gives relatively accurate results if the Plan Rows estimated by the optimizer can be corrected by a linear regression model,
otherwise the results will be inaccurate, for example, if the distribution of the table data has changed considerably.
In addition, the linear regression for correction of the estimated rows is generally often inaccurate when hash or merge joins are included.
These causes are due to the validity limit of the linear regression model for the correction of the estimated rows.


Considering these, it is obvious that the essential task is to improve the cardinality estimation of the PostgreSQL optimizer.


## 4. Future Work

### Step 1

In the first step, I will implement a bridge software between the plan_analyzer module and PostgreSQL server to feedback the analysis results of the executed plan to the optimizer.

A naive approach is that the plan_analyzer module sends the regression parameters stored in the repository to the PostgreSQL optimizer, and the optimizer corrects the estimated Plan Rows using the regression parameters.
PostgreSQL fortunately already has a mechanism to intervene in the optimizer's processing and at least two modules have been made to improve the optimizer's processing for better results:
[pg_dbms_stats](https://github.com/ossc-db/pg_dbms_stats) and [pg_plan_advsr](https://github.com/ossc-db/pg_plan_advsr).


In addition, I will improve the algorithm of finding functional dependency in the first step.


##### Info:
`pg_plan_advsr` has already achieved one of the goals of this step using another approach.


### Step 2

The approach mentioned above works to a certain extent if the linear regression model for the correction of the estimated rows is valid, otherwise it will not work.


Recently, lot of research to improve DBMS functions using AI technology[[6](#references)] is being conducted,
and [hundreds of papers](https://scholar.google.com/scholar?hl=en&as_sdt=0,5&as_ylo=2015&as_yhi=2021&q=selectivity+OR+cardinality+estimation+deep+OR+machine+learning+database+planner+OR+optimizer) have been published on cardinality estimation.
These state-of-the-art methods[[7,9,8,1,2,5](#references)] are attempting to go beyond the traditional methods, such as using histograms.
As PostgreSQL uses a traditional method, it would be worthwhile to add ML methods to the optimizer.


This framework will be able to provide feedback on the differences between the estimated cardinality (Plan Rows) and the actual cardinality in Step 1;
therefore, the feedback can be used to improve learning.

![alt text](./img/fig-future-plan.png "future plan")



## 5. Related Works

Two projects have similar features to the pg_query_plan module.

1. [pg_query_state](https://github.com/postgrespro/pg_query_state) also shows the query plan on the working backend. However, this module needs to modify the PostgreSQL core by patching. On the other hand, pg_query_plan is a pure module and does not need to modify the core.
2. [pg_show_plans](https://github.com/cybertec-postgresql/pg_show_plans) shows the execution plans of all current running SQL statements. Unlike pg_query_plan and pg_query_state, pg_show_plans shows the execution plan that only contains the estimated values and not the actual values.


Query progress indicators were previously studied [[4](#references)]. However, it seems that it is not currently being extensively researched.


Two interesting demonstrations will be shown at [VLDB 2021: Demonstrations](http://vldb.org/2021/?program-schedule-demonstrations).
  + PostCENN: PostgreSQL with Machine Learning Models for Cardinality Estimation
  + DBMind: A Self-Driving Platform in openGauss


## 6. Limitations and Warning

### 6.1. Limitations

1. These modules cannot work if the query handles the partitioned tables.
2. These modules cannot work if the query has custom scans or foreign scans.
3. These modules do not consider the effects of triggers.
4. The pg_query_plan module is currently not available on standby.

### 6.2. Warning

Use this framework at your own risk.

+ With the pg_query_plan module, the performance of query processing is reduced by a few percent due to the overhead of data collection.
+ The values provided by the plan_analyzer module are estimates, not guaranteed to be 100% accurate.


## Version

Version 0.1 (POC model)

## References

[1] Andreas Kipf, et al. "[Learned Cardinalities:Estimating Correlated Joins with Deep Learning](http://cidrdb.org/cidr2019/papers/p101-kipf-cidr19.pdf)". In CIDR, 2019.  
[2] Benjamin Hilprecht, et al. "[DeepDB: Learn from Data, not from Queries!](http://www.vldb.org/pvldb/vol13/p992-hilprecht.pdf)". Proceedings of the VLDB, Vol. 13, Issue 7, March 2020, pages 992–1005.  
[3] Lukas Fittl. "[What's Missing for Postgres Monitoring](https://www.pgcon.org/events/pgcon_2020/sessions/session/132/slides/49/Whats%20Missing%20for%20Postgres%20Monitoring.pdf)". PGCon 2020.  
[4] Patil L.V., and Mane Urmila P. "[Survey on SQL Query Progress Indicator](https://www.ijert.org/research/survey-on-sql-query-progress-indicator-IJERTV2IS3286.pdf)". International Journal of Engineering Research & Technology (IJERT) Vol. 2, Issue 3, March 2013.  
[5] Rong Zhu, Ziniu Wu, et al. "[FLAT: Fast, Lightweight and Accurate Method for Cardinality Estimation](https://arxiv.org/pdf/2011.09022.pdf)". arXiv preprint arXiv:2011.09022(2020).  
[6] X. Zhou, C. Chai, G. Li, and J. Sun. "[Database Meets Artificial Intelligence: A Survey](https://www.researchgate.net/publication/341427551_Database_Meets_Artificial_Intelligence_A_Survey)". TKDE, 2020.  
[7] Xiaoying Wang, et al. "[Are We Ready For Learned Cardinality Estimation?](https://arxiv.org/abs/2012.06743)". arXiv:2012.14743, December 2020.  
[8] Ziniu Wu, et al. "[BayesCard: Revitalizing Bayesian Networks for Cardinality Estimation](https://arxiv.org/pdf/2012.14743.pdf)". arXiv:2012.14743, December 2020.  
[9] Zongheng Yang, et al. "[NeuroCard: One Cardinality Estimator for All Tables](https://vldb.org/pvldb/vol14/p61-yang.pdf)". Proceedings of the VLDB, Vol. 14, Issue 1, September 2020.


## Change Log

 - 28th July 2021: Version 0.1 Released.

