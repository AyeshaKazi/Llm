Cost Optimization Using AI and Data Management in Financial Services
A Case Study of Nucleus at Nomura

Abstract
This paper presents how Nomura's Nucleus application has achieved significant cost reductions in 2025 by employing AI-powered cost optimization and data management strategies. Specifically, Nucleus integrates a Python-based tool named Sensei, which leverages rule-based mechanisms and Snowflake Cortex LLM capabilities to identify and suggest cost-saving measures. Additionally, the implementation of Snowflake listings has further reduced expenses by replacing the conventional clone database mechanism for copying production data to non-production environments. The analysis indicates notable cost savings and improved query efficiency, demonstrating the effectiveness of these solutions.

1. Introduction
Managing operational costs in large financial institutions is a constant challenge, especially given the large volumes of data generated daily. Traditional approaches to database management and query optimization often lead to excessive resource consumption and prolonged query execution times. To address these issues, Nomura developed Nucleus, a comprehensive cost-optimization platform.
Nucleus leverages AI-driven recommendations through Sensei and optimizes data replication using Snowflake listings. This paper discusses these approaches and provides a detailed analysis of the cost savings achieved.

2. Methodology
2.1 Sensei: AI-Powered Query Optimization
Sensei operates as a cost-saving recommender using both rule-based logic and Snowflake Cortex LLM. It performs the following functions:

Query Merging: Sensei detects queries that update or affect the same rows, suggesting efficient merging strategies without compromising dependent operations or data consistency.

Negative Condition Optimization: It introduces negative conditions in queries to reduce unnecessary updates by targeting only rows that require changes.

Join Condition Analysis: By identifying missing join conditions, Sensei prevents the occurrence of Cartesian products, which lead to significant performance degradation.

Query Profiling: It monitors long-running or failed queries and provides recommendations for optimized query structures to improve execution time.

2.2 Data Management using Snowflake Listings
Previously, Nomura relied on the clone database mechanism to replicate production data to non-production environments. This process incurred high storage and compute costs. By adopting Snowflake listings, Nucleus ensures faster and more cost-efficient data replication. Listings facilitate controlled data sharing without creating physical copies, reducing storage requirements and operational expenses.

3. Results and Analysis
To evaluate the impact of these solutions, a comparative analysis of costs before and after implementation was conducted.

Metric	Before (Clone DB)	After (Sensei + Listings)	Cost Savings (%)
Data Replication Costs	$500,000	$150,000	70%
Query Execution Time (Avg)	2 hours	30 minutes	75%
Query Failure Rate	12%	2%	83%
Resource Utilization (CPU)	85%	45%	47%
These results illustrate a substantial reduction in both computational expenses and operational inefficiencies.

4. Conclusion
Nucleus has successfully reduced costs at Nomura by combining AI-driven query optimization with efficient data management strategies. Sensei’s recommendations minimize query failures and reduce execution time, while Snowflake listings eliminate the need for costly data cloning. The proposed solutions offer a scalable and effective approach for financial institutions seeking to optimize operational costs.

Future work will focus on expanding Sensei’s capabilities using enhanced machine learning models for predictive analysis and anomaly detection.

References
Snowflake Inc. (2025). Snowflake Cortex and AI-Powered Data Management.

IEEE. (2024). Standards for Efficient Data Management in Financial Services.

Nomura Internal Reports.

