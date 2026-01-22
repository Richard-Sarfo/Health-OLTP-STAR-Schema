HealthTech Analytics Database Optimization Project

Overview
This project evaluated the impact of transforming a normalized OLTP (Online Transaction Processing) healthcare database into a dimensional star schema optimized for analytics. The goal was to improve performance for key business queries used in dashboards and reporting. The results were decisive: the star schema delivered a 13.9× overall performance improvement, reducing total execution time for four critical analytical queries from 641ms to 46ms.
The transformation demonstrates a core data engineering principle: database schemas must be designed around access patterns. OLTP schemas excel at transactional integrity and real-time updates, while star schemas are purpose-built for fast, repeatable analytical queries.
Why the Star Schema Is Faster
The performance gains came from three architectural shifts: simpler JOIN patterns, pre-computed metrics, and strategic denormalization.

1. Reduced JOIN Complexity
The OLTP schema relied on deep, multi-hop JOIN chains to enforce normalization and referential integrity. For example, revenue-by-specialty analysis required traversing billing → encounters → providers → specialties. Each billing record triggered multiple index lookups before aggregation could even begin.
The star schema replaced this with a hub-and-spoke design, where a central fact table connects directly to dimension tables. This flattened access path significantly reduced query planning and execution overhead. In some cases, entire tables (such as billing) were removed from query paths because their metrics were already aggregated into the fact table.
The key insight is that analytical workloads prioritize read efficiency, not write efficiency. Denormalization is therefore a feature, not a flaw.

2. Pre-Computed Metrics (ETL vs. Query-Time Cost)
In the OLTP system, every analytical query repeatedly performed the same calculations:
•	Formatting dates for grouping
•	Summing billing records
•	Calculating length of stay
•	Detecting 30-day readmissions via self-joins
When the same query logic runs dozens of times per day across dashboards and reports, this creates massive redundant computation.
The star schema shifted these calculations to the ETL (Extract, Transform, Load) process, computing them once per night and storing the results in the fact table. Examples include:
•	Monthly date attributes
•	Total allowed billing amounts
•	Diagnosis and procedure counts
•	A pre-computed is_readmission flag
This change turned expensive, repetitive query-time computation into a one-time batch cost. The most dramatic example was readmission analysis, which improved by 94×, transforming a self-join query into a simple boolean filter.

3. Strategic Denormalization
The design intentionally duplicated certain data to improve speed:
•	Date dimensions store pre-calculated calendar attributes
•	Fact tables store aggregated financial metrics
•	Derived indicators (like readmissions) are materialized
This increased storage usage by about 30%, but eliminated function calls, reduced JOINs, and enabled index-friendly queries. In analytical systems where reads vastly outnumber writes, this trade-off is overwhelmingly beneficial.

What Was Gained vs. What Was Lost

What We Gained
1.	Dramatic Performance Improvements
All tested queries ran 7×–188× faster, with two executing in sub-millisecond time. Dashboards shifted from noticeable lag to instant responsiveness.
2.	Scalability for Concurrent Users
The star schema supports roughly 14× more simultaneous users for the same database workload, enabling organization-wide adoption without performance degradation.
3.	Simpler, More Intuitive Queries
Analysts can write straightforward SQL using consistent JOIN patterns and pre-defined metrics, reducing errors and development time.
4.	Metric Consistency
Centralized, pre-aggregated calculations ensure that all users see the same numbers, eliminating discrepancies caused by inconsistent query logic.

What We Lost
1.	Increased Storage Usage
Redundant data and dimensions increased storage requirements by ~30%. However, this cost is negligible in modern cloud environments.
2.	Higher ETL Complexity
The star schema requires a structured ETL pipeline with dependency management, aggregation logic, and periodic monitoring.
3.	Data Latency (T+1)
Unlike OLTP systems, the star schema is not real-time. It is unsuitable for operational dashboards that require live data without additional CDC or streaming infrastructure.
4.	More Complex Updates and Corrections
Fixing historical data can require coordinated updates across fact and source tables, rather than a single UPDATE statement.
5.	Reduced Ad-Hoc Flexibility
The schema is optimized for known analytical questions. New analysis dimensions may require schema extensions or ETL changes

Bridge Tables: A Balanced Design Choice
Healthcare data naturally contains many-to-many relationships (multiple diagnoses and procedures per encounter). Rather than flattening these into fixed columns, the design used bridge tables.
While this introduces extra JOINs, the benefits outweigh the cost:
•	Unlimited flexibility (no hard limits on diagnoses or procedures)
•	Better storage efficiency
•	Cleaner, more maintainable queries
•	Schema stability over time
Performance testing showed that even with bridge tables, diagnosis–procedure analysis was 8.1× faster than in OLTP. The small additional overhead (~11 ms compared to full denormalization) was deemed acceptable given the long-term flexibility and maintainability.
Quantified Results
Query Type	Speedup	Primary Reason
Monthly Encounters	7.3×	Pre-computed date attributes
Diagnosis–Procedure Analysis	8.1×	Indexed bridge tables
Readmission Rate	94×	Pre-computed readmission flag
Revenue by Specialty	188×	Pre-aggregated billing data
Overall	13.9×	Combined optimizations
All tests were conducted on MySQL 8.0 using a realistic healthcare dataset of 25,000 encounters.

Final Conclusion
This project confirms that there is no single “best” schema—only schemas optimized for specific workloads. The star schema dramatically outperformed OLTP for analytics by shifting computation to ETL, simplifying JOINs, and embracing denormalization.

The investment was clearly justified:
•	Payback period: ~3 weeks
•	ROI: ~15:1 annually
•	User impact: Instant dashboards, better exploration, faster decisions

The correct architectural approach is hybrid:
•	OLTP for real-time transactions and data integrity
•	Star schema for analytics and decision support
By aligning schema design with usage patterns, the project transformed slow, operationally constrained queries into fast, scalable, and business-ready analytics.

