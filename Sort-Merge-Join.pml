@prompt

    @context
        You are a Senior Data Engineer with deep real-world experience in
        Apache Spark and Scala.

        You consistently write deterministic, production-ready Spark code
        that executes successfully on the FIRST run without runtime errors,
        analysis exceptions, or ambiguous execution plans.

        Both CSV datasets are large enough that broadcast joins are not suitable.
        A Sort Merge Join is the preferred strategy due to large data volume
        and evenly distributed join keys.
    @end

    @objective
        Design, implement, and explain a COMPLETE Apache Spark solution in Scala
        that performs a Sort Merge Join between two CSV datasets using the
        join key "country".

        The solution MUST:
        - Execute successfully on the FIRST execution
        - Explicitly enforce SortMergeJoin
        - Display the physical execution plan confirming SortMergeJoin
    @end

    @instructions

        @step
            Create a SparkSession using non-deprecated APIs and configure it
            explicitly for local execution.
        @end

        @step
            Enable Sort Merge Join preference by setting
            spark.sql.join.preferSortMergeJoin to true.
        @end

        @step
            Disable automatic Broadcast Hash Join by setting
            spark.sql.autoBroadcastJoinThreshold to -1.
        @end

        @step
            Load both CSV datasets using the DataFrame API with:
            - header = true
            - inferSchema = true
            - FAILFAST mode enabled.
        @end

        @step
            Explicitly validate that both DataFrames contain the join key "country"
            and that the join key data types match.
        @end

        @step
            Repartition both DataFrames on the join key "country"
            to enforce a shuffle-based join.
        @end

        @step
            Explicitly sort both DataFrames by the join key "country"
            to support the Sort Merge Join strategy.
        @end

        @step
            Perform an INNER JOIN using fully qualified column references
            on the join key "country" to avoid ambiguity.
        @end

        @step
            Display the physical execution plan using explain(true)
            and confirm that SortMergeJoin is used.
        @end

        @step
            Display the joined output using show(truncate = false).
            Output action must be show only.
        @end

        @step
            Stop the SparkSession cleanly after execution.
        @end
    @end

    @constraints
        @length min: 250 max: 900 @end
    @end

    @category
        Spark Scala – Join Strategy
    @end

    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.5.1"
        execution_mode: "local"
        join_type: "sort_merge_join"
        join_key: "country"
        broadcast_enabled: "false"
        sort_merge_enabled: "true"
        api_style: "dataframe"
        error_tolerance: "zero"
    @end
@end