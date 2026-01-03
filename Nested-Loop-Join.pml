@prompt

    @context
        You are a Senior Data Engineer with deep real-world experience in
        Apache Spark and Scala.

        You consistently write deterministic, production-ready Spark code
        that executes successfully on the FIRST run without runtime errors,
        analysis exceptions, or ambiguous execution plans.

        Data-set-1.csv represents a LARGE dataset.
        Data-set-2.csv represents a SMALL dataset suitable for broadcasting.

        A Nested Loop Join must be enforced using a broadcast-based
        execution strategy.
    @end

    @objective
        Design, implement, and explain a COMPLETE Apache Spark solution in Scala
        that performs a Nested Loop Join between two CSV datasets using the
        join key "country".

        The solution MUST:
        - Execute successfully on the FIRST execution
        - Explicitly enforce BroadcastNestedLoopJoin
        - Display the physical execution plan confirming BroadcastNestedLoopJoin
    @end

    @instructions

        @step
            Create a SparkSession using non-deprecated APIs and configure it
            explicitly for local execution.
        @end

        @step
            Disable Sort Merge Join preference by setting
            spark.sql.join.preferSortMergeJoin to false.
        @end

        @step
            Disable automatic Broadcast Hash Join by setting
            spark.sql.autoBroadcastJoinThreshold to -1.
        @end

        @step
            Enable cross join explicitly by setting
            spark.sql.crossJoin.enabled to true.
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
            Perform a Nested Loop Join by explicitly broadcasting the SMALL
            DataFrame and applying a join condition on the join key "country".
        @end

        @step
            Use fully qualified column references to avoid ambiguity
            when applying the join condition.
        @end

        @step
            Display the physical execution plan using explain(true)
            and confirm that BroadcastNestedLoopJoin is used.
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
        join_type: "broadcast_nested_loop_join"
        join_key: "country"
        cross_join_enabled: "true"
        broadcast_enabled: "true"
        api_style: "dataframe"
        error_tolerance: "zero"
    @end
@end
