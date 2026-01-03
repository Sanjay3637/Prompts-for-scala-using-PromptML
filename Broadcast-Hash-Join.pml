@prompt

    @context
        You are a Senior Data Engineer and Apache Spark expert with deep, real-world
        production experience in Scala.

        You write deterministic, production-ready Spark code that executes successfully
        on the FIRST run with zero runtime errors, zero analysis exceptions, and zero
        ambiguous schemas.

        Data-set-1.csv represents the LARGE transactional dataset.
        Data-set-2.csv represents the SMALL lookup dataset suitable for broadcasting.
    @end

    @objective
        Design a COMPLETE Scala Spark solution that performs a Broadcast Hash Join
        using two CSV files.

        The solution MUST:
        - Execute successfully on the FIRST execution
        - Explicitly use BroadcastHashJoin
        - Show the physical execution plan confirming BroadcastHashJoin
    @end

    @instructions

        @step
            Create a SparkSession using non-deprecated APIs.
            Configure it correctly for local execution and set a reasonable
            shuffle partition count optimized for local mode.
        @end

        @step
            Load the LARGE CSV file using the DataFrame API with:
            - header = true
            - inferSchema = true
            - FAILFAST mode enabled

            File path:
            - D:\projects\spark-scala-final\src\main\scala\Data-set-1.csv
        @end

        @step
            Load the SMALL CSV file using the DataFrame API with:
            - header = true
            - inferSchema = true
            - FAILFAST mode enabled

            File path:
            - D:\projects\spark-scala-final\src\main\scala\Data-set-2.csv
        @end

        @step
            Explicitly validate that the join column exists in BOTH DataFrames.
            If the column is missing, fail fast with a clear, human-readable error.

            Join key:
            - country
        @end

        @step
            Apply a Broadcast Hash Join by explicitly broadcasting the SMALL DataFrame
            using spark.sql.functions.broadcast.
        @end

        @step
            Perform an INNER JOIN using fully qualified column references
            (df.column) to avoid column ambiguity.
        @end

        @step
            Display the full execution plan using explain(true).
            Ensure the physical plan explicitly shows BroadcastHashJoin.
        @end

        @step
            Display sample output using show(truncate = false).
            Output action must be show only.
        @end

        @step
            Stop the SparkSession cleanly at the end of execution.
        @end
    @end


    @constraints
        @length min: 200 max: 800 @end
    @end


    @category
        Spark Scala – Join Optimization
    @end


    @metadata
        scala_version: "2.12.18"
        spark_version: "3.5.1"
        execution_mode: "local"
        api_style: "DataFrame"
        join_type: "broadcast_hash_join"
        error_tolerance: "zero"
    @end

@end
