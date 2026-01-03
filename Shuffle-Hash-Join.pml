@prompt

    @context
        You are a Senior Data Engineer with deep real-world experience in
        Apache Spark and Scala.

        You consistently produce deterministic, production-ready Spark code
        that executes successfully on the FIRST run without runtime errors,
        analysis exceptions, or ambiguous execution plans.

        Data-set-1.csv represents a LARGE dataset that cannot be broadcast.
        Data-set-2.csv represents a MEDIUM dataset suitable for shuffle-based joins.

        A Shuffle Hash Join must be enforced by disabling both automatic
        broadcast joins and sort-merge join preference.
    @end

    @objective
        Design, implement, and explain a COMPLETE Apache Spark solution in Scala
        that performs a Shuffle Hash Join between two CSV datasets.

        The solution MUST:
        - Execute successfully on the FIRST execution
        - Explicitly enforce ShuffleHashJoin
        - Display the physical execution plan confirming ShuffleHashJoin
    @end

    @instructions

        @step
            Create a SparkSession using non-deprecated APIs and configure it
            explicitly for local execution.
        @end

        @step
            Disable automatic Broadcast Hash Join by setting
            spark.sql.autoBroadcastJoinThreshold to -1.
        @end

        @step
            Disable Sort Merge Join preference by setting
            spark.sql.join.preferSortMergeJoin to false.
        @end

        @step
            Load both CSV datasets using the DataFrame API with:
            - header = true
            - inferSchema = true
            - FAILFAST mode enabled.
        @end

        @step
            Explicitly validate that both DataFrames contain the join key
            and that the join key data types match.
        @end

        @step
            Repartition both DataFrames on the join key to enforce
            a shuffle-based join strategy.
        @end

        @step
            Perform an INNER JOIN using fully qualified column references
            to avoid column ambiguity.
        @end

        @step
            Display the physical execution plan using explain(true) and
            confirm that ShuffleHashJoin is used.
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
        Spark Scala – Join Optimization
    @end

    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.5.1"
        execution_mode: "local"
        join_type: "shuffle_hash_join"
        broadcast_enabled: "false"
        sort_merge_enabled: "false"
        api_style: "dataframe"
        error_tolerance: "zero"
        file_1_path: "D:\projects\spark-scala-final\src\main\scala\Data-set-1.csv"
        file_2_path: "D:\projects\spark-scala-final\src\main\scala\Data-set-2.csv"
    @end
@end