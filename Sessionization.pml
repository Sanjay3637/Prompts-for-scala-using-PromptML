@prompt

    @context
        You are a senior Apache Spark engineer with deep production
        experience in Scala.

        You always generate complete, syntactically correct, and
        production-ready Spark code that compiles and executes
        successfully on the FIRST run without runtime errors
        or logical bugs.

        The input event data is unordered and must not be assumed
        to be pre-sorted.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        The input data will be loaded from a CSV file into a Spark
        DataFrame with the following columns:

        - user_id
        - event_time

        A session is defined as a sequence of consecutive events
        for the same user where the time gap between two consecutive
        events is less than or equal to 30 minutes.

        Generate an output DataFrame with exactly the following columns:
        - user_id
        - session_id
        - session_start
        - session_end
    @end

    @instructions

        @step
            Use Scala and Apache Spark DataFrame API only.
            Do not use RDDs, Spark SQL strings, or deprecated APIs.
        @end

        @step
            Create a SparkSession using standard, non-deprecated
            configuration suitable for local execution.
        @end

        @step
            Load the input CSV file into a DataFrame using:
            - header = true
            - inferSchema = true
            - FAILFAST mode enabled.
        @end

        @step
            Validate that the DataFrame contains the columns
            user_id and event_time.
            Fail fast with a clear error if any column is missing.
        @end

        @step
            Explicitly cast event_time to TimestampType.
            Do not assume any implicit ordering or timezone behavior.
        @end

        @step
            Define a window specification partitioned by user_id
            and ordered by event_time in ascending order.
        @end

        @step
            Use a lag window function to compute the previous
            event_time for each user.
        @end

        @step
            Identify the start of a new session when either:
            - the previous event_time is null, or
            - the time gap between consecutive events exceeds 30 minutes.
        @end

        @step
            Generate session_id by cumulatively summing the
            session-start indicator within each user partition.
        @end

        @step
            Group events by user_id and session_id to compute:
            - session_start as the minimum event_time
            - session_end as the maximum event_time.
        @end

        @step
            Produce a final DataFrame with exactly the following
            columns in this order:
            user_id, session_id, session_start, session_end.
        @end

        @step
            Output COMPLETE, runnable Scala Spark code including
            all required imports and valid window definitions.
        @end
    @end

    @constraints
        @length min: 350 max: 1000 @end
    @end

    @category
        Spark Scala Analytics
    @end

    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.5.1"
        execution_mode: "local"
        api_style: "dataframe"
        problem_type: "sessionization"
        window_functions: "lag,sum"
        time_gap_minutes: "30"
        ordering: "event_time_asc"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        input_file_path: "D:/projects/spark-scala-final/src/main/scala/Data.csv"
    @end
@end