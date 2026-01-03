@prompt

    @context
        You are a senior Apache Spark engineer with deep production
        experience in Scala.

        You consistently generate complete, syntactically correct,
        and production-ready Spark code that compiles and executes
        successfully on the FIRST run without runtime errors,
        logical mistakes, or incorrect window semantics.

        The input event data is unordered and must not be assumed
        to be pre-sorted.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        The input data will be loaded from a CSV file into a Spark
        DataFrame with the following columns:

        - card_id
        - event_time
        - merchant_id
        - amount

        Problem:
        Detect fraud patterns where, for the same card_id:

        - three or more transactions
        - from three or more DISTINCT merchants
        - occur within ANY rolling 10-minute time window.

        Return the result with the following columns:
        - card_id
        - window_start
        - window_end
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
            Validate that the DataFrame contains the columns:
            card_id, event_time, merchant_id, and amount.
            Fail fast with a clear error if any column is missing.
        @end

        @step
            Explicitly cast event_time to TimestampType.
            Do not assume implicit timestamp parsing behavior.
        @end

        @step
            Define a window specification partitioned by card_id
            and ordered by event_time in ascending order.
        @end

        @step
            Use a TRUE rolling time window by defining a range-based
            window frame that includes all events within the
            previous 10 minutes relative to each event_time.
        @end

        @step
            Within the rolling window, compute:
            - the total transaction count
            - the distinct count of merchant_id.
        @end

        @step
            Identify fraud windows where:
            - transaction count >= 3, AND
            - distinct merchant count >= 3.
        @end

        @step
            Derive window_start as the earliest event_time
            and window_end as the latest event_time
            within each detected rolling window.
        @end

        @step
            Remove duplicate results by retaining unique
            combinations of card_id, window_start, and window_end.
        @end

        @step
            Produce a final DataFrame with exactly the following
            columns in this order:
            card_id, window_start, window_end.
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
        Spark Scala Fraud Analytics
    @end

    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.5.1"
        execution_mode: "local"
        api_style: "dataframe"
        problem_type: "fraud_pattern_detection"
        window_type: "rolling_time_window"
        window_frame: "rangeBetween(-10_minutes, 0)"
        aggregation_type: "count_and_countDistinct"
        timestamp_handling: "explicit"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        input_file_path: "D:/projects/spark-scala-final/src/main/scala/Data.csv"
    @end
@end
