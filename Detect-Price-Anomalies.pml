@prompt

    @context
        You are a senior Apache Spark engineer with deep production
        experience in Scala.

        You consistently generate complete, syntactically correct,
        and production-ready Spark code that compiles and executes
        successfully on the FIRST run without runtime errors,
        logical bugs, or missing imports.

        The input event data is unordered and must not be assumed
        to be pre-sorted.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        The input data will be loaded from a CSV file into a Spark
        DataFrame with the following columns:

        - product_id
        - price
        - event_time

        Problem:
        Identify rows where the price deviates by more than
        two standard deviations from the rolling average of the
        previous 10 records for the same product.

        Rows with fewer than 10 prior records must be excluded
        from the result.
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
            product_id, price, and event_time.
            Fail fast with a clear error if any column is missing.
        @end

        @step
            Explicitly cast price to a numeric type and event_time
            to TimestampType to avoid type-related runtime errors.
        @end

        @step
            Define a window specification partitioned by product_id
            and ordered by event_time in ascending order.
        @end

        @step
            Define a rolling row-based window frame that includes
            exactly the previous 10 rows and excludes the current row.
        @end

        @step
            Compute the rolling average and rolling standard deviation
            of price using the defined window.
            Use stddev_samp explicitly for standard deviation.
        @end

        @step
            Exclude rows where fewer than 10 prior records exist
            by validating the rolling count.
        @end

        @step
            Identify anomaly rows where the absolute difference
            between the current price and the rolling average
            is greater than two times the rolling standard deviation.
        @end

        @step
            Remove all intermediate calculation columns
            from the final output.
        @end

        @step
            Output ONLY the anomalous rows with the original columns:
            product_id, price, event_time.
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
        Spark Scala Anomaly Detection
    @end


    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.5.1"
        execution_mode: "local"
        api_style: "dataframe"
        problem_type: "rolling_window_anomaly_detection"
        window_size: "10"
        window_frame: "rowsBetween(-10,-1)"
        deviation_threshold: "2_stddev"
        stddev_function: "stddev_samp"
        null_handling: "explicit"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        input_file_path: "D:/projects/spark-scala-final/src/main/scala/Data.csv"
    @end

@end
