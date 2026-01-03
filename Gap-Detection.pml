@prompt

    @context
        You are a senior Apache Spark engineer with deep production experience in Scala.
        You consistently generate complete, syntactically correct, and production-ready
        Spark code that compiles and executes successfully on the first run without
        runtime errors, logical bugs, or missing imports.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        Problem:
        You are given a Spark DataFrame with columns:
        device_id, event_date.

        Dates are expected to be continuous per device_id.

        Identify all missing date ranges per device and reconstruct a DataFrame
        with columns:
        device_id, missing_start_date, missing_end_date,
        where one or more dates are missing between consecutive event_date values.
    @end

    @instructions
        @step
            Use Scala and Apache Spark DataFrame API only.
            Do not use RDDs, Spark SQL strings, or deprecated APIs.
        @end

        @step
            Create a SparkSession using standard, non-deprecated configuration
            suitable for local execution.
        @end

        @step
            Load the input CSV data from the provided file path.
            Enable header handling and schema inference explicitly.
        @end

        @step
            Validate that the input DataFrame contains the columns
            device_id and event_date.
        @end

        @step
            Explicitly cast event_date to DateType to avoid
            data type and date arithmetic errors.
        @end

        @step
            Define a window specification that partitions data by device_id
            and orders rows by event_date in ascending order.
        @end

        @step
            Use a window function to retrieve the next event_date
            for each device_id.
        @end

        @step
            Identify gaps where the difference between consecutive event_date
            values is greater than one day.
        @end

        @step
            Derive missing_start_date as one day after the current event_date.
        @end

        @step
            Derive missing_end_date as one day before the next event_date.
        @end

        @step
            Exclude records where no gap exists between dates.
        @end

        @step
            Correctly handle edge cases including:
            devices with a single record,
            devices with no missing dates,
            and devices with multiple missing ranges.
        @end

        @step
            Produce a final DataFrame with exactly the following columns
            in this order:
            device_id, missing_start_date, missing_end_date.
        @end

        @step
            Output COMPLETE, runnable Scala Spark code including
            all required imports, ensuring the solution is logically
            correct and error-free on the first execution.
        @end
    @end

    @constraints
        @length min: 300 max: 900 @end
    @end

    @category
        Spark Scala Time Series Analysis
    @end

    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.x"
        api_style: "dataframe"
        problem_type: "gap_detection_time_series"
        window_function: "lead"
        date_handling: "explicit_and_safe"
        null_handling: "explicit"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        file_path: "D:/projects/spark-scala-final/src/main/scala/Data.csv"
    @end

@end
