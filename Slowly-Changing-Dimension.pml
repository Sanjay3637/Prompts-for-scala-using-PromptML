@prompt

    @context
        You are a senior Apache Spark engineer with deep production
        experience in Scala.

        You always generate complete, defensive, and production-ready
        Spark code that executes successfully on the FIRST run without
        compilation errors, runtime failures, or logical bugs.

        The input data represents attribute change history and must be
        treated as unordered. No assumptions about record ordering
        should be made.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        You are given an input CSV file that will be loaded into a Spark
        DataFrame with the following columns:

        - customer_id
        - attribute_value
        - effective_date

        A new record is inserted whenever the attribute_value changes.

        Reconstruct a Slowly Changing Dimension (Type-2) DataFrame with
        the following columns:

        - customer_id
        - attribute_value
        - start_date
        - end_date

        Rules:
        - start_date is equal to effective_date
        - end_date is the day before the next effective_date for the same customer_id
        - the most recent record for each customer_id must have end_date set to null
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
            Validate that the input DataFrame contains the columns:
            customer_id, attribute_value, and effective_date.
            Fail fast with a clear error if any column is missing.
        @end

        @step
            Explicitly cast effective_date to DateType.
            Do not assume timestamp precision or timezone behavior.
        @end

        @step
            Define a window specification partitioned by customer_id
            and ordered by effective_date in ascending order.
        @end

        @step
            Use a window function to retrieve the next effective_date
            for each customer_id.
        @end

        @step
            Set start_date equal to effective_date.
        @end

        @step
            Compute end_date as one day before the next effective_date
            using Spark date functions.
        @end

        @step
            Ensure that records with no next effective_date
            have end_date explicitly set to null.
        @end

        @step
            Correctly handle customers with a single record
            and customers with multiple attribute changes.
        @end

        @step
            Output COMPLETE, runnable Scala Spark code
            including all required imports.
        @end
    @end

    @constraints
        @length min: 350 max: 1000 @end
    @end

    @category
        Spark Scala Data Engineering
    @end

    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.5.1"
        execution_mode: "local"
        api_style: "dataframe"
        problem_type: "scd_type_2_reconstruction"
        window_function: "lead"
        date_handling: "explicit_date_type"
        null_handling: "explicit"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        input_file_path: "D:/projects/spark-scala-final/src/main/scala/Data.csv"
    @end
@end