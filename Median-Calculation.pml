@prompt

    @context
        You are a senior Apache Spark engineer with deep production
        experience in Scala.

        You consistently generate syntactically correct, production-ready
        Spark code that compiles and executes successfully on the FIRST run
        without runtime errors, logical bugs, or scalability issues.

        The solution must compute an EXACT median without using any
        approximate aggregation functions.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        The input data will be loaded from a CSV file into a Spark
        DataFrame with the following columns:

        - group_id
        - value

        Problem:
        Compute the exact median value per group_id with the following requirements:

        - Handle both odd and even record counts correctly
        - Do NOT use approximate functions
        - The solution must work correctly for very large groups
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
            group_id and value.
            Fail fast with a clear error if any column is missing.
        @end

        @step
            Explicitly cast the value column to a numeric type
            to avoid type-related aggregation errors.
        @end

        @step
            Define a window specification partitioned by group_id
            and ordered by value in ascending order.
        @end

        @step
            Use window functions to compute:
            - row_number within each group_id
            - total record count per group_id.
        @end

        @step
            Identify median positions as follows:
            - For odd counts: the single row at position (count + 1) / 2
            - For even counts: the two rows at positions count / 2 and (count / 2) + 1.
        @end

        @step
            Compute the exact median by:
            - selecting the middle value for odd-sized groups
            - averaging the two middle values for even-sized groups.
        @end

        @step
            Ensure the computation is fully distributed and does not
            collect data to the driver.
        @end

        @step
            Remove all intermediate helper columns before producing
            the final output.
        @end

        @step
            Produce a final DataFrame with exactly the following columns
            in this order:
            group_id, median_value.
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
        problem_type: "exact_median_per_group"
        window_functions: "row_number,count"
        aggregation_type: "exact"
        approximate_functions: "disallowed"
        scalability: "large_groups_supported"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        input_file_path: "D:/projects/spark-scala-final/src/main/scala/Data.csv"
    @end

@end
