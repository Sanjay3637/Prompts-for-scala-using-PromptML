@prompt

    @context
        You are a senior Apache Spark engineer with deep production
        experience in Scala.

        You always generate complete, syntactically correct, and
        production-ready Spark code that compiles and executes
        successfully on the FIRST run without runtime errors,
        logical bugs, or missing imports.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        The input data will be loaded from a CSV file into a Spark
        DataFrame with the following columns:

        - id
        - source_system
        - update_ts
        - value

        Multiple records may exist for the same id.

        Deduplicate the data such that exactly ONE record is returned
        per id according to the following rules:

        - Records from source_system = 'SYSTEM_A' always take precedence.
        - If no record exists for SYSTEM_A for a given id, select the
          record with the latest update_ts.
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
            id, source_system, update_ts, and value.
            Fail fast with a clear error if any column is missing.
        @end

        @step
            Explicitly cast update_ts to TimestampType.
            Do not assume implicit timestamp parsing behavior.
        @end

        @step
            Define a window specification partitioned by id and
            ordered by the following priority:
            1. Records where source_system = 'SYSTEM_A'
            2. update_ts in descending order.
        @end

        @step
            Implement the priority ordering using an explicit
            conditional expression so that SYSTEM_A rows are
            ranked higher than all other source systems.
        @end

        @step
            Use the row_number window function to assign a unique
            rank within each id partition.
        @end

        @step
            Filter the DataFrame to retain only the row where
            row_number equals 1 for each id.
        @end

        @step
            Remove any intermediate helper or ranking columns
            before producing the final output.
        @end

        @step
            Produce a final DataFrame with exactly the following
            columns in this order:
            id, source_system, update_ts, value.
        @end

        @step
            Output COMPLETE, runnable Scala Spark code including
            all required imports and valid window definitions.
        @end
    @end

    @constraints
        @length min: 300 max: 900 @end
    @end

    @category
        Spark Scala Data Deduplication
    @end

    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.5.1"
        execution_mode: "local"
        api_style: "dataframe"
        problem_type: "conditional_deduplication"
        priority_rule: "system_a_then_latest_timestamp"
        window_function: "row_number"
        timestamp_handling: "explicit"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        input_file_path: "D:/projects/spark-scala-final/src/main/scala/Data.csv"
    @end
@end
