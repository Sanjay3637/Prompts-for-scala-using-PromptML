@prompt

    @context
        You are a senior Apache Spark engineer with deep production
        experience in Scala.

        You consistently generate complete, correct, and production-ready
        Spark code that compiles and executes successfully on the FIRST run
        without runtime errors, logical bugs, or performance issues such as
        cartesian or cross-join explosions.

        The solution must be implemented using only deterministic
        DataFrame-based transformations.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        The input data will be loaded from two CSV files into Spark
        DataFrames with the following schemas:

        1) user_id, country
        2) country, tax_rate, effective_from

        Problem:
        Join the datasets such that each user receives the latest
        applicable tax_rate for their country as of the current date.

        Constraints:
        - Do not use subqueries
        - Do not use UDFs
        - Avoid cartesian or cross-join explosion
    @end

    @instructions

        @step
            Use Scala and Apache Spark DataFrame API only.
            Do not use RDDs, Spark SQL strings, subqueries, or UDFs.
        @end

        @step
            Create a SparkSession using standard, non-deprecated
            configuration suitable for local execution.
        @end

        @step
            Load both input CSV files into DataFrames using:
            - header = true
            - inferSchema = true
            - FAILFAST mode enabled.
        @end

        @step
            Validate that the user DataFrame contains the columns:
            user_id and country, and that the tax DataFrame contains:
            country, tax_rate, and effective_from.
            Fail fast with a clear error if any column is missing.
        @end

        @step
            Explicitly cast effective_from to DateType.
            Do not assume implicit date parsing behavior.
        @end

        @step
            Filter the tax DataFrame to include only records where
            effective_from is less than or equal to current_date().
        @end

        @step
            Define a window specification partitioned by country
            and ordered by effective_from in descending order.
        @end

        @step
            Use a window ranking function to select the single
            most recent applicable tax record per country.
        @end

        @step
            Remove any intermediate ranking columns from the
            derived tax DataFrame.
        @end

        @step
            Join the user DataFrame with the derived latest-tax
            DataFrame using an equality join on country only.
        @end

        @step
            Ensure the join strategy does not introduce cartesian
            joins and operates only on the reduced tax dataset.
        @end

        @step
            Produce a final DataFrame with exactly the following columns:
            user_id, country, tax_rate.
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
        Spark Scala Join Optimization
    @end


    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.5.1"
        execution_mode: "local"
        api_style: "dataframe"
        problem_type: "as_of_join_without_cartesian"
        join_strategy: "window_then_equi_join"
        date_reference: "current_date"
        subqueries_used: "false"
        udfs_used: "false"
        cartesian_allowed: "false"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        input_files:
            users: "D:/projects/spark-scala-final/src/main/scala/users.csv"
            taxes: "D:/projects/spark-scala-final/src/main/scala/taxes.csv"
    @end

@end
