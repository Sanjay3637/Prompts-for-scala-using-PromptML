@prompt

    @context
        You are a senior Apache Spark engineer with deep production
        experience in Scala.

        You consistently generate complete, correct, and production-ready
        Spark code that compiles and executes successfully on the FIRST run
        without runtime errors, logical bugs, or missing imports.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        The input data will be loaded from a CSV file into a Spark DataFrame
        with the following columns:

        - category
        - product_id
        - revenue

        Problem:
        Compute the top 3 products per category based on revenue.

        If multiple products are tied at the 3rd position by revenue,
        include ALL tied products, even if this results in more than
        3 rows per category.
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
            category, product_id, and revenue.
            Fail fast with a clear error if any column is missing.
        @end

        @step
            Ensure revenue is treated as a numeric column
            and explicitly cast it if required.
        @end

        @step
            Define a window specification partitioned by category
            and ordered by revenue in descending order.
        @end

        @step
            Use the dense_rank window function so that equal
            revenue values receive the same rank.
        @end

        @step
            Filter the results to include all rows where the rank
            is less than or equal to 3.
        @end

        @step
            Ensure that ties at the 3rd rank are included,
            even if this results in more than 3 products per category.
        @end

        @step
            Remove any intermediate ranking columns from
            the final output.
        @end

        @step
            Produce a final DataFrame with exactly the following
            columns in this order:
            category, product_id, revenue.
        @end
        @step
            Output COMPLETE, runnable Scala Spark code,
            including all required imports.
        @end
    @end

    @constraints
        @length min: 300 max: 900 @end
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
        problem_type: "top_k_with_ties"
        window_function: "dense_rank"
        ordering: "revenue_desc"
        tie_handling: "include_all_at_rank_3"
        null_handling: "explicit"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        input_file_path: "D:/projects/spark-scala-final/src/main/scala/Data.csv"
    @end
@end