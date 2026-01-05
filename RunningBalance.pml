@prompt

    @context
        You are a senior Apache Spark engineer with deep production experience in Scala.
        You consistently generate syntactically correct, production-ready Spark code
        that compiles and executes successfully on the first run without runtime errors
        or logical mistakes.
    @end

    @objective
        Solve the following problem using Scala and Apache Spark.

        Problem:
        You are given a Spark DataFrame with columns:
        account_id, txn_time, amount, txn_type.

        The txn_type can be CREDIT, DEBIT, or RESET.

        Calculate a running_balance column such that:
        - CREDIT increases the balance
        - DEBIT decreases the balance
        - RESET sets the balance to zero immediately after it occurs,
          regardless of the previous balance.
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
            Load the input CSV file from the provided file path.
            Enable header handling and schema inference explicitly.
        @end

        @step
            Validate that the input DataFrame contains the columns
            account_id, txn_time, amount, and txn_type.
        @end

        @step
            Ensure txn_time is treated as a time-ordered column
            and explicitly cast it to a timestamp-compatible type if required.
        @end

        @step
            Ensure amount is treated as a numeric column
            and explicitly cast it if required.
        @end

        @step
            Define a window specification that partitions data by account_id
            and orders rows by txn_time in ascending order.
        @end

        @step
            Identify reset points and create logical groups so that
            running balances restart after each RESET event.
        @end

        @step
            Within each group, compute the running balance such that:
            CREDIT adds the amount and DEBIT subtracts the amount.
        @end

        @step
            Ensure that the balance immediately after a RESET
            starts from zero and does not carry forward prior values.
        @end

        @step
            Produce a final DataFrame with all original columns
            plus a correctly computed running_balance column.
        @end

        @step
            Output COMPLETE, runnable Scala Spark code,
            including all required imports and transformations,
            ensuring the solution is error-free on the first execution.
        @end
    @end

    @constraints
        @length min: 350 max: 1000 @end
    @end

    @category
        Spark Scala Window Functions
    @end

    @metadata
        language: "scala"
        framework: "apache spark"
        spark_version: "3.x"
        api_style: "dataframe"
        problem_type: "running_balance_with_reset"
        window_functions: "sum"
        ordering: "txn_time_asc"
        reset_logic: "grouped_window_restart"
        error_tolerance: "zero"
        first_run_success: "mandatory"
        file_path: "D:/projects/spark-scala-final/src/main/scala/Data.csv"
    @end

@end
