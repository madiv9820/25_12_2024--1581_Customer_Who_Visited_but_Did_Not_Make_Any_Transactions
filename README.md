# 1581. Customer Who Visited but Did Not Make Any Transactions

- ### Intuition
    The task requires identifying customers who visited the mall but did not make any transactions, and counting how many such visits each customer made. We need to find the difference between two sets of data: the visits and the transactions. Specifically, for each visit, if there is no corresponding transaction, we count that visit for the customer. The goal is to aggregate this count per customer.

- ### Key Insights:
    1. **Left Join**: We can perform a left join between the `Visits` table (or DataFrame) and the `Transactions` table (or DataFrame). This allows us to retain all visits while attempting to match them with any transaction data.
    2. **Missing Transactions**: After the join, if a `visit_id` has no corresponding `transaction_id`, it means that visit did not result in a transaction. These are the visits we are interested in.
    3. **Counting the Visits**: We need to count how many visits each customer made without transactions. This requires grouping the data by `customer_id` and aggregating the count of such visits.

- ### Approach
    - #### Step 1: **Join the Visits and Transactions Data**
        - We need to combine the data from the `Visits` and `Transactions` tables based on the `visit_id`. A left join will ensure that all visits are kept, and transactions will be added wherever they exist.
        - If a customer visit has no associated transaction (i.e., the `transaction_id` is `NULL`), this indicates a visit without a transaction.

    - #### Step 2: **Filter for Visits Without Transactions**
        - After the join, we filter the data to only include the rows where the `transaction_id` is `NULL`. This will give us the visits where no transaction was made.

    - #### Step 3: **Count the Visits per Customer**
        - Group the filtered data by `customer_id` to count how many visits each customer made without a transaction.
        - This gives us the number of visits for each customer where no transaction was made.

    - #### Step 4: **Return the Results**
        - We return the `customer_id` along with the count of visits without transactions, ensuring the result is ordered as needed (in this case, the order doesn't matter, but we may sort it for clarity).

- ### Common Approach (for SQL, PySpark, and Pandas)
    1. **Join Operation**: 
        - SQL: Use a `LEFT JOIN` between the `Visits` and `Transactions` tables.
        - PySpark: Use the `.join()` function with `how='left'`.
        - Pandas: Use `.merge()` with `how='left'`.

    2. **Filtering Missing Transactions**:
        - SQL: Use a `WHERE` clause to check for `NULL` values in the `transaction_id` column.
        - PySpark: Use `.filter()` to check for `NULL` values in the `transaction_id` column.
        - Pandas: Use `.isna()` to filter for rows with missing `transaction_id`.

    3. **Group and Aggregate**:
        - SQL: Use `GROUP BY` on `customer_id` and count the occurrences.
        - PySpark: Use `.groupBy()` on `customer_id` and aggregate with `.agg()` and `count()`.
        - Pandas: Use `.groupby()` on `customer_id` and `.size()` to count the visits.

- ### Time Complexity Consideration
    - **SQL**: The time complexity is typically driven by the `JOIN` and `GROUP BY` operations. If there are `n` rows in `Visits` and `m` rows in `Transactions`, the time complexity is approximately `O(n + m)`, assuming efficient indexing.
    - **PySpark**: The time complexity can be similar to SQL, but PySpark may have some overhead due to distributed processing, especially for large datasets.
    - **Pandas**: Similar to SQL and PySpark, but the time complexity will depend on the size of the DataFrames. The operations (merge, filter, and groupby) each take `O(n)` time, where `n` is the number of rows.

- ### Code Implementation
    - **SQL:**
        ```sql []
        SELECT 
            v.customer_id, 
            COUNT(*) AS count_no_trans 
        FROM Visits v 
        LEFT JOIN Transactions t ON
        v.visit_id = t.visit_id
        WHERE t.transaction_id IS NULL
        GROUP BY v.customer_id
        ```
    - **PySpark:**
        ```python3 []
        def find_customers(visits: pyspark.sql.dataframe.DataFrame, 
                        transactions: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
            output = visits.join(transactions, on = 'visit_id', how = 'left')\
                            .filter(transactions.transaction_id.isNull())\
                            .select('customer_id')\
                            .groupBy('customer_id')\
                            .agg(count('customer_id').alias('count_no_trans'))
            return output
        ```
    - **Pandas**
        - **Code 1**
        ```python3 []
        def find_customers(visits: pd.DataFrame, transactions: pd.DataFrame) -> pd.DataFrame:
            output = visits.merge(transactions, on = 'visit_id', how = 'left')
            output = output[output.transaction_id.isna()]
            output = Counter(output.customer_id)
            return pd.DataFrame(output.items(), columns = ['customer_id', 'count_no_trans'])
        ```
        - **Code 2**
        ```python3 []
        def find_customers(visits: pd.DataFrame, transactions: pd.DataFrame) -> pd.DataFrame:
            output = visits.merge(transactions, on = 'visit_id', how = 'left')
            output = output[output.transaction_id.isna()]
            output = output.groupby('customer_id').size().reset_index(name = 'count_no_trans')
            return output
        ```