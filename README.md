# API Description

## API Features

- The REST API has two POST endpoints:
    - update
    - query
- The __update__ POST endpoint receives a JSON in the form of a document with two fields: a pool-id (numeric) and a pool-values (array of values) and is meant to append (if pool already exists) or insert (new pool) the values to the appropriate pool (as per the id).
The response from the append is a status field confirming "appended" or "inserted".
- The __query__ POST endpoint is meant to query a pool, the two fields are pool-id (numeric) identifying the queried pool, and a quantile (in percentile form). The response from the query has two fields:
    - calculated_quantile: the calculated quantile 
    - total_count_of_elements: the total count of elements in the pool

## Validity requirements for input data
- poolId: is an integer (be interpreted as int type in Python programming language)
- poolValues: is a 1-dimensional array of real number (be interpreted as a list of int and/or float type in Python programming language)
- percentile: is a real number in the range [0, 100]

## Data storage mechanism and optimization strategy
- Data is sharded and stored in different files based on poolId (_abs_(poolId) // 1000), each file will contain data of up to 1000 pools, which reduces query time, reduces CPU and memory consumption, and makes it easier to manage.
- The structure of the records has the following form:

| poolId |      poolValues      | sorted |
| :----: | :------------------: | :----: |
|  123   | "[-1, 5, 4, 5.5, 9]" |   0    |
|  -12   | "[1, 3, 5, 7, 9.9]"  |   1    |
|  ...   |         ...          |  ...   |

- When record is saved to file, __poolValues__ is converted to string
- __sorted__ field acts as a label
    - If _sorted_ = 1, the data in poolValues is already sorted, we won't need to reorder _poolValues_ anymore to calculate quantile
    - If _sorted_ = 0, before calculating quantile, we proceed to sort _poolValues_, update _sorted_ to 1 then save the result back to file
    - Every time _poolValues_ list is appended, the _sorted_ field will be reset to 0

> In the future, it is possible to modify the code to save the position of the last element of poolValues ​​when the last time poolValues ​​was sorted, so that you can customize the sort algorithm to be the most suitable.

## API Functions
### validate_pool
| validate_pool(pool)                                                                                                                                                                    |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Check if data sent to __update__ endpoint is valid                                                                                                                                     |
| Parameters:<ul><li>pool: dict</li></ul>                                                                                                                                                |
| Returns: <ul><li>is_valid: bool <ul><li>True if data is valid</li><li>False if data is invalid</li></ul><li>message: str<ul>Detailed explanation of the returned result</ul></li></ul> |

### validate_query

| validate_query(query)                                                                                                                                                                  |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Check if data sent to __query__ endpoint is valid                                                                                                                                      |
| Parameters:<ul><li>query: dict</li></ul>                                                                                                                                               |
| Returns: <ul><li>is_valid: bool<ul><li>True if data is valid</li><li> False if data is invalid</li></ul><li>message: str<ul>Detailed explanation of the returned result</ul></li></ul> |

### does_path_exist

| does_path_exist(path)                                                                                                                     |
| :---------------------------------------------------------------------------------------------------------------------------------------- |
| Check if the given path exists                                                                                                            |
| Parameters:<ul><li>path: str</li></ul>                                                                                                    |
| Returns:<ul><li>path_exists: bool<ul><li>True if the given path exists</li><li>False if the given path does not exist</li></ul></li></ul> |

### does_pool_exist

| does_pool_exist(id, list_id)                                                                                                                      |
| :------------------------------------------------------------------------------------------------------------------------------------------------ |
| Check if given _poolId_ exists in the _poolId_ list                                                                                               |
| Parameters:<ul><li>id: int</li><li>list_id: list</li></ul>                                                                                        |
| Returns:<ul><li>pool_exists: bool<ul><li>True if the given _poolId_ exists</li><li>False if the given _poolId_ does not exist</li></ul></li></ul> |


### get_path_by_id

| get_path_by_id(id)                            |
| :-------------------------------------------- |
| Get file path corresponding to given _poolId_ |
| Parameters:<ul><li>id: int</li></ul>          |
| Returns:<ul><li>file_path: str</li></ul>      |


### load_data

| load_data(path)                                                   |
| :---------------------------------------------------------------- |
| Load data of file which saved in given path into Pandas Dataframe |
| Parameters:<ul><li>path: str</li></ul>                            |
| Returns:<ul><li>df: Pandas Dataframe</li></ul>                    |


### save_data

| save_data(path, df)                                                 |
| :------------------------------------------------------------------ |
| Write Dataframe to given file path                                  |
| Parameters:<ul><li>path: str</li><li>df: Pandas Dataframe</li></ul> |
| Returns: <ul><li>None</ul></li>                                     |


### calculate_quantile

| calculate_quantile(sorted_list, percentile)                                     |
| :------------------------------------------------------------------------------ |
| Compute the percentile % quantile of the given sorted list                      |
| Parameters:<ul><li>sorted_list: list</li><li>percentile: int or float</li></ul> |
| Returns: <ul><li>quantile: int or float</li><li>total_elements: int</li></ul>   |


### insert_pool

| insert_pool(pool, current_df=None)                                                                                          |
| :-------------------------------------------------------------------------------------------------------------------------- |
| Write pool data to Dataframe, if it does not provide Dataframe, create new one, else concatenate with the current Dataframe |
| Parameters:<ul><li>pool: dict</li><li>current_df: None or Pandas Dataframe, optional</li></ul>                              |
| Returns: <ul><li>new_df: Pandas Dataframe</li></ul>                                                                         |


### append_pool_values

| append_pool_values(pool, df)                                                                                                                                            |
| :---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Append new values in given pool to current _poolValues_ list in Dataframe corresponding to _poolId_ in given pool, update _sorted_ field to 0                           |
| Parameters:<ul><li>pool: dict</li><li>df: Pandas Dataframe</li></ul>                                                                                                    |
| Returns: <ul><li>new_df: Pandas Dataframe</li><li>df_has_changed: bool<ul><li> True if Dataframe has changed</li><li> False if Dataframe has not changed</li></li></ul> |

### sort_pool_values

| sort_pool_values(query, df)                                                                                                                                                                                                                         |
| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Try sorting pool values for _poolId_ given by the query then update Dataframe if need                                                                                                                                                               |
| Parameters:<ul><li>query: dict</li><li>df: Pandas Dataframe</li></ul>                                                                                                                                                                               |
| Returns: <ul><li>sorted_pool_values_list: list<ul><li> a list of sorted values</li></ul></li><li>df: Pandas Dataframe</li><li>df_has_changed: bool<ul><li> True if Dataframe has changed</li><li> False if Dataframe has not changed</li></li></ul> |


### update

| update()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Handle requests from __update__ POST endpoint. <ul><li>First, check if the data is valid, if the data does not satisfy the condition, return __Error 400__ and a message detailing the error.</li></ul><ul><li>Next, check if _poolId_ already exists or not.</li></ul> <ul><li>If _poolId_ already exists, append new values to current _poolValues_ field and update _sorted_ field, save results back to file and return "appended" message. <br> If _poolId_ does not exist, create a new Dataframe containing the information of the new pool, add right value to sorted field, save the record to the file corresponding to the _poolId_ and return "inserted" message. </li></ul> |
| Input:<ul><li>JSON</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Output: <ul><li>JSON</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |


### query

| query()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Handle requests from __query__ POST endpoint.<ul><li>First, check if the data is valid, if the data does not satisfy the condition, return __Error 400__ and a message detailing the error.</li></ul><ul><li>Next, check if _poolId_ already exists or not.</li></ul><ul><li>If _poolId_ does not exist, return __Error 400__ and "poolId does not exist" message. </li></ul><ul><li>If _poolId_ already exists, check if _poolValues_ is sorted or not</li></ul><ul><li>If _poolValues_ is not sorted, sort the _poolValues_ list, update the _sorted_ field to value of 1, and save the record back to file. If _poolValues_ are sorted, do nothing but move to quantile calculation.</li></ul><ul><li>The *calculate_quantile* function will take care of the quantile calculation and return the calculated quantile and the total count of elements.</li></ul><ul><li>Return a message containing information about the calculated quantile and the total count of elements.</li></ul> |
| Input:<ul><li>JSON</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Output: <ul><li>JSON</li></ul>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |

## How to run

1. Install virtualenv
    - pip install virtualenv
1. Create virtual environment
    - virtualenv venv
1. Work with virtual environment
    - Activate virtual environment
        - venv\Scripts\activate
    - Deactivate virtual environment
        - deactivate
1. Install requirements
    - pip install -r requirements.txt
1. Run API
    - python api.py
1. Run test
    - pytest test_api.py
    
#### __Noted__
> When running test, the data created when you interact with the API stored in the file 99991.csv will be deleted.