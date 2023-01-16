from flask import Flask, request
import os
import ast
import pandas as pd
import math
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/update", methods=['POST'])
def update():
    logger.info("ENDPOINT /update")
    data = request.get_json()
    logger.info(f"POOL DATA: {str(data)}")
    
    is_pool_valid, message = validate_pool(data)
    if not is_pool_valid:
        logger.info('RETURN ERROR 400, INVALID POOL')
        return {"error": message}, 400
    
    file_path = get_path_by_id(data['poolId'])
    if not does_path_exist(file_path):
        new_df = insert_pool(data)
        save_data(file_path, new_df)
        logger.info('Successfully inserted new pool')
        return {"status": "inserted"}
    
    else:
        df_file_path = load_data(file_path)
        df_index_values = df_file_path.index.values
        
        if does_pool_exist(data["poolId"], df_index_values): #append
            new_df = append_pool_values(data, df_file_path)
            save_data(file_path, new_df)
            logger.info('Successfully appended pool')
            return {"status": "appended"}
        else: 
            new_df = insert_pool(data, df_file_path) #insert
            save_data(file_path, new_df)
            logger.info('Successfully inserted new pool')
            return {"status": "inserted"} 

@app.route("/query", methods=['POST'])
def query():
    logger.info("ENDPOINT /query")
    data = request.get_json()
    logger.info(f"QUERY DATA: {str(data)}")
    
    is_query_valid, message = validate_query(data)
    if not is_query_valid:
        logger.info('RETURN ERROR 400, INVALID QUERY')
        return {"error": message}, 400
    
    file_path = get_path_by_id(data['poolId'])
    if not does_path_exist(file_path):
        logger.info('RETURN ERROR 400, poolId does not exist')
        return {"error": "poolId does not exist"}, 400
    
    df_file_path = load_data(file_path)
    df_index_values = df_file_path.index.values
    
    if does_pool_exist(data["poolId"], df_index_values):
        sorted_pool_values_list, new_df, df_has_changed = sort_pool_values(data, df_file_path) # try sorting poolValues list
        
        if df_has_changed is True:
            save_data(file_path, new_df)
            
        quantile, total_elements = calculate_quantile(sorted_pool_values_list, data["percentile"])       
        resp = {"calculated_quantile": quantile, "total_count_of_elements": total_elements}
        logger.info(f'{resp}')
        return resp
    else:
        logger.info('RETURN ERROR 400, poolId does not exist')
        return {"error": "poolId does not exist"}, 400

def validate_pool(pool):
    logger.info("Check if pool data is valid")
    
    length_of_pool = len(pool)
    pool_keys_set = set(pool.keys())
    expected_set = set(['poolValues', 'poolId'])
    
    is_valid = False
    if (length_of_pool != 2) or (pool_keys_set != expected_set):
        message = "Pool must contain both 'poolId' and 'poolValues' and only contain this values"
    elif type(pool['poolId']) is not int:
        message = "'poolId' must be an integer"
    elif type(pool['poolValues']) is not list:
        message = "'poolValues' must be a list"
    elif len(pool['poolValues']) <1:
        message = "Number of elements in 'poolValues' must be greater than 0"
    elif any(map(lambda x: type(x) not in (int, float), pool['poolValues'])):
        message = "All elements of 'poolValues' must be real number"
    else:
        is_valid = True
        message = "Valid pool"
    return is_valid, message

def validate_query(query):
    logger.info("Check if query data is valid")
    
    length_of_query = len(query)
    query_keys_set = set(query.keys())
    expected_set = set(['percentile', 'poolId'])
    
    is_valid = False
    if (length_of_query != 2) or (query_keys_set!=expected_set):
        message = "Query must contain both 'poolId' and 'percentile' and only contain this values"
    elif type(query['poolId']) is not int:
        message = "'poolId' must be an integer"
    elif type(query['percentile']) not in (int,float):
        message = "'percentile' must be a real number"
    elif query['percentile'] < 0 or query['percentile'] > 100:
        message = "Percentiles must be in the range [0, 100]"
    else:
        is_valid = True
        message = "Valid query"
    return is_valid, message

def does_path_exist(path):
    logger.info("Check if given path exists")
    path_exists = os.path.exists(path)
    return path_exists

def does_pool_exist(id, list_id):
    logger.info("Check if poolId exists in the id list")
    pool_exists = (id in list_id)
    return pool_exists

def get_path_by_id(id):
    logger.info("Get file path by poolId")
    file_path = 'data/' + str(abs(id)//1000) + '.csv'
    return file_path

def load_data(path):
    logger.info("Load file into Dataframe")
    df = pd.read_csv(path, index_col= "poolId")
    return df

def save_data(path, df):
    logger.info("Write Dataframe to path")
    df.to_csv(path, index=True)

def calculate_quantile(sorted_list, percentile):
    logger.info("Compute the percentile quantile of the sorted list")
    
    total_elements = len(sorted_list)
    
    logger.info("Check for special cases to limit computation")
    
    if (total_elements == 1) or (sorted_list[0] == sorted_list[-1]) or (percentile == 0):
        logger.info("Special case")
        quantile = sorted_list[0]
    elif percentile == 100:
        logger.info("Special case")
        quantile = sorted_list[-1]
    else:
        rank = (total_elements - 1) * percentile/ 100
        left_index = max(0, math.floor(rank))
        right_index = min(total_elements-1, left_index+1)
        weight = rank - math.floor(rank)
        quantile = sorted_list[left_index] * (1-weight) + sorted_list[right_index] * weight
        
    return quantile, total_elements

def insert_pool(pool, current_df=None):
    logger.info("Write new pool data to Dataframe")
    
    total_elements = len(pool["poolValues"])
    pool["poolValues"] = str(pool["poolValues"])
    pool_df = pd.DataFrame([pool]).set_index('poolId')
    if total_elements == 1:
        pool_df['sorted'] = int(1) # no need to be sorted before calculating quantile
    else:
        pool_df['sorted'] = int(0) # need to be sorted before calculating quantile
    
    if current_df is None:
        new_df = pool_df
    else:
        new_df = pd.concat([current_df, pool_df])
    return new_df
    
def append_pool_values(pool, df):
    logger.info("In Dataframe, append poolValues to current poolValues corresponding to given poolId, update 'sorted' label to 0")
    
    current_pool = df.loc[pool["poolId"]]  
    current_pool_values_list = ast.literal_eval(current_pool["poolValues"]) # safely convert string to list

    new_pool_values_list = current_pool_values_list + pool["poolValues"]
      
    df.loc[pool['poolId'],['poolValues', 'sorted']] = str(new_pool_values_list), 0 # update values
    
    return df
    
def sort_pool_values(query, df):
    logger.info("Try sorting pool values for given poolId")
    queried_pool = df.loc[query["poolId"]]   
    
    queried_pool_values_list = ast.literal_eval(queried_pool["poolValues"]) # safely convert string to list
    
    if queried_pool["sorted"] == 0:
        logger.info(f"Sort pool values list: {queried_pool_values_list}, update 'sorted' label to 1")
        
        sorted_pool_values_list = sorted(queried_pool_values_list)
        df.loc[query['poolId'],['poolValues', 'sorted']] = str(sorted_pool_values_list), 1 
        df_has_changed = True
        
        return sorted_pool_values_list, df, df_has_changed # dataframe changed, need to update file
    else:
        logger.info("The pool values list is already sorted, no need to do anything")
        df_has_changed = False
        return queried_pool_values_list, df, df_has_changed # no need to update file

if __name__ == '__main__':
    app.run(host = '127.0.0.1', port = 1234, debug = True)