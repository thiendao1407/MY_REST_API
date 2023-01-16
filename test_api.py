import pytest
from api import insert_pool, append_pool_values, sort_pool_values, get_path_by_id
import requests
import os
import numpy as np
import pandas as pd

class TestUpdate(object):
    URL_update = "http://127.0.0.1:1234/update"
    
    def test_update_insert_then_append_pool(self):
        #setup
        pool_1 = {"poolId": 99991369, "poolValues": [1, 7, 2, 6, 5.5, 3.141592653589793]}
        pool_2 = {"poolId": 99991369, "poolValues": [2]}
        
        # clear file path if exxists
        file_path_created_by_calling_api = get_path_by_id(pool_1["poolId"])
        if os.path.exists(file_path_created_by_calling_api):
            os.remove(file_path_created_by_calling_api)
                    
        response_1 = requests.post(self.URL_update, json = pool_1, headers = {"Content-Type": "application/json"})
        response_2 = requests.post(self.URL_update, json = pool_2, headers = {"Content-Type": "application/json"})
        data_1 = response_1.json()
        data_2 = response_2.json()
  
        #assert
        assert response_1.status_code == 200
        assert data_1['status'] == "inserted"
        assert response_2.status_code == 200
        assert data_2['status'] == "appended"
        
        #teardown
        os.remove(file_path_created_by_calling_api)
        
    def test_update_just_insert_pool(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [1, 7, 2, 6, 5.5, 3.141592653589793]}

        # clear file path if exxists
        file_path_created_by_calling_api = get_path_by_id(pool["poolId"]) 
        if os.path.exists(file_path_created_by_calling_api):
            os.remove(file_path_created_by_calling_api)
                    
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
       
        #assert
        assert response.status_code == 200
        assert data['status'] == "inserted"

		#teardown
        os.remove(file_path_created_by_calling_api)
        
    def test_update_list_has_string_element(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [1, 2, 4, 3, 5, 6, "7"]}
        expected_message = "All elements of 'poolValues' must be real number"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_update_list_has_nest_list(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [[2,3,4], [1,2,3]]}
        expected_message = "All elements of 'poolValues' must be real number"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_update_list_has_bool_element(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [True]}
        expected_message = "All elements of 'poolValues' must be real number"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_update_empty_list(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": []}
        expected_message = "Number of elements in 'poolValues' must be greater than 0"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_update_poolvalues_is_string(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": "abcd"}
        expected_message = "'poolValues' must be a list"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_update_poolid_is_string(self):
        #setup
        pool = {"poolId": "abcd", "poolValues": [1]}
        expected_message = "'poolId' must be an integer"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_update_wrong_key_name(self):
        #setup
        pool = {"POOLID": 99991369, "poolValues": [1]}
        expected_message = "Pool must contain both 'poolId' and 'poolValues' and only contain this values"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_update_lacking_poolid(self):
        #setup
        pool = {"poolValues": [1]}
        expected_message = "Pool must contain both 'poolId' and 'poolValues' and only contain this values"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_update_lacking_poolvalues(self):
        #setup
        pool = {"poolId": 99991369}
        expected_message = "Pool must contain both 'poolId' and 'poolValues' and only contain this values"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_update_unknown_key(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [1], "unknown_key": 4444}
        expected_message = "Pool must contain both 'poolId' and 'poolValues' and only contain this values"
        
        response = requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message

class TestQuery(object):
    URL_query = "http://127.0.0.1:1234/query"
    URL_update = "http://127.0.0.1:1234/update"
    
    def test_query_percentile_less_than_0(self):
        #setup
        query = {"poolId": 99991369, "percentile": -1}       
        expected_message = "Percentiles must be in the range [0, 100]"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_query_percentile_greater_than_100(self):
        #setup
        query = {"poolId": 99991369, "percentile": 100.1}     
        expected_message = "Percentiles must be in the range [0, 100]"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_query_poolid_does_not_exist(self):
        #setup
        query = {"poolId": 99991369, "percentile": 90}       
        expected_message = "poolId does not exist"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_query_percentile_is_string(self):
        #setup
        query = {"poolId": 99991369, "percentile": "a"}     
        expected_message = "'percentile' must be a real number"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_query_poolid_is_string(self):
        #setup
        query = {"poolId": "abcd", "percentile": 99.5}      
        expected_message = "'poolId' must be an integer"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_query_wrong_key_name_1(self):
        #setup
        query = {"POOLID": 99991369, "percentile": 99.5}     
        expected_message = "Query must contain both 'poolId' and 'percentile' and only contain this values"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_query_wrong_key_name_2(self):
        #setup
        query = {"poolId": 99991369, "percENTIle": 99.5}     
        expected_message = "Query must contain both 'poolId' and 'percentile' and only contain this values"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        #assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_query_lacking_poolid(self):
        # setup
        query = {"percentile": 1}     
        expected_message = "Query must contain both 'poolId' and 'percentile' and only contain this values"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        # assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_query_lacking_poolvalues(self):
        #setup
        query = {"poolId": 99991369}       
        expected_message = "Query must contain both 'poolId' and 'percentile' and only contain this values"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        # assert
        assert response.status_code == 400
        assert data['error'] == expected_message

    def test_query_unknown_key(self):
        #setup
        query = {"poolId": 99991369, "percentile": 1, "unknown_key": 4444}      
        expected_message = "Query must contain both 'poolId' and 'percentile' and only contain this values"
        
        response = requests.post(self.URL_query, json = query, headers = {"Content-Type": "application/json"})
        data = response.json()
        
        # assert
        assert response.status_code == 400
        assert data['error'] == expected_message
        
    def test_query_valid_data(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [1, 7, 2, 6, 5.5, 2, -2, -3, -2, -3]}
        values_list = pool['poolValues']
        total_elements = len(values_list)

        file_path_created_by_calling_api = get_path_by_id(pool["poolId"]) 
        if os.path.exists(file_path_created_by_calling_api):
            os.remove(file_path_created_by_calling_api)
        
        requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        
        query_1 = {"poolId": 99991369, "percentile": 90}
        query_2 = {"poolId": 99991369, "percentile": 0}  
        query_3 = {"poolId": 99991369, "percentile": 100}       
        
        response_1 = requests.post(self.URL_query, json = query_1, headers = {"Content-Type": "application/json"})
        response_2 = requests.post(self.URL_query, json = query_2, headers = {"Content-Type": "application/json"})
        response_3 = requests.post(self.URL_query, json = query_3, headers = {"Content-Type": "application/json"})
        data_1 = response_1.json()
        data_2 = response_2.json()
        data_3 = response_3.json()

        #assert
        assert response_1.status_code == 200
        assert data_1['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_1["percentile"]/100))
        assert data_1['total_count_of_elements'] == total_elements
        assert response_2.status_code == 200
        assert data_2['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_2["percentile"]/100))
        assert data_2['total_count_of_elements'] == total_elements
        assert response_3.status_code == 200
        assert data_3['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_3["percentile"]/100))
        assert data_3['total_count_of_elements'] == total_elements
        
        #teardown
        os.remove(file_path_created_by_calling_api)

    def test_query_list_has_two_elements(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [3,1]}
        values_list = pool['poolValues']
        total_elements = len(values_list)
        
        file_path_created_by_calling_api = get_path_by_id(pool["poolId"]) 
        if os.path.exists(file_path_created_by_calling_api):
            os.remove(file_path_created_by_calling_api)
            
        requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        
        query_1 = {"poolId": 99991369, "percentile": 90}
        query_2 = {"poolId": 99991369, "percentile": 0}  
        query_3 = {"poolId": 99991369, "percentile": 100}       
        
        response_1 = requests.post(self.URL_query, json = query_1, headers = {"Content-Type": "application/json"})
        response_2 = requests.post(self.URL_query, json = query_2, headers = {"Content-Type": "application/json"})
        response_3 = requests.post(self.URL_query, json = query_3, headers = {"Content-Type": "application/json"})
        data_1 = response_1.json()
        data_2 = response_2.json()
        data_3 = response_3.json()

        #assert
        assert response_1.status_code == 200
        assert data_1['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_1["percentile"]/100))
        assert data_1['total_count_of_elements'] == total_elements
        assert response_2.status_code == 200
        assert data_2['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_2["percentile"]/100))
        assert data_2['total_count_of_elements'] == total_elements
        assert response_3.status_code == 200
        assert data_3['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_3["percentile"]/100))
        assert data_3['total_count_of_elements'] == total_elements
        
        #teardown
        os.remove(file_path_created_by_calling_api)

    def test_query_list_has_identical_elements_1(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [2,2]}
        values_list = pool['poolValues']
        total_elements = len(values_list)
        
        file_path_created_by_calling_api = get_path_by_id(pool["poolId"]) 
        if os.path.exists(file_path_created_by_calling_api):
            os.remove(file_path_created_by_calling_api)
        
        requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        
        query_1 = {"poolId": 99991369, "percentile": 90}
        query_2 = {"poolId": 99991369, "percentile": 0}  
        query_3 = {"poolId": 99991369, "percentile": 100}       
        
        response_1 = requests.post(self.URL_query, json = query_1, headers = {"Content-Type": "application/json"})
        response_2 = requests.post(self.URL_query, json = query_2, headers = {"Content-Type": "application/json"})
        response_3 = requests.post(self.URL_query, json = query_3, headers = {"Content-Type": "application/json"})
        data_1 = response_1.json()
        data_2 = response_2.json()
        data_3 = response_3.json()

        #assert
        assert response_1.status_code == 200
        assert data_1['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_1["percentile"]/100))
        assert data_1['total_count_of_elements'] == total_elements
        assert response_2.status_code == 200
        assert data_2['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_2["percentile"]/100))
        assert data_2['total_count_of_elements'] == total_elements
        assert response_3.status_code == 200
        assert data_3['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_3["percentile"]/100))
        assert data_3['total_count_of_elements'] == total_elements
        
        #teardown
        os.remove(file_path_created_by_calling_api)
        
    def test_query_list_has_identical_elements_2(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [3,3,3]}
        values_list = pool['poolValues']
        total_elements = len(values_list)
        
        file_path_created_by_calling_api = get_path_by_id(pool["poolId"]) 
        if os.path.exists(file_path_created_by_calling_api):
            os.remove(file_path_created_by_calling_api)
        
        requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        
        query_1 = {"poolId": 99991369, "percentile": 90}
        query_2 = {"poolId": 99991369, "percentile": 0}  
        query_3 = {"poolId": 99991369, "percentile": 100}       
        
        response_1 = requests.post(self.URL_query, json = query_1, headers = {"Content-Type": "application/json"})
        response_2 = requests.post(self.URL_query, json = query_2, headers = {"Content-Type": "application/json"})
        response_3 = requests.post(self.URL_query, json = query_3, headers = {"Content-Type": "application/json"})
        data_1 = response_1.json()
        data_2 = response_2.json()
        data_3 = response_3.json()

        #assert
        assert response_1.status_code == 200
        assert data_1['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_1["percentile"]/100))
        assert data_1['total_count_of_elements'] == total_elements
        assert response_2.status_code == 200
        assert data_2['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_2["percentile"]/100))
        assert data_2['total_count_of_elements'] == total_elements
        assert response_3.status_code == 200
        assert data_3['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_3["percentile"]/100))
        assert data_3['total_count_of_elements'] == total_elements
        
        #teardown
        os.remove(file_path_created_by_calling_api)

    def test_query_list_has_one_element(self):
        #setup
        pool = {"poolId": 99991369, "poolValues": [1]}
        values_list = pool['poolValues']
        total_elements = len(values_list)
        
        requests.post(self.URL_update, json = pool, headers = {"Content-Type": "application/json"})
        file_path_created_by_calling_api = get_path_by_id(pool["poolId"])
        
        query_1 = {"poolId": 99991369, "percentile": 90}
        query_2 = {"poolId": 99991369, "percentile": 0}  
        query_3 = {"poolId": 99991369, "percentile": 100}       
        
        response_1 = requests.post(self.URL_query, json = query_1, headers = {"Content-Type": "application/json"})
        response_2 = requests.post(self.URL_query, json = query_2, headers = {"Content-Type": "application/json"})
        response_3 = requests.post(self.URL_query, json = query_3, headers = {"Content-Type": "application/json"})
        data_1 = response_1.json()
        data_2 = response_2.json()
        data_3 = response_3.json()

        #assert
        assert response_1.status_code == 200
        assert data_1['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_1["percentile"]/100))
        assert data_1['total_count_of_elements'] == total_elements
        assert response_2.status_code == 200
        assert data_2['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_2["percentile"]/100))
        assert data_2['total_count_of_elements'] == total_elements
        assert response_3.status_code == 200
        assert data_3['calculated_quantile'] == pytest.approx(np.quantile(values_list, query_3["percentile"]/100))
        assert data_3['total_count_of_elements'] == total_elements
        
        #teardown
        os.remove(file_path_created_by_calling_api)

class TestInsertPool(object): 
    def test_insert_poolvalues_has_one_element(self):
        # setup
        pool = {"poolId": 1111, "poolValues": [2]}
        new_df = insert_pool(pool)
        # assert
        assert len(new_df) == 1
        assert new_df.loc[pool["poolId"]]["poolValues"] == str(pool["poolValues"])
        assert new_df.loc[pool["poolId"]]["sorted"] == 1 # because poolValues has just 1 element
    
    def test_insert_poolvalues_has_more_elements(self):
        # setup
        pool = {"poolId": 1111, "poolValues": [2,3]}
        new_df = insert_pool(pool)
        # assert
        assert len(new_df) == 1
        assert new_df.loc[pool["poolId"]]["poolValues"] == str(pool["poolValues"])
        assert new_df.loc[pool["poolId"]]["sorted"] == 0 # because poolValues has more than 1 element
    
    def test_insert_poolvalues_has_one_element_to_current_df(self):
        # setup
        # Note that since it is handled by the does_pool_exist function, pool["poolId"] will not match any poolId in current_df
        pool = {"poolId": 1111, "poolValues": [2]}
        current_df = pd.DataFrame([{"poolId": 2222, "poolValues": "[1, 3]", "sorted": 0},
                                   {"poolId": 3333, "poolValues": str(sorted([-1, 2])), "sorted": 1},
                                   {"poolId": 4444, "poolValues": "[1, 3, 5]", "sorted": 0}]).set_index('poolId')
        
        new_df = insert_pool(pool, current_df)
        diff_df = pd.concat([new_df,current_df]).drop_duplicates(keep=False) 
        # assert
        # to make sure the only difference between new_df and current_df is caused by pool data
        assert len(diff_df) == 1
        assert diff_df.loc[pool["poolId"]]["poolValues"] == str(pool["poolValues"])
        assert new_df.loc[pool["poolId"]]["sorted"] == 1 # because poolValues has just 1 element
    
    def test_insert_poolvalues_has_more_elements_to_current_df(self):
        # setup
        # Note that since it is handled by the does_pool_exist function, pool["poolId"] will not match any poolId in current_df
        pool = {"poolId": 1111, "poolValues": [2,3]}
        current_df = pd.DataFrame([{"poolId": 2222, "poolValues": "[1, 3]", "sorted": 0},
                                   {"poolId": 3333, "poolValues": str(sorted([-1, 2])), "sorted": 1},
                                   {"poolId": 4444, "poolValues": "[1, 3, 5]", "sorted": 0}]).set_index('poolId')
        
        new_df = insert_pool(pool, current_df)
        diff_df = pd.concat([new_df,current_df]).drop_duplicates(keep=False) 
        # assert
        # to make sure the only difference between new_df and current_df is caused by pool data
        assert len(diff_df) == 1
        assert diff_df.loc[pool["poolId"]]["poolValues"] == str(pool["poolValues"])
        assert diff_df.loc[pool["poolId"]]["sorted"] == 0 # because poolValues has more than 1 element
            
class TestAppendPoolValues(object): 
    def test_append_poolvalues(self):
        #setup
        # Note that since it is handled by the does_pool_exist function, pool["poolId"] will match with one poolId in current_df
        pool = {"poolId": 3333, "poolValues": [2,3]}
        dict_list = [{"poolId": 3333, "poolValues": str(sorted([-1, 2])), "sorted": 1},
                                   {"poolId": 4444, "poolValues": "[1, 3, 5]", "sorted": 0}]
        
        df = pd.DataFrame(dict_list).set_index('poolId')  
        tally_df = pd.DataFrame(dict_list).set_index('poolId')    
        tally_df = tally_df[(tally_df.index != pool["poolId"])]
        
        df = append_pool_values(pool, df)
        
        diff_df = pd.concat([df,tally_df]).drop_duplicates(keep=False)
        
        # assert
        assert len(diff_df) == 1
        assert diff_df.loc[pool["poolId"]]["poolValues"] == str([-1, 2] + pool["poolValues"])
        assert diff_df.loc[pool["poolId"]]["sorted"] == 0 # 'sorted' changed from 1 to 0 by appending new values
        
class TestSortPoolValues(object): 
    def test_sort_poolvalues_already_sorted(self):
        # Sort an already sorted row in Dataframe, need to confirm nothing changed
        #setup
        query = {"poolId": 3333, "percentile": 90}  
        dict_list = [{"poolId": 3333, "poolValues": str(sorted([-1, 2])), "sorted": 1},
                                   {"poolId": 4444, "poolValues": "[1, 3, 5, 4]", "sorted": 0}]
        df = pd.DataFrame(dict_list).set_index('poolId')
        tally_df = pd.DataFrame(dict_list).set_index('poolId')
        
        queried_pool_values_list, df, df_has_changed = sort_pool_values(query, df)   
        diff_df = pd.concat([df,tally_df]).drop_duplicates(keep=False) 
        
        #assert
        assert type(queried_pool_values_list) is list
        assert str(queried_pool_values_list) == "[-1, 2]"
        assert len(diff_df) == 0
        assert df_has_changed == False
        
    def test_sort_pool_values_unsorted(self):
        # Sort the poolValues of an unsorted row in Dataframe
        #setup
        query = {"poolId": 4444, "percentile": 90}  
        dict_list = [{"poolId": 3333, "poolValues": str(sorted([-1, 2])), "sorted": 1},
                                   {"poolId": 4444, "poolValues": "[1, 3, 5, 4]", "sorted": 0}]
        
        df = pd.DataFrame(dict_list).set_index('poolId')
        tally_df = pd.DataFrame(dict_list).set_index('poolId')
        tally_df = tally_df[(tally_df.index != query["poolId"])]
        
        queried_pool_values_list, df, df_has_changed = sort_pool_values(query, df)   
        diff_df = pd.concat([df,tally_df]).drop_duplicates(keep=False) 
        
        #assert
        assert type(queried_pool_values_list) is list
        assert str(queried_pool_values_list) == str(sorted([1, 3, 5, 4]))
        assert df.loc[query["poolId"]]["sorted"] == 1
        assert len(diff_df) == 1
        assert diff_df.index[0] == query["poolId"]
        assert df_has_changed == True
        
        