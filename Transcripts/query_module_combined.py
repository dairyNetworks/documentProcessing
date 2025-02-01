from pymongo import MongoClient

def find_top_documents_with_keyword_frequencies_or(db_name, tdm_collection_name, keywords, top_n=5, mongo_uri="mongodb://localhost:27017/"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    tdm_collection = db[tdm_collection_name]
    keywords = [keyword.replace(' ', '_') for keyword in keywords]
    
    pipeline = [
        {
            '$project': {
                'Speaker': 1,
                'Sliding_year': 1,
                **{keyword: {'$ifNull': [f'$term_document_matrix_dept.{keyword}', 0]} for keyword in keywords}
            }
        },
        {
            '$match': {
                '$or': [{keyword: {'$gt': 0}} for keyword in keywords]
            }
        },
        {
            '$group': {
                '_id': {'Speaker': '$Speaker', 'Sliding_year': '$Sliding_year'},
                'count': {'$sum': 1},
                **{keyword: {'$sum': f'${keyword}'} for keyword in keywords}
            }
        },
        {
            '$addFields': {
                'TotalFrequency': {
                    '$sum': [f'${keyword}' for keyword in keywords]
                }
            }
        },
        {'$sort': {'TotalFrequency': -1}},
        {'$limit': top_n},
        {
            '$project': {
                'Speaker': '$_id.Speaker',
                'Sliding_year': '$_id.Sliding_year',
                'count': 1,
                **{keyword: 1 for keyword in keywords}
            }
        }
    ]
    
    print("Pipeline:", pipeline)
    results = list(tdm_collection.aggregate(pipeline))
    print("Results:", results)
    
    for result in results:
        print(f"Speaker: {result['Speaker']}, Sliding_year: {result['Sliding_year']}, Documents Merged: {result['count']}")
        for keyword in keywords:
            print(f"  {keyword}: {result[keyword]}")
    
    return results           

def find_top_documents_with_keyword_frequencies_and(db_name, tdm_collection_name, keywords, top_n=5, mongo_uri="mongodb://localhost:27017/"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    tdm_collection = db[tdm_collection_name]
    keywords = [keyword.replace(' ', '_') for keyword in keywords]
    
    # Step 1: Ensure keywords are present in each unmerged term document matrix
    match_stage = {
        '$match': {
            **{f'term_document_matrix_dept.{keyword}': {'$gt': 0} for keyword in keywords}
        }
    }
    
    # Step 2: Sum up the keywords and their frequencies from the combined term document matrix
    group_stage = {
        '$group': {
            '_id': {'Speaker': '$Speaker', 'Sliding_year': '$Sliding_year'},
            'count': {'$sum': 1},
            **{keyword: {'$sum': f'$term_document_matrix_dept.{keyword}'} for keyword in keywords}
        }
    }
    
    project_stage = {
        '$project': {
            'Speaker': '$_id.Speaker',
            'Sliding_year': '$_id.Sliding_year',
            'count': 1,
            **{keyword: 1 for keyword in keywords}
        }
    }
    
    add_fields_stage = {
        '$addFields': {
            'TotalFrequency': {
                '$sum': [f'${keyword}' for keyword in keywords]
            }
        }
    }
    
    sort_stage = {'$sort': {'TotalFrequency': -1}}
    limit_stage = {'$limit': top_n}
    
    pipeline = [
        match_stage,
        group_stage,
        project_stage,
        add_fields_stage,
        sort_stage,
        limit_stage
    ]
    
    print("Pipeline:", pipeline)
    results = list(tdm_collection.aggregate(pipeline))
    print("Results:", results)
    
    for result in results:
        print(f"Speaker: {result['Speaker']}, Sliding_year: {result['Sliding_year']}, Count: {result['count']}")
        for keyword in keywords:
            print(f"  {keyword}: {result[keyword]}")
    
    return results

def function_call_combined(keywords):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['transcripts']
    speaker_collection = db['speaker_data']
    keyword_list = []
    
    if 'OR' in keywords:
        print('OR')
        keyword_list = [keyword.strip() for keyword in keywords.split('OR')]
        top_documents = find_top_documents_with_keyword_frequencies_or('transcripts', 'P1_speaker_speech', keyword_list, top_n=5)
    elif 'AND' in keywords:
        print('AND')
        keyword_list = [keyword.strip() for keyword in keywords.split('AND')]
        top_documents = find_top_documents_with_keyword_frequencies_and('transcripts', 'P1_speaker_speech', keyword_list, top_n=5)
    else:
        keyword_list.insert(0,str(keywords))
        top_documents = find_top_documents_with_keyword_frequencies_or('transcripts', 'P1_speaker_speech', keyword_list, top_n=5)

    for doc in top_documents:
        speaker = doc['Speaker']
        sliding_year = doc['Sliding_year']
        additional_info = speaker_collection.find_one({'Speaker':speaker, 'Sliding_year':sliding_year})
        if additional_info:
            doc['Organisation'] = additional_info.get('Organisation')
            doc['Designation'] = additional_info.get('Designation')
            doc['Region'] = additional_info.get('Region')
            
    return top_documents