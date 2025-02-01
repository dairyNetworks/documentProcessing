from pymongo import MongoClient

def find_top_documents_with_keyword_frequencies_or(db_name, tdm_collection_name, keywords, top_n=5, mongo_uri="mongodb://localhost:27017/"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    tdm_collection = db[tdm_collection_name]
    keywords = [keyword.replace(' ', '_') for keyword in keywords]
    
    pipeline = [
        {
            '$project': {
                'File Name': 1,
                **{keyword: {'$ifNull': [f'$Term Document Matrix.{keyword}', 0]} for keyword in keywords}
            }
        },
        {
            '$addFields': {
                'TotalFrequency': {
                    '$sum': [f'${keyword}' for keyword in keywords]
                }
            }
        },
        {
            '$match': {
                'TotalFrequency': {'$gt': 0}
            }
        },
        {'$sort': {'TotalFrequency': -1}},
        {'$limit': top_n},
        {
            '$project': {
                'File Name': 1,
                **{keyword: 1 for keyword in keywords}
            }
        }
    ]
    
    print("Pipeline:", pipeline)
    results = list(tdm_collection.aggregate(pipeline))
    print("Results:", results)
    
    for result in results:
        print(f"File Name: {result['File Name']}")
        for keyword in keywords:
            print(f"  {keyword}: {result[keyword]}")
    
    return results

def find_top_documents_with_keyword_frequencies_and(db_name, tdm_collection_name, keywords, top_n=5, mongo_uri="mongodb://localhost:27017/"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    tdm_collection = db[tdm_collection_name]
    keywords = [keyword.replace(' ', '_') for keyword in keywords]
    
    pipeline = [
        {
            '$project': {
                'File Name': 1,
                **{keyword: {'$ifNull': [f'$Term Document Matrix.{keyword}', 0]} for keyword in keywords}
            }
        },
        {
            '$match': {
                **{keyword: {'$gt': 0} for keyword in keywords}
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
                'File Name': 1,
                **{keyword: 1 for keyword in keywords}
            }
        }
    ]
    
    print("Pipeline:", pipeline)
    results = list(tdm_collection.aggregate(pipeline))
    print("Results:", results)
    for result in results:
        print(f"File Name: {result['File Name']}")
        for keyword in keywords:
            print(f"  {keyword}: {result[keyword]}")
    
    return results

def function_call(keywords):
    keyword_list = []
    
    if 'OR' in keywords:
        print('OR')
        keyword_list = [keyword.strip() for keyword in keywords.split('OR')]
        top_documents = find_top_documents_with_keyword_frequencies_or('transcripts', 'complete_documents', keyword_list, top_n=5)
    elif 'AND' in keywords:
        print('AND')
        keyword_list = [keyword.strip() for keyword in keywords.split('AND')]
        top_documents = find_top_documents_with_keyword_frequencies_and('transcripts', 'complete_documents', keyword_list, top_n=5)
    else:
        keyword_list.insert(0,str(keywords))
        top_documents = find_top_documents_with_keyword_frequencies_or('transcripts', 'complete_documents', keyword_list, top_n=5)
    
    return top_documents