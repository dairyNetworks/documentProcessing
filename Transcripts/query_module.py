from pymongo import MongoClient

def find_top_documents_with_keyword_frequencies_or(db_name, tdm_collection_name, keywords, speaker, top_n=15, mongo_uri="mongodb://localhost:27017/"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    tdm_collection = db[tdm_collection_name]
    keywords = [keyword.replace(' ', '_') for keyword in keywords]
    
    pipeline = [
        {
            '$match': {
                'Speaker': speaker,
                '$or': [{f'term_document_matrix_dept.{keyword}': {'$exists': True}} for keyword in keywords]
            }
        },
        {
            '$project': {
                'Speaker': 1,
                'file_name': 1,
                'Exact Text': {
                    '$replaceAll': {
                        'input': {
                            '$replaceAll': {
                                'input': '$Exact Text',
                                'find': '- ',
                                'replacement': ''
                            }
                        },
                        'find': '\xad',
                        'replacement': ''
                    }
                },
                **{keyword: {'$ifNull': [f'$term_document_matrix_dept.{keyword}', 0]} for keyword in keywords}
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
                'Speaker': 1,
                'file_name': 1,
                'Exact Text': 1,
                **{keyword: 1 for keyword in keywords}
            }
        }
    ]
    
    #print("Pipeline:", pipeline)
    results = list(tdm_collection.aggregate(pipeline))
    #print("Results:", results)
    
    for result in results:
        print(f"File Name: {result['file_name']}, Speaker: {result['Speaker']}, Exact Text: {result['Exact Text']}")
        for keyword in keywords:
            print(f"  {keyword}: {result[keyword]}")
    
    return results

def find_top_documents_with_keyword_frequencies_and(db_name, tdm_collection_name, keywords, speaker, top_n=15, mongo_uri="mongodb://localhost:27017/"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    tdm_collection = db[tdm_collection_name]
    keywords = [keyword.replace(' ', '_') for keyword in keywords]
    
    pipeline = [
        {
            '$match': {
                'Speaker': speaker,
                **{f'term_document_matrix_dept.{keyword}': {'$gt': 0} for keyword in keywords}
            }
        },
        {
            '$project': {
                'Speaker': 1,
                'file_name': 1,
                'Exact Text': {
                    '$replaceAll': {
                        'input': {
                            '$replaceAll': {
                                'input': '$Exact Text',
                                'find': '- ',
                                'replacement': ''
                            }
                        },
                        'find': '\xad',
                        'replacement': ''
                    }
                },
                **{keyword: {'$ifNull': [f'$term_document_matrix_dept.{keyword}', 0]} for keyword in keywords}
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
                'Speaker': 1,
                'file_name': 1,
                'Exact Text': 1,
                **{keyword: 1 for keyword in keywords}
            }
        }
    ]
    
    results = list(tdm_collection.aggregate(pipeline))
    
    for result in results:
        print(f"File Name: {result['file_name']}, Speaker: {result['Speaker']}, Exact Text: {result['Exact Text']}")
        for keyword in keywords:
            print(f"  {keyword}: {result[keyword]}")
    
    return results

def function_call_speaker(keywords, speaker):
    keyword_list = []
    
    if 'OR' in keywords:
        print('OR')
        keyword_list = [keyword.strip() for keyword in keywords.split('OR')]
        top_documents = find_top_documents_with_keyword_frequencies_or('transcripts', 'P1_speaker_speech', keyword_list, speaker, top_n=15)
    elif 'AND' in keywords:
        print('AND')
        keyword_list = [keyword.strip() for keyword in keywords.split('AND')]
        top_documents = find_top_documents_with_keyword_frequencies_and('transcripts', 'P1_speaker_speech', keyword_list, speaker, top_n=15)
    else:
        keyword_list.insert(0,str(keywords))
        top_documents = find_top_documents_with_keyword_frequencies_or('transcripts', 'P1_speaker_speech', keyword_list, speaker, top_n=15)
    
    return top_documents
