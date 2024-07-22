import functions_framework
from openai import OpenAI
from pymongo import MongoClient
from bson.objectid import ObjectId
from pinecone import Pinecone
import os

@functions_framework.http
def generateMarkdownEmbedding(request):
    """HTTP Cloud Function.
    Generates markdown summary by reading mongodb
    """
    request_json = request.get_json(silent=True)
    request_args = request.args
    if request_json and 'mongoIds' in request_json:
        ids = request_json['mongoIds']
    elif request_args and 'mongoIds' in request_args:
        ids = request_args['mongoIds']
    else:
        ids = []
    
    print(ids)

    if ids == []:
        print(1)
        return "Fail"
    
    embedding_model = os.environ.get('embedding_model') 
    key = os.environ.get('GPT_key') 
    mongo_url = os.environ.get('mongo_url') 
    db_name = os.environ.get('db_name') 
    collection_name_source = os.environ.get('collection_name_source') 
    key_pinecone = os.environ.get('key_pinecone') 
    index_name = os.environ.get('index_name') 
    
    try:
        #open AI
        openai_client = OpenAI(api_key=key)
        # MongoDB
        mongo_client = MongoClient(mongo_url)
        db = mongo_client[db_name]
        source_collection = db[collection_name_source]
        # Pinecone
        pinecone = Pinecone(api_key=key_pinecone)
        index = pinecone.Index(name=index_name)
        
        embedded_data = []
        
        for id in ids:
            mongoId = ObjectId(id)
            
            mongo_result = source_collection.find_one({"_id": mongoId})
            
            if mongo_result is None:
                print("No result found in {} for id {}".format(collection_name_source, id))
                continue
            
            print("Generating Embedding for id {}".format(id))

            text = mongo_result.get("LearningSummary")
            
            text = text.replace("\n", " ")
            embedded_query = openai_client.embeddings.create(
                    input=text, 
                    model=embedding_model,
                ).data[0].embedding
            
            temp = {
                "id": id,
                "values": embedded_query,
                "metadata":{
                    "mongo_data_id": id,
                    "mongo_collection_name": collection_name_source
                }
            }

            embedded_data.append(temp)

        index.upsert(embedded_data)
        mongo_client.close()
        return "Success" 
    except Exception as e:
        print(e)
        return "Fail"