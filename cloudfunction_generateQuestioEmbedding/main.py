import functions_framework
from openai import OpenAI
from pymongo import MongoClient
from bson.objectid import ObjectId
from pinecone import Pinecone
import os

@functions_framework.http
def generateQuestionEmbedding(request):
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
    pinecone_question_namespace = os.environ.get('pinecone_question_namespace')
    pinecone_answers_namespace = os.environ.get('pinecone_answers_namespace')
    
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
        
        embedded_data_questions = []
        embedded_data_answers = []
        
        for id in ids:
            mongoId = ObjectId(id)
            
            mongo_result = source_collection.find_one({"_id": mongoId})
            
            if mongo_result is None:
                print("No result found in {} for id {}".format(collection_name_source, id))
                continue
            
            print("Generating Embedding for id {}".format(id))

            question = mongo_result.get("question")
            answer = mongo_result.get("answer")

            embedded_question= openai_client.embeddings.create(
                    input=question, 
                    model=embedding_model,
                ).data[0].embedding
            
            temp_question = {
                "id": id,
                "values": embedded_question,
                "metadata":{
                    "mongo_data_id": id,
                    "mongo_collection_name": collection_name_source,
                    "filed": "question"
                }
            }
            
            embedded_answers = openai_client.embeddings.create(
                    input=answer, 
                    model=embedding_model,
                ).data[0].embedding
            
            temp_answer = {
                "id": id,
                "values": embedded_answers,
                "metadata":{
                    "mongo_data_id": id,
                    "mongo_collection_name": collection_name_source,
                    "filed": "answer"
                }
            }

            embedded_data_questions.append(temp_question)
            embedded_data_answers.append(temp_answer)

        index.upsert(embedded_data_questions, namespace=pinecone_question_namespace)
        index.upsert(embedded_data_answers, namespace=pinecone_answers_namespace)
        mongo_client.close()
        return "Success" 
    except Exception as e:
        print(e)
        return "Fail"