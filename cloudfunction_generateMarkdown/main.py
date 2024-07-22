import functions_framework
from openai import OpenAI
from pymongo.mongo_client import MongoClient
from bson.objectid import ObjectId
import os

@functions_framework.http
def generateMarkdown(request):
    """HTTP Cloud Function.
    Generates markdown summary by reading mongodb
    """
    request_json = request.get_json(silent=True)
    request_args = request.args
    if request_json and 'mongoId' in request_json:
        mongoId = request_json['mongoId']
    elif request_args and 'mongoId' in request_args:
        mongoId = request_args['mongoId']
    else:
        mongoId = None
        
    if mongoId is None:
        return "Fail"
    else:
        mongoId = ObjectId(mongoId)
    
    GPT_MODEL = os.environ.get('GPT_MODEL') 
    key = os.environ.get('GPT_key') 
    mongo_url = os.environ.get('mongo_url') 
    db_name = os.environ.get('db_name') 
    collection_name_source = os.environ.get('collection_name_source') 
    collection_name_target = os.environ.get('collection_name_target') 
    
    try:
        # OpenAI client
        openai_client = OpenAI(api_key=key)
        
        # mongo client 
        mongo_client = MongoClient(mongo_url)
        db = mongo_client[db_name]
        source_collection = db[collection_name_source]
        target_collection = db[collection_name_target]
        
        # get source data
        mongo_result = source_collection.find_one({"_id": mongoId})
        if mongo_result is None:
            return "Fail"
        learnings = mongo_result.get("LearningOutcomes")
        text = mongo_result.get("Summary")
        topic = mongo_result.get("NameOfTheTopic")
        
        # traget markdown list
        markdown_id_list = []

        system_content = f"""
        You are a expert in the content: {text}. Users will ask you for explanation on topics from the content, return the explaination in a summary manner formated as markdown 
        """

        for i, learning in enumerate(learnings):
            print("Generating summary for learning count {}".format(i))
            query = f"""
            From your experties in the content explain me topic: {learning}
            """

            response = openai_client.chat.completions.create(
                messages=[
                    {
                    'role': 'system', 
                    'content': system_content
                    },
                    {
                    'role': 'user', 
                    'content': query
                    },
                ],
                model=GPT_MODEL,
                temperature=0,
            )

            markdown_summary = response.choices[0].message.content

            new_document = {
            "NameOfTheTopic": topic,
            "LearningSummary": markdown_summary,
            "Learning": learning
            }

            temp_result = target_collection.insert_one(new_document)

            markdown_id_list.append(temp_result.inserted_id)

        if markdown_id_list:
            source_collection.update_one({"_id": mongoId}, {"$set": {"LearningSummaryMarkdowns": markdown_id_list, "Status": True}})

        mongo_client.close()
        return "Success"
    except Exception as e:
        return "Fail"