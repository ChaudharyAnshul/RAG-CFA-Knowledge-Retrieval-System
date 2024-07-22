import openai
from pymongo import MongoClient
from bson.objectid import ObjectId
from pinecone import Pinecone
import os
from dotenv import load_dotenv
import pymongo
import certifi
import csv
import pandas as pd

load_dotenv()

def process_documents(all_documents, collection_los, los_pinecone, key):
    data = []
    correct = 0
    for document in all_documents:
        question = document['question']

        if key == 0:
            set_name = "A"
            answer = (((document['answer']).split())[4])[0]
        elif key == 1:
            set_name = "B"
            answer = (((document['answer']).split())[0])[0]

        embedded_question = openai.Embedding.create(
            input=question, 
            model=os.getenv('embedding_model'),
        ).data[0].embedding

        found = los_pinecone.query(vector=embedded_question, top_k=3, include_metadata=True)
        context = ''

        for each in found['matches']:
            mongoId = ObjectId(each['id'])
            mongo_result = collection_los.find_one({"_id": mongoId})
            los = mongo_result.get("LearningSummary")
            context += los

        query = "Based on this context" + context + "Answer the following question by only giving the correct option id (like A or B or C or D) and not any other text " + question

        messages = [{"role": "user", "content": query}]

        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo-0125",
            messages=messages,
            temperature=0.1
        )
        gpt_response = response['choices'][0]['message']['content']

        match = 1 if gpt_response[0] == answer else 0
        correct += match

        question = question.replace('\n', ' ')
        data.append({'Set': set_name, 'Question': question, 'GPT Answer': gpt_response[0], 'KB Answer': answer, 'Match': match})

    return correct, data

def main():
    key = os.getenv('GPT_key') 
    mongo_url = os.getenv('mongo_url') 
    db_name = os.getenv('db_name') 
    collection_los_name = os.getenv('collection_los') 
    collection_set_A_name = os.getenv('collection_set_A') 
    collection_set_B_name = os.getenv('collection_set_B') 
    collection_part_4_report = os.getenv('collection_part_4_report')
    key_pinecone = os.getenv('key_pinecone') 
    index_name = os.getenv('index_name')


    openai.api_key = key
    client = pymongo.MongoClient(mongo_url,tlsCAFile=certifi.where())
    db = client[db_name]

    collection_set_A = db[collection_set_A_name]
    collection_set_B = db[collection_set_B_name]
    collection_los = db[collection_los_name]
    collection_part_4_report = db[collection_part_4_report]

    pinecone = Pinecone(api_key=key_pinecone)
    los_pinecone = pinecone.Index(name=index_name)
    
    all_documents_A = collection_set_A.find()
    all_documents_B = collection_set_B.find()

    correct_A, data_A = process_documents(all_documents_A, collection_los, los_pinecone, 0)
    correct_B, data_B = process_documents(all_documents_B, collection_los, los_pinecone, 1)

    print("Correct answers for set A:", correct_A)
    print("Correct answers for set B:", correct_B)

    all_data = data_A + data_B

    df = pd.DataFrame(all_data)
    collection_part_4_report.insert_many(df.to_dict(orient='records'))
  
main()
