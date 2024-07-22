import functions_framework
import openai
from pymongo import MongoClient
from bson.objectid import ObjectId
from pinecone import Pinecone
import os
from dotenv import load_dotenv
import pymongo
import certifi
import csv
import re
import pandas as pd

load_dotenv()

def process_documents(setB, pinecone_setA, pinecone_question_namespace, collection_set_A):
    correct = 0

    for document in setB:
        data_to_insert = []
        question = document['question']
        answer = (((document['answer']).split())[0])[0]

        embedded_question = openai.Embedding.create(
            input=question, 
            model=os.getenv('embedding_model'),
        ).data[0].embedding

        found = pinecone_setA.query(vector=embedded_question, top_k=3, include_metadata=True, namespace="questions")

        context = ''
        for each in found['matches']:
            mongoId = ObjectId(each['id'])
            mongo_result = collection_set_A.find_one({"_id": mongoId})
            context += f"""
            Question: 
            """
            los = mongo_result.get("question")
            context += los
            context += f"""
            Answer: 
            """
            los = mongo_result.get("answer")
            context += los
            context += f"""

            """
        
        query = f'''Based on this context 
        {context}

        Answer the following question by only giving the correct option id (like A or B or C or D) along with a very short explanation on why that is correct and not any other text 
        {question}

        The response format must strictly be in this format
        Option:
        Explanation:
        '''


        messages = [{"role": "user", "content": query}]

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=messages,
            temperature=0.1
        )
        gpt_response = response['choices'][0]['message']['content']
        print(gpt_response)

        pattern = r"Option:\s*([A-Za-z0-9]+)\s*Explanation:\s*(.+)"
        match = re.search(pattern, gpt_response, re.DOTALL)

        if match:
            option_id = match.group(1).strip()
            explanation = match.group(2).strip()

        question = question[1:]
        question = question.replace('\n', ' ')

        if answer == option_id:
            Match = 1
            correct+=1
        else: 
            Match = 0

        data_to_insert.append({
            'Question': question,
            'GPT Explanation': explanation,
            'GPT Answer': option_id,
            'KB Answer': answer,
            'Match': Match
        })

    collection_part_3_report = collection_set_A.database.get_collection(os.getenv('collection_part_3_report'))
    collection_part_3_report.insert_many(data_to_insert)


    return correct

def main():

    key = os.getenv('GPT_key') 
    mongo_url = os.getenv('mongo_url') 
    db_name = os.getenv('db_name') 

    collection_set_A_name = os.getenv('collection_set_A') 
    collection_set_B_name = os.getenv('collection_set_B') 
    key_pinecone_setA = os.getenv('key_pinecone_setA') 
    index_name_setA = os.getenv('index_name_setA')
    pinecone_question_namespace = os.getenv('pinecone_question_namespace')

    openai.api_key = key
    client = pymongo.MongoClient(mongo_url,tlsCAFile=certifi.where())
    db = client[db_name]

    collection_set_A = db[collection_set_A_name]
    collection_set_B = db[collection_set_B_name]

    pinecone = Pinecone(api_key=key_pinecone_setA)
    pinecone_setA = pinecone.Index(name=index_name_setA)

    all_documents_B = collection_set_B.find()

    correct = process_documents(all_documents_B, pinecone_setA, pinecone_question_namespace, collection_set_A)
    print("The number of questions that had the same answers: ",correct)

main()
