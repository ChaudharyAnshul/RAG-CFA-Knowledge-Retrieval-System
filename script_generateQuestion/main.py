
from google.cloud import storage
import os
import openai
import pymongo
import configparser


def initialize():
    config = configparser.ConfigParser()
    config.read('configuration.properties')
    openai.api_key = config['openai']['OPENAI_API_KEY'] 

    mongo_client = pymongo.MongoClient(config['mongodb']['MONGODB_CONNECTION_STRING'])
    db = mongo_client[config['mongodb']['DATABASE_NAME']]
    collection = db[config['mongodb']['COLLECTION_NAME']]

    PATH = os.path.join(os.getcwd() , 'Bucket_key_file_path')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
    storage_client = storage.Client()

    return config, db, collection, storage_client

def gcp_store_from_string(storage_client,string_data, file_name):
    bucket = storage_client.get_bucket('bucket_name')
    blob = bucket.blob(file_name)
    blob.upload_from_string(string_data)

def gcp_read_string(storage_client,file_name):
    bucket = storage_client.get_bucket('bucket_name')
    blob = bucket.blob(file_name)
    return str(blob.download_as_string())


def process_text_files_and_generate_analysis(combined_text_file, storage_client):
    combined_text_data = gcp_read_string(storage_client, combined_text_file)
    prompt = f"Here are few sample questions:\n{combined_text_data[:16385]}\n\n. Perform detailed analysis on the format, question creation and how the Answers and explanation are given. Also mention few generalized examples in them."
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0125",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )
    combined_analysis = response['choices'][0]['message']['content']
    gcp_store_from_string(storage_client, combined_analysis, "analysis.txt")  
    return combined_analysis



def generate_questions_from_mongo(mongo_summary, combined_analysis, collection_name, storage_client, num_questions=50, max_single_prompt=5):
    if num_questions < max_single_prompt:
        max_single_prompt = num_questions       
    print("Total remaining Questions: ",num_questions,", Questions to be Generated in current run: ",max_single_prompt)
    prompt = f"""Following is the analysis of historical questions: 
    {combined_analysis}
    
    Refer the formatting of this analysis and strictly generate {max_single_prompt} new questions and provide their solutions on :
    {mongo_summary}
    
    
    Ensure that each question had for options and solutions provide thorough explanation for why a particular choice is correct and list explanation why each other answer choices are incorrect. 
    Each generated question should be numbered and have four following sections,--> Question Number:, --> Question: , --> Option:, --> Explanation:
    Add *--------------* between each generated questions with all sections of one question together.
    """
    messages = [{"role": "user", "content": prompt}]
    
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0125",
        messages=messages,
        temperature=0.1
    )
    
    first = response['choices'][0]['message']['content']
    final = first
    
    pushed = store_generated_questions_in_mongo(first, collection_name)
    num_questions -= pushed  
    
    while num_questions > 0:
        if num_questions < max_single_prompt:
            max_single_prompt = num_questions
            
        print("Total remaining Questions: ",num_questions,", Questions to be Generated in current run: ",max_single_prompt)
        
        messages.append({"role": "assistant", "content": first})
        messages.append({"role": "user", "content": f"Generate next {max_single_prompt} more questions"})
    
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo-0125",
            messages=messages,
            temperature=0.1
        )
        first = response['choices'][0]['message']['content']
        final += response['choices'][0]['message']['content']     
        
        pushed = store_generated_questions_in_mongo(first, collection_name)
        if pushed==0:
            print(first)
        num_questions -= pushed

    return final


def store_generated_questions_in_mongo(generated_questions, generated_questions_collection):
    total = 0
    for question_with_solution in generated_questions.split("*--------------*"):

        if "explanation" in question_with_solution.lower():
            solution_set = question_with_solution.split("Explanation:")
            
            question = solution_set[0]
            solution = "".join(solution_set[1:])
            
            question = question.replace("Question Number: ","").replace("Question: ","").replace("Option:","Options:")
                    
            generated_questions_collection.insert_one({
                "question": question.strip().replace("-->",""),
                "answer": solution.strip().replace("-->","")

            })
            total +=1
        else:
            pass
    print("Split Genearted Questions: ", len(generated_questions.split("*--------------*")),", Pushed to Mongo: ", total)
    
    return total
            
        

def main():
    config, db, collection, storage_client = initialize()
    output_file = 'analysis.txt' 
    combined_text_file = "combined_text_data.txt"
    combined_analysis = process_text_files_and_generate_analysis(combined_text_file, storage_client)

    summaries = collection.find(
        {"NameOfTheTopic": {"$in": ["Introduction to Linear Regression", "Sampling and Estimation", "Hypothesis Testing"]}},
        {"Summary": 1, "_id": 0}
    )
    
    used_topics = []
    for summary in summaries:
        used_topics.append(summary["Summary"])
    
    mongo_summary = "\n\n".join([summary["Summary"] if summary["Summary"] is not None else "" for summary in summaries])

    SetACollection = db[config['mongodb']['SET_A_COLLECTION_NAME']]
    SetBCollection = db[config['mongodb']['SET_B_COLLECTION_NAME']]

    if config['mongodb']['SET_A_COLLECTION_NAME'] not in db.list_collection_names():
        generate_questions_from_mongo(mongo_summary, combined_analysis, SetACollection, storage_client, num_questions=50, max_single_prompt=50)
    else:
        print("SetA already exists")

    if config['mongodb']['SET_B_COLLECTION_NAME'] not in db.list_collection_names():
        generate_questions_from_mongo(mongo_summary, combined_analysis, SetBCollection, storage_client, num_questions=50, max_single_prompt=50)
    else:
        print("SetB already exists")
        
    for summary in summaries:
        if summary["Summary"] in used_topics:
            collection.update_one(
                {"Summary": summary["Summary"]},
                {"$set": {"status": True}}
            )

if __name__ == "__main__":
    main()