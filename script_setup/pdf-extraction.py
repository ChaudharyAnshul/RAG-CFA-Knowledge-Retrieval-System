
from PyPDF2 import PdfReader
import os
import io
from google.cloud import storage
import google.cloud.storage
import json
import os
import sys
import openai
import pymongo
import configparser


PATH = os.path.join(os.getcwd() , 'Bucket_key_file_path')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
storage_client = storage.Client()

def gcp_store_from_string(string_data, file_name):
    bucket = storage_client.get_bucket('bucket_name')
    blob = bucket.blob(file_name)
    blob.upload_from_string(string_data)

def gcp_read_string(file_name):
    file_obj = io.BytesIO()
    bucket = storage_client.get_bucket('bucket_name')
    blob = bucket.blob(file_name)
    blob.download_to_file(file_obj)
    return file_obj


pdf_files = ['pdfFiles/sample-level-i-questions.pdf', 'pdfFiles/sample-level-i-questions.pdf', 'pdfFiles/sample-level-i-questions.pdf']
start_line = "Answers to Sample Level"
output_directory = "pdfToText/"

for idx, pdf_file in enumerate(pdf_files):
    pdf_reader = PdfReader(gcp_read_string(pdf_file))
    num_pages = len(pdf_reader.pages)
    all_text = ''

    for page_num in range(num_pages):
        page = pdf_reader.pages[page_num]
        all_text += page.extract_text()

    sections = all_text.split(start_line)

    text_file_name = f'sample-questions{idx + 1}'

    for i, section in enumerate(sections[1:]): 
        section = section.strip()  
        
        file_name = f'{output_directory}{text_file_name}.txt'
        print(file_name)
            
        gcp_store_from_string(section, file_name)


text_files = ["pdfToText/sample-questions1.txt", "pdfToText/sample-questions2.txt", "pdfToText/sample-questions3.txt"]
sample_questions = ''

def gcp_read_string2(storage_client,file_name):
    bucket = storage_client.get_bucket('bucket_name')
    blob = bucket.blob(file_name)
    return str(blob.download_as_string())

sample_questions=''
for file_path in text_files:
    sample_questions += gcp_read_string2(storage_client, file_path)

combined_text_data_file = "combined_text_data.txt"

gcp_store_from_string(sample_questions, combined_text_data_file)


