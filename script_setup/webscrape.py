from bs4 import BeautifulSoup as bs
import logging
import requests
from selenium import webdriver
from typing import List, Tuple
import time
import re
from pymongo import MongoClient
import configparser

# Configure logging
file_name = "logs/webscrapping.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    handler = logging.FileHandler(file_name)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# set the file to empty at start
with open(file_name, 'w'):
    pass

# test the URL connection
def testConnection():
    try:
        logger.info('Testing URL connection for Web Scrapping')
        url_test = "https://www.cfainstitute.org/en/membership/professional-development/refresher-readings#first={first}&sort=%40refreadingcurriculumyear%20descending&numberOfResults=100".format(first=0)
        print("URL: ",url_test)
        response_test = requests.get(url_test)
        print(response_test)
        logger.info('Successfully connected to the site')
        return True
    except:
        logger.error("Error in loading the URL")
        return False

# function to return individual page Links
def getPageURLs() -> list:
    # base URL
    base_url = "https://www.cfainstitute.org/en/membership/professional-development/refresher-readings"
    
    # additional section for the URL to specify the first row and the number of results
    additional_section = "#first={first}&sort=%40refreadingcurriculumyear%20descending&numberOfResults=100"
    
    individual_topics_urls = [] # store individual page URLs (sub pages to collect data)
    first_entry = 0 # start of the entries on web page
    has_more_pages = True # flag to keep scrapping
    count = 0

    # Set up Selenium WebDriver to scrape dynamic data
    driver = webdriver.Chrome()

    logger.info("------- Starting URL Extraction -------")
    while has_more_pages:
        try:
            # increment counter
            count += 1
        
            # Construct URL to Scrape Data
            url = base_url + additional_section.format(first=first_entry)  
        
            # load the Web Page that has Dyanmic content with help of selenium 
            driver.get(url)

            # Sleep for 5 seconds for page to load
            time.sleep(2)
            
            # get the source HTML code
            page = driver.page_source
        
            # use BeautifulSoup to load pase the content
            parsed_content = bs(page, 'html.parser')
        
            # find all the individual 224 entries and get the URL
            for parse in parsed_content.find_all("div", {"class": "coveo-list-layout"}):
                a_tag = parse.find('a') # get the <a> tags
                individual_topics_urls.append((a_tag.text, a_tag.get('href')))
    
            # Find the Next page button on the page
            next_button = parsed_content.find_all("li", {"class": "coveo-pager-next"})
    
            # if next button is present go to next page 
            if next_button:
                first_entry += 100
            else:
                has_more_pages = False
            

            
            logger.info("Total topics found after iteration {count} iterations are {length}".format(count=count, length=len(individual_topics_urls)))
            
        except Exception as e:
            logger.error("Exception:", e)
            break
            
    # Close the Selenium WebDriver
    driver.quit()
    
    logger.info("------- Ending URL Extraction -------")
    return individual_topics_urls   

# function to scrape Data from CFA site (Raw Data)
def extractData(individual_topics_urls: List[Tuple[str, str]]):
    
    logger.info("------- Starting Data Extraction -------")
    
    # Set up Selenium WebDriver to scrape dynamic data
    driver = webdriver.Chrome()
    
    # initialize data frame
    data_raw = []

    # iteration counter
    counter = 0
    
    for topic, topic_url in individual_topics_urls:
        try:
            # load the Web Page that has Dyanmic content with help of selenium 
            driver.get(topic_url)
            
            # reset temp
            temp = {}

            # increment counter
            counter += 1
            
            # get the source HTML code
            page = driver.page_source

            # use BeautifulSoup to load pase the content
            parsed_content = bs(page, 'html.parser')

            # extract title
            try:
                title = parsed_content.find("h1", {"class": "article-title"}).text.strip().strip("\u200b")
                title = re.sub(r'\s+', ' ', title)
                title = title.strip()
            except:
                logger.error("Iteration: {} topic: {} -> title not found".format(counter, topic))
                title = None

            # extract Year and Level
            try:
                content_utility = parsed_content.find("div", {"class": "content-utility"})
                try: 
                    year = content_utility.find('span', {"class": "content-utility-curriculum"}).text.strip().split('\n')[0]
                    year = re.sub(r'\s+', ' ', year)
                    year = year.strip()
                except:
                    logger.error("Iteration: {} topic: {} -> content-utility-curriculum (Year) not found".format(counter, topic))
                    year = None
                try:
                    level = content_utility.find("span", {"class": "content-utility-topic"}).text.strip().strip("Level ")
                    level = re.sub(r'\s+', ' ', level)
                    level = level.strip()
                except:
                    logger.error("Iteration: {} topic: {} -> content-utility-topic (level) not found".format(counter, topic))
                    level = None
            except:
                logger.error("Iteration: {} topic: {} -> content_utility (Year and Level) not found".format(counter, topic))
                year = None
                level = None

            # extract Introduction Summary and Learning Outcomes
            try:
                h2_elements = parsed_content.find_all("h2", {"class": "article-section"})
                introduction_h2, learning_outcomes_h2, summary_h2  = None, None, None
                for h2_element in h2_elements:
                    if h2_element.text == "Introduction" or h2_element.text == "Overview":
                        introduction_h2 = h2_element
                    elif h2_element.text == "Learning Outcomes":
                        learning_outcomes_h2 = h2_element
                    elif h2_element.text == "Summary":
                        summary_h2 = h2_element
            
                # extract Introduction
                try:
                    introductions = introduction_h2.parent.find_all("p")
                    intro = ""
                    for introduction in introductions:
                        intro += introduction.text.strip()
                    intro.strip()

                    points = introduction_h2.parent.find_all("li")
                    intro_points = ''
                    for point in points:
                        intro_points += point.text
                    intro = intro + " " + intro_points
                    intro = re.sub(r'\s+', ' ', intro)
                    intro = intro.strip()
                except:
                    logger.error("Iteration: {} topic: {} -> Introduction not found".format(counter, topic))
                    intro = None
            
                # extract Learning Outcomes
                try:    
                    learning_outcomes_paras = learning_outcomes_h2.find_next_sibling().find_all("p") 
                    learning_outcomes = learning_outcomes_h2.find_next_sibling().find_all("li") 
                    learnings = []
                    learnings_f = []

                    for learning_outcomes_para in learning_outcomes_paras:
                        if learning_outcomes_para.text:
                            learnings.append(learning_outcomes_para.text.strip())

                    for learning_outcome in learning_outcomes:
                        if learning_outcome.text:
                            learnings.append(learning_outcome.text.strip())

                    for l in learnings[2:]:
                        learning_list = [s.strip() for s in l.split("\n")]
                        temp_l = ' '.join(learning_list)
                        temp_l = re.sub(r'\s+', ' ', temp_l)
                        learnings_f.append(temp_l)

                except:
                    logger.error("Iteration: {} topic: {} -> Learning Outcomes not found".format(counter, topic))
                    learnings_f = []

                # extract Summary
                try:
                    summaries = summary_h2.find_next_sibling().find_all("p")
                    summary = ""
                    for summ in summaries:
                        summary += summ.text.strip()
                    summary.strip()
                    
                    points = summary_h2.find_next_sibling().find_all("li")
                    summary_points = ''
                    for point in points:
                        summary_points += point.text
            
                    
                    summary_temp = summary + " " + summary_points
            
                    summary_list = [s.strip() for s in summary_temp.split("\n")]
                    summary = ' '.join(summary_list)
                    summary = re.sub(r'\s+', ' ', summary)
                    summary = summary.strip()
                except:
                    logger.error("Iteration: {} topic: {} -> Summary not found".format(counter, topic))
                    summary = None
            except:
                logger.error("Iteration: {} topic: {} -> Introduction, Summary and Learning Outcomes not found".format(counter, topic))
                learnings = None
                intro = None
                summary = None

            # extrat PDF File Link
            try:
                lock_content = parsed_content.find("section", {"class": "primary-asset login-required"})
                pdf_link = [a for a in lock_content.find_all('a', {"class": "locked-content"}) if a.text.strip()=='Download the full reading (PDF)'][0].get("href")  
                pdf_link = 'https://www.cfainstitute.org' + pdf_link
            except:
                logger.error("Iteration: {} topic: {} -> PDF Link not found".format(counter, topic))
                pdf_link = None

            temp['NameOfTheTopic'] = title
            temp['Year'] = year
            temp['Level'] = level
            temp['IntroductionSummary'] = intro
            temp['LearningOutcomes'] = learnings_f
            temp['PDFFileLink'] = pdf_link
            temp['Summary'] = summary
            temp['SummaryPageLink'] = topic_url
            temp['Status'] = False

            data_raw.append(temp)

        except Exception as e:
            print(e)
            logger.error("Iteration: {} topic: {} -> Unknown Error".format(counter, topic))

    # Close the Selenium WebDriver
    driver.quit()
    
    logger.info("------- Ending Data Extraction -------")
    
    return data_raw

def loadData(data: List):
    # get config data
    config = configparser.ConfigParser()
    config.read('configuration.properties')
    mongo_url = config['MongoDB']['mongo_url']
    db_name = config['MongoDB']['db_name']
    collection_name = config['MongoDB']['collection_name']
    
    # create client
    client = MongoClient(mongo_url)
    db = client[db_name]
    
    if collection_name in db.list_collection_names():
        db[collection_name].drop()
        
    collection = db[collection_name]
    
    for i in data:
        id = collection.insert_one(i).inserted_id
    client.close()
    return

if __name__ == "__main__":
    # test connectuion
    testConnection()

    # get topic URL list
    individual_topics_urls = getPageURLs()

    # extract data in form of DF
    data = extractData(individual_topics_urls)

    # load data to MongoDB
    loadData(data)