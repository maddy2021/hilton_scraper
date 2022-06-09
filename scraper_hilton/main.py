import json
import os
import time
import traceback
from typing import Set
from xml.etree.ElementTree import Element
import xml.etree.cElementTree as ET
import urllib.request

from requests import request
import requests
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import elasticsearch_helper as es_helper
import pathlib
import os

file_parent_directory = pathlib.Path(__file__).parent.resolve()
MAX_THREADS = 32
PARENT_URL = "https://jobs.hilton.com/us/en/sitemap_index.xml"
RESOURCE_DIR = os.path.join(file_parent_directory, "resources")
JOB_URLS_FILE = "job_data_links.txt"
ERROR_URL_FILE = "error_job_urls.txt"
OTHER_URL_FILE = "other_urls.txt"
JOB_TYPE = "JobPosting"
JOB_CONTEXT = "http://schema.org"
job_data = []
# namespace = root.tag.split("}")[0] +"}"
es = es_helper.connect_elasticsearch()

def make_url_list(parent_url: str, job_url_list: Set = set()):
    try:
        tree = ET.ElementTree(file=urllib.request.urlopen(parent_url))
        xml_root = tree.getroot()
        namespace = xml_root.tag.split("}")[0] + "}"
        for child in xml_root.findall(".//"+namespace+"loc"):
            if ".xml" in child.text.lower():
                make_url_list(parent_url=child.text, job_url_list=job_url_list)
            else:
                write_to_file(os.path.join(
                    RESOURCE_DIR, JOB_URLS_FILE), child.text)
                job_url_list.add(child.text.replace(" ", ""))
    except Exception as e:
        print(e)
    return list(job_url_list)


def write_to_file(file_path, data_line):
    with open(file_path, "a+", encoding="utf-8") as fp:
        fp.writelines(data_line+"\n")


def read_file(file_path):
    data = []
    with open(file_path, "r+", encoding="utf-8") as fp:
        data = fp.read().split("\n")
    return [item for item in data if item]


def extract_dump_data(job_link):
    try:
        print(f"processing: {job_link}")
        html_data = requests.get(job_link)
        soup = BeautifulSoup(html_data.content, 'lxml')
        data = json.loads(soup.find('script', type='application/ld+json').text)
        # print("@type" in data and "@context" in data)
        if("@type" in data and 
            "@context" in data and 
            data["@type"].lower()==JOB_TYPE.lower() and 
            data['@context'].lower()==JOB_CONTEXT.lower()):
                # add logic to append in elastic_search
                if es is not None:
                    if es_helper.create_index(es, 'jobpost'):
                        out = es_helper.store_record(es, 'jobpost', data)
                        if(out):
                            print('Data indexed successfully')
                job_data.append(data)
                print(f"successfully scraped: {job_link}")
        else:
           write_to_file(os.path.join(
                    RESOURCE_DIR, OTHER_URL_FILE), job_link) 
    except Exception as e:
        write_to_file(os.path.join(
                    RESOURCE_DIR, ERROR_URL_FILE), job_link)
        print(f"Not able to scrape: {job_link}")
        print(e)

if __name__ == "__main__":
    try:
        job_data = []
        user_input = input("Do you want to re-create job links file ? Y or N : ")
        print(f"started scraping data from {PARENT_URL}")
        if(user_input.lower()=="y"):
            for f in os.listdir(RESOURCE_DIR):
                os.remove(os.path.join(RESOURCE_DIR, f))
            job_urls_list = make_url_list(parent_url=PARENT_URL)
        elif(user_input.lower()=="n"):
            for f in os.listdir(RESOURCE_DIR):
                print(f)
                if f==JOB_URLS_FILE:
                    continue
                os.remove(os.path.join(RESOURCE_DIR, f))
        else:
            raise Exception("Please provide valid input, rerun the script")
        job_links = read_file(os.path.join(RESOURCE_DIR,JOB_URLS_FILE))
        print(job_links)
        with ThreadPoolExecutor() as executor:
            executor.map(extract_dump_data, job_links)
        with open(os.path.join(RESOURCE_DIR,"job_data.json"), "w+") as fp:
            fp.write(json.dumps(job_data))
    except Exception as e:
        print(traceback.print_exc())
        print(e)
