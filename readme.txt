Steps to setup:
------------------------------------------------

prerequisite:
1. Python 3 (i am using 3.8.0)
2. up and running elastic search (on local or cloud) 

Steps:
1. pip install -r requirements.txt  (Optional Recommendation: install requirements in virtualenv)
2. update config.ini file based on your elasticsearch's configuration.
   -> in host provide cloud or local elasticserach url
   -> update user, password and port
   -> update protocol
SETUP IS ALL DONE !!!

Run Project:
1. Now in cmd run
  -> python main.py
2. In inputbox for the first time provide "y" because we need to create urls for job and store it in file
   -> While running the script again its upto you to recreate the files for job_links or utilize the previous links and scrape data again


Document:
main.py: entry point for the script
config.py: to read elasticsearch configuration data 
elasticsearch_helper.py: helper functions to create connection and store data in elastic search
config.ini: provide configuration data for elastic serach (up to client)
resources: Here all the data is getting stored
    -> job_data_links.txt = All the links which we are getting from website
    -> error_job_urls.txt = Urls which we are not able to scrape (Pages dont have any data)
    -> other_urls.txt     = Urls for category Pages
    -> job_data.json      = All the Data for jobposting same is stored in elasticsearch


Your data will be scraped in dumped in elastic search in maximum 15 to 20 minutes.
for 1000 data it just took approx 4 min. 
If you need any improvemment please let us know.
THANK YOU !!!


