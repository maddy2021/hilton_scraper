import json
from elasticsearch import Elasticsearch
import config as CONFIG

def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    print(res)

def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings":{
            "hilton_job":{
                "properties": {
                    "identifier": {
                    "type": "object",
                    "properties": {
                        "@type": {
                            "type": "text"
                        },
                        "name": {
                            "type": "text"
                        },
                        "value": {
                            "type": "text"
                        }
                    }
                    },
                    "hiringOrganization": {
                        "type": "object",
                        "properties": {
                            "@type": {
                                "type": "text"
                            },
                            "name": {
                                "type": "text"
                            },
                            "sameAs": {
                                "type": "text"
                            },
                            "logo": {
                                "type": "text"
                            },
                            "url": {
                                "type": "text"
                            }
                        }
                    },
                    "jobLocation": {
                        "type": "object",
                        "properties": {
                            "address": {
                                "type": "object",
                                "properties": {
                                    "addressCountry": {
                                        "type": "text"
                                    },
                                    "@type": {
                                        "type": "text"
                                    },
                                    "addressLocality": {
                                        "type": "text"
                                    }
                                }
                            },
                            "@type": {
                            "type": "text"
                            },
                            "geo": {
                                "type": "object",
                                "properties": {
                                    "@type": {
                                        "type": "text"
                                    },
                                    "latitude": {
                                        "type": "text"
                                    },
                                    "longitude": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "employmentType": {
                        "type": "nested"
                    },
                    "@type": {
                        "type": "text"
                    },
                    "workHours": {
                        "type": "text"
                    },
                    "description": {
                        "type": "text"
                    },
                    "title": {
                        "type": "text"
                    },
                    "datePosted": {
                        "type": "date"
                    },
                    "@context": {
                        "type": "text"
                    },
                    "occupationalCategory": {
                        "type": "text"
                    },
                    "skills": {
                        "type": "text"
                    }
                }
                }
            }
        }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, doc_type='hilton_job', body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def connect_elasticsearch():
    _es = None
    # _es = Elasticsearch("http://elastic:vMx6cR6UVYOod4uprl0Y@localhost:9200")
    _es = Elasticsearch(
        [CONFIG.HOST],
        http_auth=(CONFIG.USER, CONFIG.PASSWORD),
        scheme=CONFIG.SCHEME, port=CONFIG.PORT,)
    if _es.ping():
        print('Yay Connected')
    else:
        print('Awww it could not connect!')
    return _es


# if __name__ == '__main__':
#     data = [
#     {
#         "identifier": {
#             "@type": "PropertyValue",
#             "name": "Hilton",
#             "value": "HOT07YZJ"
#         },
#         "hiringOrganization": {
#             "@type": "Organization",
#             "name": "Hilton Hotels & Resorts, Hilton",
#             "sameAs": "https://jobs.hilton.com/us/en",
#             "logo": "https://assets.phenompeople.com/CareerConnectResources/pp/HILTGLOBAL/images/job_logo_config-1576666596684.jpg",
#             "url": "https://jobs.hilton.com/us/en/job/HOT07YZJ/Chef-de-Partie"
#         },
#         "jobLocation": {
#             "address": {
#                 "addressCountry": "Maldives",
#                 "@type": "PostalAddress",
#                 "addressLocality": "Maldives"
#             },
#             "@type": "Place",
#             "geo": {
#                 "@type": "GeoCoordinates",
#                 "latitude": "4.2850341",
#                 "longitude": "73.553584"
#             }
#         },
#         "employmentType": [
#             "FULL_TIME"
#         ],
#         "@type": "JobPosting",
#         "workHours": "40 hours per week",
#         "description": "&lt;p&gt;A Chef de Partie is responsible for supervising staff and ensuring high levels of food preparation to deliver an excellent Guest and Member experience while assisting with food cost controls. &lt;/p&gt;&lt;br&gt;&lt;br&gt;&lt;b&gt;What will I be doing?&lt;/b&gt;&lt;br&gt;&lt;br&gt;&lt;p&gt;A Chef de Partie, will supervise staff and ensure high levels of food preparation to deliver an excellent Guest and Member experience. A Chef de Partie will also be required to assist with food cost controls. Specifically, you will be responsible for performing the following tasks to the highest standards:&lt;/p&gt;&lt;ul&gt;     \n&lt;li&gt;Ensure all food preparation meets standards&lt;/li&gt;\n&lt;li&gt;Prepare and present high quality food&lt;/li&gt;     \n&lt;li&gt;Supervise staff&lt;/li&gt;  \n&lt;li&gt;Keep all working areas clean and tidy and ensure no cross contamination&lt;/li&gt;    \n&lt;li&gt;Prepare all mis-en-place for all relevant menus&lt;/li&gt;     \n&lt;li&gt;Assist in positive outcomes from guest queries in a timely and efficient manner&lt;/li&gt;   \n&lt;li&gt;Ensure food stuffs are of a good quality and stored correctly&lt;/li&gt;   \n&lt;li&gt;Contribute to controlling costs, improving gross profit margins, and other departmental and financial targets&lt;/li&gt;   \n&lt;li&gt;Assist other departments wherever necessary and maintain good working relationships&lt;/li&gt;     \n&lt;li&gt;Assist Head Chef/Sous Chef in the training of all staff in compliance of company procedures&lt;/li&gt;    \n&lt;li&gt;Report maintenance, hygiene and hazard issues&lt;/li&gt;    \n&lt;li&gt;Comply with hotel security, fire regulations and all health and safety and food safety legislation&lt;/li&gt;   \n&lt;li&gt;Be environmentally aware&lt;/li&gt;&lt;/ul&gt;&lt;b&gt;What are we looking for?&lt;/b&gt;&lt;br&gt;&lt;br&gt;&lt;p&gt;A Chef de Partie serving Hilton brands is always working on behalf of our Guests and working with other Team Members. To successfully fill this role, you should maintain the attitude, behaviours, skills, and values that follow:&lt;/p&gt;&lt;ul&gt;     \n&lt;li&gt;A minimum of 2 years of previous experience as a Chef de Partie or strong experience as a Demi Chef de Partie role&lt;/li&gt;   \n&lt;li&gt; A current, valid, and relevant trade commercial cookery qualification (proof may be required)&lt;/li&gt;     \n&lt;li&gt;Strong coaching skills&lt;/li&gt;     \n&lt;li&gt;Ability and desire to motivating Team&lt;/li&gt;     \n&lt;li&gt;Excellent communication skills&lt;/li&gt;    \n&lt;li&gt;NVQ Level 3&lt;/li&gt;    \n&lt;li&gt;Achieved Basic Food Hygiene Certificate&lt;/li&gt;   \n&lt;li&gt;Supervisory experience&lt;/li&gt;   \n&lt;li&gt;Positive attitude&lt;/li&gt;     \n&lt;li&gt;Ability to work under pressure&lt;/li&gt;      \n&lt;li&gt;Ability to work on own or in teams&lt;/li&gt;&lt;/ul&gt;\n\n&lt;p&gt;It would be advantageous in this position for you to demonstrate the following capabilities and distinctions:&lt;/p&gt;&lt;ul&gt;     \n&lt;li&gt;Previous kitchen experience in similar role&lt;/li&gt;    \n&lt;li&gt;Intermediate Food Hygiene&lt;/li&gt;    \n&lt;li&gt;Knowledge of current food trends&lt;/li&gt;&lt;/ul&gt;&lt;br&gt;&lt;br&gt;&lt;b&gt;What will it be like to work for Hilton?&lt;/b&gt;&lt;br&gt;&lt;br&gt;\n&lt;p&gt;Hilton is the leading global hospitality company, spanning the lodging sector from luxurious full-service hotels and resorts to extended-stay suites and mid-priced hotels. For nearly a century, Hilton has offered business and leisure travelers the finest in accommodations, service, amenities and value. Hilton is dedicated to continuing its tradition of providing exceptional guest experiences across its &lt;a href=\"http://jobs.hiltonworldwide.com/our-brands/index.php\" target=\"_blank\"&gt;global brands&lt;/a&gt;.&nbsp; Our vision &ldquo;to fill the earth with the light and warmth of hospitality&rdquo; unites us as a team to create remarkable hospitality experiences around the world every day.&nbsp; And, our amazing Team Members are at the heart of it all! &lt;/p&gt;\n&lt;br&gt;&lt;br&gt;&lt;br&gt;&lt;br&gt;",
#         "title": "Chef de Partie",
#         "datePosted": "2021-12-19T10:23:23.373+0000",
#         "@context": "http://schema.org",
#         "occupationalCategory": "Hotel",
#         "skills": "Chef De Partie, Chef De Projet, Head Resident Assistant, Chef Developer, Chef & Nimbula Director, Outlet Manager, Tour Operator, Sous Chef, Desinger & Development, Senior Tele Partner Executive"
#     },
#     {
#         "identifier": {
#             "@type": "PropertyValue",
#             "name": "Hilton",
#             "value": "HOT07ZZ1"
#         },
#         "hiringOrganization": {
#             "@type": "Organization",
#             "name": "Hilton Hotels & Resorts, Hilton",
#             "sameAs": "https://jobs.hilton.com/us/en",
#             "logo": "https://assets.phenompeople.com/CareerConnectResources/pp/HILTGLOBAL/images/job_logo_config-1576666596684.jpg",
#             "url": "https://jobs.hilton.com/us/en/job/HOT07ZZ1/Food-Beverage-Associate-Hilton-Singapore-Orchard"
#         },
#         "jobLocation": {
#             "address": {
#                 "addressCountry": "Singapore",
#                 "@type": "PostalAddress",
#                 "postalCode": "238867",
#                 "addressLocality": "Singapore"
#             },
#             "@type": "Place",
#             "geo": {
#                 "@type": "GeoCoordinates",
#                 "latitude": "1.2800945",
#                 "longitude": "103.8509491"
#             }
#         },
#         "employmentType": [
#             "FULL_TIME"
#         ],
#         "@type": "JobPosting",
#         "workHours": "40 hours per week",
#         "description": "&lt;p style=\"font-family: Arial;\"&gt;The Food &amp; Beverage Associate is concerned with the efficient and professional service of food and beverages, while ensuring guests receive optimum service in accordance with the standards, policies and procedures of the hotel and Hilton.&lt;br /&gt;\n&lt;b&gt;What will I be doing?&lt;/b&gt;&lt;/p&gt;\n\n&lt;p style=\"font-family: Arial;\"&gt;As the Food &amp; Beverage Associate, you will be responsible for performing the following tasks to the highest standards:&lt;/p&gt;\n\n&lt;ul&gt;\n\t&lt;li&gt;Maintain high customer service focus by approaching your job with the guests always in mind.&lt;/li&gt;\n\t&lt;li&gt;Have a positive impact, taking personal responsibility and initiative to resolve issues, always clearly communicating with both guests and colleagues.&lt;/li&gt;\n\t&lt;li&gt;Contribute ideas and suggestions to enhance operational/ environmental procedures in the hotel.&lt;/li&gt;\n\t&lt;li&gt;Actively promote the services and facilities of Hilton hotels to guests and suppliers of the hotel.&lt;/li&gt;\n\t&lt;li&gt;Perform all duties and responsibilities in a manner that ensures your safety and that of others in your workplace.&lt;/li&gt;\n\t&lt;li&gt;Confidently know the food and beverage menu contents and explain them in detail to guests.&lt;/li&gt;\n\t&lt;li&gt;Understand dietary requirements and offer appropriate suggestions.&lt;/li&gt;\n\t&lt;li&gt;Complete checklists on product knowledge.&lt;/li&gt;\n\t&lt;li&gt;Make suggestions on the menu that might suit guests of different nationalities.&lt;/li&gt;\n\t&lt;li&gt;Familiarize with menu items of all other outlets to recommend guests to other outlets.&lt;/li&gt;\n\t&lt;li&gt;Confidently know opening hours of all restaurants and hotel outlets.&lt;/li&gt;\n\t&lt;li&gt;Able to recommend other restaurants and city attractions to hotel guests.&lt;/li&gt;\n\t&lt;li&gt;Complete the checklist on preparing the restaurant for service.&lt;/li&gt;\n\t&lt;li&gt;Greet guests with a smile, offer assistance and introduce yourself.&lt;/li&gt;\n\t&lt;li&gt;Follow-up on any guest questions or queries immediately and if you don&rsquo;t have the answers, check with your Manager.&lt;/li&gt;\n\t&lt;li&gt;Ensure that all service procedures are carried out to the standards required.&lt;/li&gt;\n\t&lt;li&gt;Make sure all areas are cleaned and maintained in accordance with operating procedures.&lt;/li&gt;\n\t&lt;li&gt;Take personal responsibility for the service experience of all guests in your designated area.&lt;/li&gt;\n\t&lt;li&gt;Smile and greet all guests as they enter and exit the restaurant, even if they are not designated to your section.&lt;/li&gt;\n\t&lt;li&gt;Give guest service the highest priority.&lt;/li&gt;\n\t&lt;li&gt;Display professional behaviour at all times.&lt;/li&gt;\n\t&lt;li&gt;Avoid offensive or impolite language.&lt;/li&gt;\n\t&lt;li&gt;Report any accidents/ incidents to the Supervisor/ Assistant Manager/ Manager.&lt;/li&gt;\n\t&lt;li&gt;Carry out any other reasonable duties and responsibilities as assigned.&lt;/li&gt;\n\t&lt;li&gt;The Management reserves the right to make changes to this job description at its sole discretion and without advance notice.&lt;/li&gt;\n&lt;/ul&gt;&lt;p style=\"font-family: Arial;\"&gt;&lt;b&gt;What are we looking for?&lt;/b&gt;&lt;/p&gt;\n\n&lt;p style=\"font-family: Arial;\"&gt;A Food &amp; Beverage Associate serving Hilton Brands is always working on behalf of our Guests and working with other Team Members. To successfully fill this role, you should maintain the attitude, behaviours, skills, and values that follow:&lt;/p&gt;\n\n&lt;ul&gt;\n\t&lt;li&gt;Senior High School education or specialty in Hospitality.&lt;/li&gt;\n\t&lt;li&gt;Good command of English to meet business needs.&lt;/li&gt;\n\t&lt;li&gt;Motivated and committed, approaching all tasks with enthusiasm and seizing opportunities to learn new skills or knowledge in order to improve your personal performance.&lt;/li&gt;\n\t&lt;li&gt;Flexible and responds quickly and positively to changing requirements including the performance of any tasks requested of you.&lt;/li&gt;\n\t&lt;li&gt;Maintain high team focus by showing cooperation and support to colleagues in the pursuit of team goals.&lt;/li&gt;\n\t&lt;li&gt;Possess basic knowledge of food and beverage preparation and service of various alcoholic.&lt;/li&gt;\n\t&lt;li&gt;Able to remember, recite and promote the variety of menu items.&lt;/li&gt;\n\t&lt;li&gt;Open minded with an outgoing personality.&lt;/li&gt;\n\t&lt;li&gt;Willing to work for long hours and possess a positive attitude.&lt;/li&gt;\n&lt;/ul&gt;\n\n&lt;p style=\"font-family: Arial;\"&gt;&lt;b&gt;What will it be like to work for Hilton?&lt;/b&gt;&lt;/p&gt;\n\n&lt;p style=\"font-family: Arial;\"&gt;Hilton is the leading global hospitality company, spanning the lodging sector from luxurious full-service hotels and resorts to extended-stay suites and mid-priced hotels. For nearly a century, Hilton has offered business and leisure travelers the finest in accommodations, service, amenities and value. Hilton is dedicated to continuing its tradition of providing exceptional guest experiences across its &lt;a href=\"http://jobs.hiltonworldwide.com/our-brands/index.php\" target=\"_blank\"&gt;global brands&lt;/a&gt;.&nbsp; Our vision &ldquo;to fill the earth with the light and warmth of hospitality&rdquo; unites us as a team to create remarkable hospitality experiences around the world every day.&nbsp; And, our amazing Team Members are at the heart of it all!&lt;/p&gt;",
#         "title": "Food & Beverage Associate - Hilton Singapore Orchard",
#         "datePosted": "2022-01-05T02:19:30.766+0000",
#         "@context": "http://schema.org",
#         "occupationalCategory": "Hotel",
#         "skills": "Cocktail Server, Assistant Manager - Performance Management, Restaurant Server, Assistant Manager - Project Management Office, Private Events Coordinator & Corporate Catering Sales Manager, Food & Beverage Manager, Concessions Supervisor, Leasing Agent & Assistant Property Manager, National Casino Marketing Coordinator, Marketing Intern & Street Team Member"
#     },
#     ]
#     es = Elasticsearch("http://elastic:vMx6cR6UVYOod4uprl0Y@localhost:9200")

#     for item in data:
#         if es is not None:
#             if create_index(es, 'jobpost'):
#                 out = store_record(es, 'jobpost', item)
#                 if(out):
#                     print('Data indexed successfully')

#     # es = connect_elasticsearch()
#     if es is not None:
#         # search_object = {'query': {'match': {'calories': '102'}}}
#         search_object = {'_source': ['title'], 'query': {'match': {'title': 'Chef de Partie'}}}
#         # search_object = {'_source': ['title'], 'query': {'range': {'calories': {'gte': 20}}}}
#         search(es, 'jobpost', json.dumps(search_object))