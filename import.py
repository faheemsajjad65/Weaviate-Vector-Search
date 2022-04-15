"""
Imports the complete Wiki dataset into Weaviate
"""

import json
import weaviate
from uuid import uuid3, NAMESPACE_DNS
from loguru import logger
import time
from pprint import pprint


def create_weaviate_schema(client):
    """
    ...
    """

    # flush the schema and data
    client.schema.delete_all()
    # create schema
    schema = {
        "classes": [
            {
                "class": "Study",
                "description": "A study with a title",
                "vectorizer": "none",
                "vectorIndexConfig": {
                    "skip": True
                },
                "properties": [
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Id of the study",
                        "name": "studyId",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Name of the study",
                        "name": "studyName",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Description of study",
                        "name": "studyDescription",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string[]"
                        ],
                        "description": "Tags of study",
                        "name": "tags",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string[]"
                        ],
                        "description": "Interested Areas of study",
                        "name": "interestAreas",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "Transcript"
                        ],
                        "description": "Transcripts of study",
                        "name": "hasTranscripts",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "Pal"
                        ],
                        "description": "Pals of study",
                        "name": "hasPals",
                        "indexInverted": True
                    }
                ]
            },
            {
                "class": "Pal",
                "description": "Pals",
                "vectorizer": "none",
                "vectorIndexConfig": {
                    "skip": True
                },
                "properties": [
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Id of the pal",
                        "name": "palId",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Name of the pal",
                        "name": "palName",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Email of the pal",
                        "name": "palEmail",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Phone number of the pal",
                        "name": "palNumber",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Age of the pal",
                        "name": "palAge",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Gender of the pal",
                        "name": "palGender",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "DOB of the pal",
                        "name": "palDOB",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Hourly Pricing of the pal",
                        "name": "hourlyPricing",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "Study"
                        ],
                        "description": "Hourly Pricing of the pal",
                        "name": "inStudy",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "Transcript"
                        ],
                        "description": "Transcript",
                        "name": "inTranscript",
                        "indexInverted": True
                    }
                ]
            },
            {
                "class": "Transcript",
                "description": "A Transcript with a title",
                "vectorizer": "none",
                "vectorIndexConfig": {
                    "skip": True
                },
                "properties": [
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Id of the Transcript",
                        "name": "transcriptId",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "transcript_date of the Transcript",
                        "name": "transcriptDate",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "Study"
                        ],
                        "description": "Linked with Study",
                        "name": "inStudy",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "Nugget"
                        ],
                        "description": "List of Nuggets this Transcript has",
                        "name": "hasNuggets",
                        "indexInverted": True
                    },
                    {
                        "dataType": [
                            "Pal"
                        ],
                        "description": "List of Nuggets this Transcript has",
                        "name": "hasPals",
                        "indexInverted": True
                    }
                ]
            },
            {
                "class": "Nugget",
                "description": "A Nugget",
                "vectorizer": "text2vec-contextionary",
                "moduleConfig": {
                    "text2vec-contextionary": {  
                        "vectorizeClassName": True
                    }
                },
                "properties": [
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "Id of the Nugget",
                        "name": "nuggetId",
                        "indexInverted": False,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "string"
                        ],
                        "description": "question of the nugget",
                        "name": "question",
                        "indexInverted": False,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "text"
                        ],
                        "description": "The answer of the nugger",
                        "name": "answer",
                        "indexInverted": False,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": False,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "string[]"
                        ],
                        "description": "codes of the nugget",
                        "name": "codes",
                        "indexInverted": False,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "int"
                        ],
                        "description": "Order of the nugget",
                        "name": "order",
                        "indexInverted": True,
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": False,
                            }
                        }
                    },
                    {
                        "dataType": [
                            "Transcript"
                        ],
                        "description": "Transcript this nugget is in",
                        "name": "inTranscript",
                        "moduleConfig": {
                            "text2vec-contextionary": {
                                "skip": True,
                                "vectorizePropertyName": True,
                            }
                        }
                    }
                ]
            }
        ]
    }
    #
    # add schema
    #
    client.schema.create(schema)

def add_study_to_batch(studyDetails):
    return [
        {
            "studyId"           : studyDetails["studyId"],
            "studyName"         : studyDetails["studyName"],
            "studyDescription"  : studyDetails["studyDescription"],
            "tags"              : studyDetails["tags"],
            "interestAreas"     : studyDetails["interestAreas"]
        },
        str(uuid3(NAMESPACE_DNS, studyDetails["studyName"].replace(" ", "_")))
    ]

def add_nuggets_to_batch(transcript_uuid,transcript_meta, parsed_line):
    return_array = []
    counter = 1
    
    for nugget in transcript_meta['nuggets']:
        
        nuggetId = str(transcript_meta["transcriptId"]) + "_" + str(counter)
        counter = counter + 1

        nuggets_uuid = str(uuid3(NAMESPACE_DNS, parsed_line["studyName"].replace(" ", "_") + str(transcript_meta["transcriptId"]) + str(nuggetId)))

        add_object = {    
            "nuggetId"      : nuggetId,
            "question"      : nugget["question"],
            "answer"        : nugget["answer"],
            "order"         : nugget["order"],
            "codes"         : nugget["codes"],
            "inTranscript": [{
                "beacon": "weaviate://localhost/" + transcript_uuid
            }]
        }
       
        # add to batch
        return_array.append([
            add_object,
            nuggets_uuid
        ])

    return return_array

def add_Transcripts_to_batch(study_uuid,transcript,parsed_line):
    return [
        {    
            "transcriptId"  : transcript["transcriptId"],
            "transcriptDate": transcript["transcriptDate"],
            "inStudy": [{
                "beacon": "weaviate://localhost/" + study_uuid
            }]
        },
        str(uuid3(NAMESPACE_DNS, parsed_line["studyName"].replace(" ", "_") + str(transcript["transcriptId"])))
    ]

def add_pals_to_batch(study_uuid,pal,parsed_line):
    return [
        {    
            "palId"         : pal["palId"],
            "palName"       : pal["palName"],
            "palEmail"      : pal["palEmail"],
            "palNumber"     : pal["palNumber"],
            "palAge"        : pal["palAge"],
            "palGender"     : pal["palGender"],
            "palDOB"        : pal["palDOB"],
            "hourlyPricing" : pal["hourlyPricing"],
            "inStudy": [{
                "beacon": "weaviate://localhost/" + study_uuid
            }]
        },
        str(uuid3(NAMESPACE_DNS, parsed_line["studyName"].replace(" ", "_") + str(pal["palId"])))
    ]

def handle_results(results):
    if results is not None:
        for result in results:
            if 'result' in result and 'errors' in result['result'] and  'error' in result['result']['errors']:
                for message in result['result']['errors']['error']:
                    logger.debug(message['message'])

def import_study_transcripts_data(transcripts_meta):
    with open(transcripts_meta) as f:
        for line in f:
            parsed_line = json.loads(line)
            try:
                if len(parsed_line) > 0:
                    for study in parsed_line:
                        studyObject = add_study_to_batch(study)
                        client.data_object.create(studyObject[0], "Study", studyObject[1])

                        if len(study["studyPals"]) > 0:
                            # add the study Pals
                            for pal in study["studyPals"]:
                                palsObject = add_pals_to_batch(studyObject[1],pal,study)
                                client.data_object.create(palsObject[0], "Pal", palsObject[1])
                                client.batch.add_reference(studyObject[1], "Study", "hasPals", palsObject[1])

                        if len(study["studyTranscripts"]) > 0:
                            # add the study Transcripts
                            for transcript in study["studyTranscripts"]:
                                transcriptObject = add_Transcripts_to_batch(studyObject[1],transcript,study)
                                client.data_object.create(transcriptObject[0], "Transcript", transcriptObject[1])
                                client.batch.add_reference(studyObject[1], "Study", "hasTranscripts", transcriptObject[1])
                                # add the study Transcript Nuggets
                                if len(transcript["nuggets"]) > 0:
                                    for nuggetsObj in add_nuggets_to_batch(transcriptObject[1],transcript,study):
                                        client.batch.add_data_object(nuggetsObj[0], "Nugget" ,nuggetsObj[1])
                                        client.batch.add_reference(transcriptObject[1], "Transcript", "hasNuggets", nuggetsObj[1])

                                # add the study Transcript Pals
                                if len(transcript["pals"]) > 0:
                                    for pal in transcript["pals"]:
                                        pal_uuid = str(uuid3(NAMESPACE_DNS, study["studyName"].replace(" ", "_") + str(pal["palId"])))
                                        palObjectToBeUpdated = client.data_object.get_by_id(pal_uuid)
                                        if(len(palObjectToBeUpdated) > 0):
                                            client.data_object.reference.add(pal_uuid, "inTranscript", transcriptObject[1])
                                            client.data_object.reference.add(transcriptObject[1], "hasPals", pal_uuid)
                    
                    client.batch.create_objects()    
                    client.batch.create_references()

            except Exception as e:
                logger.debug(e)
                pass

if __name__ == "__main__":
    
    logger.info("Start import")
    
    transcripts_data_file = "transcripts-meta.json"
    
    # connect Weaviate
    client = weaviate.Client("http://localhost:8080")
    
    # create schema
    create_weaviate_schema(client)
    
    # import data objects
    import_study_transcripts_data(transcripts_data_file)

    # done
    logger.info("Done")
