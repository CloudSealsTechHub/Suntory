# import libraries
import json
from flask import Flask, jsonify,request
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import uuid
import re

# Get today's date
today_date = datetime.today().date()

# 1. Validate Email Address
def is_valid_email(email):
    email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    if bool(re.match(email_pattern, email)):
        return (f"{email} is a valid email address.")
    else:
        return (f"{email} is not a valid email address.")
    
# 2. Extract Valid Phone Numbers
def extract_valid_phone_numbers(phone):
    phone_pattern = re.compile(r'^\d{10}$')
    if bool(re.match(phone_pattern, phone)):
        return (f"{phone} is a valid PhoneNumber")
    else:
        return (f"{phone} is not a valid PhoneNumber")

# 3. Extract content based on query
def recursive_search(document, desired_value):
    
    for key, value in document.items():
        if isinstance(value, dict):
            # Recursively search nested dictionaries
            for key1,value1 in value.items():
                if value1==desired_value:
                    return True                
        elif value == desired_value:
            return True
    return False

# 4. Extract key based on value from dictionary
def get_key_by_value(dictionary, value):
    key1=[]
    for key, val in dictionary.items():
        if val == value:
            key1.append(key)
    # If the value is not found, you can return a default value or raise an exception
    return key1

def check_existence(data):
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client['suntory_db']
        collection = db['users_data']

        # Check if each identifier exists in the collection
        results = {}
        query ={}
        missed_data=[]
        ## loop for each input entry
        for key in data:
            value1 = data[key]
            matching_documents = [document for document in collection.find() if recursive_search(document, value1)==True]
            if len(matching_documents) == 0:
                results[f"{key}_exist"] = "No"
                results[f"{key}_suntory_id"] = ""
                missed_data.append(key)
            elif len(matching_documents) == 1:
                results[f"{key}_exist"] = "Yes"
                results[f"{key}_suntory_id"] = str(matching_documents[0].get("suntory_id", ""))
                result_key = get_key_by_value(matching_documents[0][key], data.get(key))
                query[key+"."+result_key[0]]=data.get(key)
            else:
                return {"Error":"Input entires matches with more than one suntory IDs"}
        print(missed_data)    
        # if any input entry matches with exsted database need to addmissed data to existing sutory ID
        if len(missed_data)<3:    
            user_to_update = collection.find_one(query)

            missed_data1=[]
            for i in missed_data:
                if i=="Email":
                    if "is a valid email address" in is_valid_email(data['Email']):
                        number = int(len(user_to_update['Email'])/3)+1
                        missed_data1.append({"$set": {i+'.'+i+'_'+str(number): data.get(i)}})
                        missed_data1.append({"$set": {i+'.'+i+"Interaction"+str(number): 1}})
                        missed_data1.append({"$set": {i+'.'+i+"LastInteractionDate"+str(number): today_date.strftime("%Y-%m-%d")}})
                    else:
                        continue
                elif i=="PhoneNumber":
                    if "is a valid PhoneNumber" in extract_valid_phone_numbers(data['PhoneNumber']):
                        number = int(len(user_to_update['PhoneNumber'])/3)+1
                        missed_data1.append({"$set": {i+'.'+i+'_'+str(number): data.get(i)}})
                        missed_data1.append({"$set": {i+'.'+i+"Interaction"+str(number): 1}})
                        missed_data1.append({"$set": {i+'.'+i+"LastInteractionDate"+str(number): today_date.strftime("%Y-%m-%d")}})
                    else:
                        continue
                else:
                    number = int(len(user_to_update['ExternalID'])/3)+1
                    missed_data1.append({"$set": {i+'.'+i+'_'+str(number): data.get(i)}})
                    missed_data1.append({"$set": {i+'.'+i+"Interaction"+str(number): 1}})
                    missed_data1.append({"$set": {i+'.'+i+"LastInteractionDate"+str(number): today_date.strftime("%Y-%m-%d")}})
            for i in query:
                if "Email" in i:
                    if "is a valid email address" in is_valid_email(data['Email']):
                        missed_data1.append({"$set": {i[:-2]+"Interaction"+str(i[-1]): user_to_update[i.split('.')[0]][[i.split('.')[1]][0][:-2]+"Interaction"+i[-1]]+1}})
                        missed_data1.append({"$set": {i[:-2]+"LastInteractionDate"+str(i[-1]): today_date.strftime("%Y-%m-%d")}})
                    else:
                        continue
                elif "PhoneNumber" in i:
                    if"is a valid PhoneNumber" in extract_valid_phone_numbers(data['PhoneNumber']):
                        missed_data1.append({"$set": {i[:-2]+"Interaction"+str(i[-1]): user_to_update[i.split('.')[0]][[i.split('.')[1]][0][:-2]+"Interaction"+i[-1]]+1}})
                        missed_data1.append({"$set": {i[:-2]+"LastInteractionDate"+str(i[-1]): today_date.strftime("%Y-%m-%d")}})
                    else:
                        print(data["PhoneNumber"])
                        continue

                else:
                    missed_data1.append({"$set": {i[:-2]+"Interaction"+str(i[-1]): user_to_update[i.split('.')[0]][[i.split('.')[1]][0][:-2]+"Interaction"+i[-1]]+1}})
                    missed_data1.append({"$set": {i[:-2]+"LastInteractionDate"+str(i[-1]): today_date.strftime("%Y-%m-%d")}})

            update_result = collection.update_one({"_id": user_to_update["_id"]},missed_data1)
            user_to_update = collection.find_one(query)
            if "is a valid email address" in is_valid_email(data['Email']):
                Email_no = get_key_by_value(user_to_update['Email'],max([num for num in list(user_to_update['Email'].values()) if isinstance(num, (int))]))[-1][-1]
                email_res = user_to_update['Email']['Email'+"_"+Email_no]
            else:
                email_res = is_valid_email(data['Email'])

            ExternalID_no =get_key_by_value(user_to_update['ExternalID'],max([num for num in list(user_to_update['ExternalID'].values()) if isinstance(num, (int))]))[-1][-1]
            if"is a valid PhoneNumber" in extract_valid_phone_numbers(data['PhoneNumber']):
                PhoneNumber_no =get_key_by_value(user_to_update['PhoneNumber'],max([num for num in list(user_to_update['PhoneNumber'].values()) if isinstance(num, (int))]))[-1][-1]
                phone_res =user_to_update['PhoneNumber']['PhoneNumber'+"_"+PhoneNumber_no]
            else:
                phone_res= extract_valid_phone_numbers(data['PhoneNumber'])
            result ={'Suntory_ID': user_to_update['Suntory_ID'],
                    'Username': user_to_update['Username'],
                    'ExternalID': user_to_update['ExternalID']['ExternalID'+"_"+ExternalID_no],
                    "Email": email_res,
                    'PhoneNumber':phone_res }

            if update_result.modified_count > 0:
                results["email_id_update"] = "Yes"
            else:
                results["email_id_update"] = "No"
        # if no entry match with existing database
        else:
            missed_data1={'Suntory_ID':" ",
                          "Username":" ",
                          "PhoneNumber":{},
                         "ExternalID":{},
                         "Email":{}}

            missed_data1['Suntory_ID']= str(uuid.uuid4())
            if 'Username' in data.keys():
                missed_data1['Username']= data.get('Username')
            for i in missed_data:
                if i=="Email":
                    if "is a valid email address" in is_valid_email(data['Email']):
                        number = 1
                        missed_data1[i][i+"_"+str(number)]= data.get(i)
                        missed_data1[i][i+"Interaction"+str(number)]= 1
                        missed_data1[i][i+"LastInteractionDate"+str(number)]= today_date.strftime("%Y-%m-%d")
                    else:
                        continue
                elif i=="PhoneNumber":
                    if "is a valid PhoneNumber" in extract_valid_phone_numbers(data['PhoneNumber']):
                        number = 1
                        missed_data1[i][i+"_"+str(number)]= data.get(i)
                        missed_data1[i][i+"Interaction"+str(number)]= 1
                        missed_data1[i][i+"LastInteractionDate"+str(number)]= today_date.strftime("%Y-%m-%d")
                    else:
                        continue
                else:
                    number = 1
                    missed_data1[i][i+"_"+str(number)]= data.get(i)
                    missed_data1[i][i+"Interaction"+str(number)]= 1
                    missed_data1[i][i+"LastInteractionDate"+str(number)]= today_date.strftime("%Y-%m-%d")

            update_result = collection.insert_one(missed_data1)
            if "is a valid email address" in is_valid_email(data['Email']):
                email_res = missed_data1['Email']['Email_1']
            else:
                email_res = is_valid_email(data['Email'])

            if"is a valid PhoneNumber" in extract_valid_phone_numbers(data['PhoneNumber']):
                phone_res =missed_data1['PhoneNumber']['PhoneNumber_1']
            else:
                phone_res= extract_valid_phone_numbers(data['PhoneNumber'])
            result ={'Suntory_ID': missed_data1['Suntory_ID'],
                    'Username': missed_data1['Username'],
                    'ExternalID': missed_data1['ExternalID']['ExternalID_1'],
                    "Email": email_res,
                    'PhoneNumber': phone_res}
                
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    
app = Flask(__name__)    
    
@app.route('/check_existence', methods=['POST'])
def check_existence_endpoint():
    data = request.get_json()

    if not any(data.values()):
        return jsonify({"error": "At least one identifier (email_id, phone_number, e_id) is required"}), 400

    return check_existence(data)


if __name__ == '__main__':
    app.run("127.0.0.1", 5009)
