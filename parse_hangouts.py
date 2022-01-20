import pandas as pd
import json
from datetime import datetime, timedelta

# Creating empty dataframes
conversations = pd.DataFrame(columns=['conversation_id','conversation_type', 'conversation_name'])
people = pd.DataFrame(columns=['person_id','person_name'])
messages = pd.DataFrame(columns=['conversation_id','person_id','msg_timestamp', 'msg_text'])

print("Start at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

with open('Hangouts.json') as json_file:

    json_dict = json.load(json_file)

    # Accessing Conversation Object
    for c in json_dict['conversations']:
        json_conversation = c['conversation']
        json_participants = json_conversation['conversation']['participant_data']
        json_events = c['events']


        # Accessing conversation basic info
        if 'name' in json_conversation['conversation']:
            conversation = {
                                'conversation_id': json_conversation['conversation_id']['id'],
                                'conversation_type': json_conversation['conversation']['type'],
                                'conversation_name': json_conversation['conversation']['name']
                           }
        else:
            conversation = {
                                'conversation_id': json_conversation['conversation_id']['id'],
                                'conversation_type': json_conversation['conversation']['type'],
                                'conversation_name': ''
                           }
        print(conversation['conversation_id'] + ' ' + conversation['conversation_name'])

        # Adding all conversation info to Conversations dataframe
        conversations = conversations.append(conversation,ignore_index=True)


        # Accessing participants info
        for p in json_participants:

            if 'fallback_name' in p:
                participant = {
                                'person_id': p['id']['gaia_id'],
                                'person_name': p['fallback_name']
                              }
            else:
                participant = {
                                'person_id': p['id']['gaia_id'],
                                'person_name': ''
                              }

            # Adding all participants info to People dataframe
            people = people.append(participant,ignore_index=True)


        # Accessing events info
        for e in json_events:
            if 'chat_message' in e and 'segment' in e['chat_message']['message_content']:
                for s in e['chat_message']['message_content']['segment']:
                    if (s['type'] == 'TEXT' or s['type']=='LINK') and s['text'] != '':
                        event = {
                                    'conversation_id': e['conversation_id']['id'],
                                    'person_id': e['sender_id']['gaia_id'],
                                    'msg_timestamp': e['timestamp'],
                                    'msg_text': s['text']
                                }

                        # Adding all events info to Messages dataframe
                        messages = messages.append(event,ignore_index=True)

print("Start sorting at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Removing duplicates and sorting
conversations = conversations.drop_duplicates(subset=['conversation_id']).sort_values(by=['conversation_name'])
people = people.drop_duplicates(subset=['person_id']).sort_values(by=['person_name'])
messages = messages.sort_values(by=['conversation_id','msg_timestamp'])

# Converting integer timestamp to datetime
messages['msg_timestamp'] = messages['msg_timestamp'].apply(lambda x: datetime(1970,1,1) + timedelta(milliseconds=int(x)//1000))

print("Start merging at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Merging Messages to People and Conversations to have the structure:
# 'conversation_id','conversation_type', 'conversation_name','person_id','person_name','msg_timestamp', 'msg_text'
archive = pd.merge(pd.merge(messages, people, on=['person_id'], how='left'), conversations, on=['conversation_id'], how='left')

print("Start saving at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

archive.to_csv('HangoutsMsgArchive.csv', index=False)
