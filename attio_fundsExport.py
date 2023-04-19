import requests
import json
import pandas as pd

# Get deal pipeline items

url = 'https://api.attio.com/v1/collections/fd607822-c6e7-4902-a7c5-52444eb83428/entries'
auth = ('a9f9f2846a8e4bc05f2a246283d082d326c533d409bfa70193319ab5c65bae8f', '')

responselength = 500
offset = 0
data = []

# Attio API response is limited to 500 companies, so the following loop
# sends requests until the API responds with less then 500 companies
# as such a response means that the API sent the final batch of companies

while responselength >= 499:
    params = {'limit': '500', 'offset':offset}
    response = requests.get(url, auth=auth, params=params)
    attio_apiresponse = response.content

    # load response content into a dictionary
    json_data = json.loads(attio_apiresponse)
    attio_funds_output = json_data["data"] 

    for entry in attio_funds_output:
        data.append(entry)

    offset += 500
    responselength = len(attio_funds_output)

#   The most important thing we got is "data" which is a list object, where each entry is a dict that is a "collection" (attio concept) of info on a fund. Important keys are:
#   "record" – dict itself, has attio native data on company:
#        (1) company name
#        (2) unique id
#        (3) description
#        (4) domains [list obj]
#        (5) primary_location [dict, take country_name value]
#        (6) social_media [dict of dicts where keys are each sm and nested keys are either handle or url, take "linkedin" key and "url" nested key]
#           * these dicts are calleable by these names
#   "attributes" – dict itself, holds all user defined attributes:
#           * these dicts are calleable by their ids
#        (1) 096ce615-2a48-47a3-a088-55997a1d7239 – next steps
#        (2) 4284fd99-5dbf-4c6b-9359-5c332fb64e8d – key contact [dict, ___copy from pipeline export___]
#        (3) f0e44c11-639c-4201-9ab4-611860da5e24 – key contact 2 [text]
#        (4) 610f0945-9fc1-4193-b357-c8517505ab21 – notes
#        (5) 9d9f84c4-ab7c-45fd-b283-8de1b40432d9 – priority [dict, take key "title"]
#        (6) b252c503-d972-4334-a57a-8789a4464f3f – conversation stage [dict, take key "name"]
#        (7) d7e3e6c3-2740-478d-a1a8-570c5561a8e3 – stage
#        (8) dfdb6a79-705f-41f5-ad91-e6f2b5b40507 – focus
#        (9) ff14f4db-57cb-42f3-9ddd-a2ab30b2bc6a – geography

#        (*) 465f7a52-1bed-4447-aab8-f343e0a903a3 – conversation stage - Duplicated, archived

funds = {}

for collection in data:
    
    # Collecting record Attio data:

    record_data = collection["record"]
    
    T_id = record_data["id"]
    T_name = record_data["name"]
    T_description = record_data["description"]
    T_domains = ", ".join(record_data["domains"])
    T_s_location = record_data["primary_location"]
    if T_s_location is not None:
        T_location = T_s_location["country_name"]
    else:
        T_location = None
    T_s_s_linkedin = record_data["social_media"]
    if T_s_s_linkedin is not None:
        T_s_linkedin = T_s_s_linkedin["linkedin"]
    else:
        T_linkedin = None
    if T_s_linkedin is not None:
        T_linkedin = T_s_linkedin["url"]
    else:
        T_linkedin = None

    # Collecting user defined attributes data:

    attributes_data = collection["attributes"]

    T_nextsteps = attributes_data["096ce615-2a48-47a3-a088-55997a1d7239"]

    T_s_s_keycontact = attributes_data["4284fd99-5dbf-4c6b-9359-5c332fb64e8d"]
    if T_s_s_keycontact is None:
        T_keycontact = None
    else:
        T_keycontact = []
        for i in T_s_s_keycontact:
            if i["contact_type"] == "person":
                if i["first_name"] is not None and i["last_name"] is not None:
                    T_keycontact.append(" ".join([i["first_name"], i["last_name"]]))
            if i["contact_type"] == "company":
                if i["contact_type"] is not None:
                    T_keycontact.append(i["name"])
        T_keycontact = [i for i in T_keycontact if i is not None]
        T_keycontact = ", ".join(T_keycontact)

    T_keycontact2 = attributes_data["f0e44c11-639c-4201-9ab4-611860da5e24"]

    T_notes = attributes_data["610f0945-9fc1-4193-b357-c8517505ab21"]

    T_s_priority = attributes_data["9d9f84c4-ab7c-45fd-b283-8de1b40432d9"]
    if T_s_priority is not None:
        if isinstance(T_s_priority, list):
            T_priority = T_s_priority[0]["title"]
        else:
            T_priority = T_s_priority["title"]

    T_s_convstage = attributes_data["b252c503-d972-4334-a57a-8789a4464f3f"]
    if T_s_convstage is not None:
        T_convstage = T_s_convstage["name"]

    T_stage = attributes_data["d7e3e6c3-2740-478d-a1a8-570c5561a8e3"]

    T_focus = attributes_data["dfdb6a79-705f-41f5-ad91-e6f2b5b40507"]

    T_geography = attributes_data["ff14f4db-57cb-42f3-9ddd-a2ab30b2bc6a"]

    # Creating a funds entry in the "funds" dict:
    funds[T_id] = {
        "name":T_name,
        "conv_stage":T_convstage,
        "next_steps":T_nextsteps,
        "priority":T_priority,
        "keycontact":T_keycontact,
        "description":T_description,
        "stage":T_stage,
        "focus":T_focus,
        "geography":T_geography,
        "notes":T_notes,
        "location":T_location,
        "linkedin":T_linkedin,
        "domain":T_domains
        }


# Load funds dict into a pandas dataframe
df = pd.DataFrame.from_dict(funds, orient='index')

print(df)

# Sort on conv_stage
sort_order = ["Subscribed", "Work in progress", "Pipeline access", 'Conversation', 'Pre-review', 'On hold', 'No-go']

df['conv_stage'] = pd.Categorical(df['conv_stage'], categories=sort_order, ordered=True)

df = df.sort_values('conv_stage')

# Add empty rows after each opp_status category

grouped = df.groupby('conv_stage')

new_df = pd.DataFrame(columns=df.columns)

for name, group in grouped:
    new_df = new_df.append(group)
    new_df = new_df.append(pd.Series([None] * len(df.columns), index=df.columns), ignore_index=True)

new_df = new_df[:-1]

df = new_df

# delete repeated-blank rows

mask = (df['conv_stage'].isna()) & (df['conv_stage'].shift().isna())
df.drop(index=df[mask].index, inplace=True)

# add first blank row

df.loc[-1] = [None] * len(df.columns)
df.index = df.index + 1
df.sort_index(inplace=True)


# add conv_stage category group labels

df['new_convstage'] = df['conv_stage'].fillna(method='bfill')
df = df.dropna(subset=['new_convstage'])
df['new_convstage'] = df['new_convstage'].astype(str)
df.loc[df['new_convstage'].duplicated(), 'new_convstage'] = ''
new_order = ['new_convstage'] + [col for col in df.columns if col != 'new_convstage']
df = df[new_order]

df = df.rename(columns={'new_convstage': ''})


# capitalise column names

df = df.rename(columns=lambda x: x.upper())

# delete funds sheet if it exists

import xlwings as xw    

targetbook = 'package/attio_macroloadedbook.xlsm'

wb = xw.Book(targetbook, notify=False, update_links=False)

sheet_names = [sheet.name for sheet in wb.sheets]

if "funds" in sheet_names:
    wb.sheets["funds"].delete()


# import dataframe as new sheet into the macro loaded workbook

new_sheet_name = 'funds'

wb.sheets.add(new_sheet_name)
wb.sheets[new_sheet_name].range('A1').value = df


# Run macro
macro1 = wb.macro("Macro1")

macro1()

"""
wb.save()
wb.close()
wb.app.quit()
"""