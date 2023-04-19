import requests
import json
import pandas as pd

# Get deal pipeline items

url = 'https://api.attio.com/v1/collections/29135bd9-3e9d-47ba-b92b-916b64f47242/entries'
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
    attio_dealpipeline_output = json_data["data"] 

    for entry in attio_dealpipeline_output:
        data.append(entry)

    offset += 500
    responselength = len(attio_dealpipeline_output)

#   The most important thing we got is "data" which is a list object, where each entry is a dict that is a "collection" (attio concept) of info on a company. Important keys are:
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
#        (1) 6788c35c-a393-42cd-9cfb-86ac8f00c666 – passed/watchlisted reason
#        (2) 53ea71bf-93bc-4ba2-8c10-d4952b9c438c – source type [dict, get "title" key for proper value
#        (3) 6b5507ec-ff52-4de4-be73-f22b714a4207 – deal
#        (4) 8dff7eef-18d3-47df-97f2-03514d541eba – valuation
#        (5) dff3e21d-72e4-438e-a3c8-08b23faaf560 – next steps
#        (6) e72194e8-c77b-48bb-9ac9-c6ac78a3c063 – space [list of dicts, get value for "title" key from each dict to get all spaces company has been labeled to belong to]
#        (7) f035debb-fd9e-4ec6-8c7a-840b705944ef – opp status [dict, get value for "name" key]
#        (8) f7ff2094-a97d-4b9c-8cc2-159840f13cd9 – data room
#        (9) 034b934c-1bd0-4831-8fa6-776788564678 – deal source [dict for two types of entities, a company or a person]


companies = {}

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

    T_pwreason = attributes_data["6788c35c-a393-42cd-9cfb-86ac8f00c666"]
    T_s_sourcetype = attributes_data["53ea71bf-93bc-4ba2-8c10-d4952b9c438c"]
    if T_s_sourcetype is not None:
        T_sourcetype = T_s_sourcetype[0]["title"]
    else:
        T_sourcetype = None
    T_deal = attributes_data["6b5507ec-ff52-4de4-be73-f22b714a4207"]
    T_valuation = attributes_data["8dff7eef-18d3-47df-97f2-03514d541eba"]
    T_nextsteps = attributes_data["dff3e21d-72e4-438e-a3c8-08b23faaf560"]
    
    T_list__space = attributes_data["e72194e8-c77b-48bb-9ac9-c6ac78a3c063"]
    T_list_spaces = []
    if T_list__space is not None:
        for i in T_list__space:
            if i is not None:
                T_list_spaces.append((i["title"]))
    T_spaces = ", ".join(T_list_spaces)
    T_dataroom = attributes_data["f7ff2094-a97d-4b9c-8cc2-159840f13cd9"]
    T_s_oppstatus = attributes_data["f035debb-fd9e-4ec6-8c7a-840b705944ef"]
    if T_s_oppstatus is not None:
        T_oppstatus = T_s_oppstatus["name"]
    else:
        T_oppstatus = None
    T_s_s_source = attributes_data["034b934c-1bd0-4831-8fa6-776788564678"]
    if T_s_s_source is None:
        T_source = None
    else:
        T_source = []
        for i in T_s_s_source:
            if i["contact_type"] == "person":
                if i["first_name"] is not None and i["last_name"] is not None:
                    T_source.append(" ".join([i["first_name"], i["last_name"]]))
            if i["contact_type"] == "company":
                if i["contact_type"] is not None:
                    T_source.append(i["name"])
        T_source = [i for i in T_source if i is not None]
        T_source = ", ".join(T_source)


    # load data on a company into a list
    #firm = [T_id, T_name, T_oppstatus,T_deal, T_valuation, T_nextsteps, T_spaces, T_description, T_location, T_source, T_sourcetype, T_pwreason, T_dataroom, T_linkedin, T_domains]

    # Creating a company entry in the "companies" dict:
    companies[T_id] = {
        "name":T_name,
        "opp_status":T_oppstatus,
        "deal":T_deal,
        "valuation":T_valuation,
        "next_steps":T_nextsteps,
        "source":T_source,
        "source_type":T_sourcetype,
        "spaces":T_spaces,
        "description":T_description,
        "location":T_location,
        "pw_reason":T_pwreason,
        "data_room":T_dataroom,
        "linkedin":T_linkedin,
        "domain":T_domains
        }


# Load companies dict into a pandas dataframe
df = pd.DataFrame.from_dict(companies, orient='index')


# Sort on opp_status
sort_order = ["Portfolio", "IC", 'On review', 'Pre-review', 'Watchlist', "Exited", 'Passed']

df['opp_status'] = pd.Categorical(df['opp_status'], categories=sort_order, ordered=True)

df = df.sort_values('opp_status')

# Add empty rows after each opp_status category

grouped = df.groupby('opp_status')

new_df = pd.DataFrame(columns=df.columns)

for name, group in grouped:
    new_row = pd.Series([None] * len(df.columns), index=df.columns)
    new_df = new_df.append(new_row, ignore_index=True)

    new_df = new_df.append(group)
    new_df = new_df.append(pd.Series([None] * len(df.columns), index=df.columns), ignore_index=True)

new_df = new_df[:-1]

df = new_df

# delete repeated-blank rows

mask = (df['opp_status'].isna()) & (df['opp_status'].shift().isna())
df.drop(index=df[mask].index, inplace=True)

# add first blank row

df.loc[-1] = [None] * len(df.columns)
df.index = df.index + 1
df.sort_index(inplace=True)

# add opp_status category group labels

df['new_opp_status'] = df['opp_status'].fillna(method='bfill')
df = df.dropna(subset=['new_opp_status'])
df['new_opp_status'] = df['new_opp_status'].astype(str)
df.loc[df['new_opp_status'].duplicated(), 'new_opp_status'] = ''
new_order = ['new_opp_status'] + [col for col in df.columns if col != 'new_opp_status']
df = df[new_order]

df = df.rename(columns={'new_opp_status': ''})

# capitalise column names

df = df.rename(columns=lambda x: x.upper())

# delete company sheet if it exists

import xlwings as xw    

targetbook = 'package/attio_macroloadedbook.xlsm'

wb = xw.Book(targetbook, notify=False, update_links=False)

sheet_names = [sheet.name for sheet in wb.sheets]

if "companies" in sheet_names:
    wb.sheets["companies"].delete()


# import dataframe as new sheet into the macro loaded workbook

new_sheet_name = 'companies'

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