import pandas as pd
import os


#Input user parameters:
OBJECT_NAME = contacts
ORIGINAL_FILE_PATH = '/Users/smothwood/Downloads/ContactNotesInsert1.csv'
DOWNLOAD_FOLDER = '/Users/smothwood/salesforce_files'
RELATED_OBJECT_ID = "Related Contact Id"

## Phase 1: Upload Notes into Salesforce
    ##Steps 1 - 3 in this guide: https://help.salesforce.com/s/articleView?id=000387816&type=1

#load up csv:
df = pd.read_csv(ORIGINAL_FILE_PATH, encoding='utf-8')

##create a file name column:
df['RowNumber'] =  range(len(df))
df.RowNumber = df.RowNumber.apply(str)
df["FileName"] = f"/Users/smothwood/salesforce_files/{OBJECT_NAME}_" + df[RELATED_OBJECT_ID] + "_" + df["RowNumber"] + ".txt"

##clean up special characters as per Salesforce documentation:
df['Content'] = df['Content'].str.replace(r'&', '&amp;')
df['Content'] = df['Content'].str.replace(r'<', '&lt;')
df['Content'] = df['Content'].str.replace(r'>', '&gt;')
df['Content'] = df['Content'].str.replace(r'"', '&quot;')
df['Content'] = df['Content'].str.replace(r"'", '&#39;')

##define function for saving each row to a text file locally
def file_saver (row):
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    with open(f'{row.FileName}',"w") as fp:
        fp.write(row.Content)

##apply file_saver function across all rows in the csv
df.apply(file_saver, axis=1)

##create a copy of the csv with only the columns needed for data loader
df_edit = df[['Title', 'FileName', 'OwnerId']].copy()
df_edit.rename(columns={'FileName': 'Content'}, inplace=True)

##save this new csv
df_edit.to_csv(f'{DOWNLOAD_FOLDER}/{OBJECT_NAME}_final.csv', index=False)

############################################################################################################
## Phase 2: Link Notes Records to Entities in Salesforce
    ## Step 4 in this guide: https://help.salesforce.com/s/articleView?id=000387816&type=1

#read in successes file from data uploader after Phase 1
df_success = pd.read_csv('/Users/smothwood/salesforce_files/contacts/success071624053556949.csv')

#connect successes file to the original csv to link notes with contact ids

merged_table = df_success.merge(df, left_on = "Content", right_on = "FileName")

#Create a new CSV file with the following columns: ContentDocumentId, LinkedEntityId, ShareType, and Visibility.
df_link = merged_table[['ID', RELATED_OBJECT_ID]].copy()
df_link.rename(columns ={'ID': 'ContentDocumentId', RELATED_OBJECT_ID:'LinkedEntityId'}, inplace=True)

df_link["ShareType"] = "I"
df_link["Visibility"] = "AllUsers"

#save to csv to use in data uploader
df_link.to_csv(f'/Users/smothwood/salesforce_files/{OBJECT_NAME}_link.csv', index=False)