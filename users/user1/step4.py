import pandas as pd
import boto3
import requests
from io import BytesIO
from datetime import datetime
from unidecode import unidecode

# AWS S3 Setup
AWS_ACCESS_KEY_ID = 'placeholder'
AWS_SECRET_ACCESS_KEY = 'placeholder'
BUCKET_NAME = 'placeholder'

s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# Function to upload image to S3 and return the new URL
def upload_image_to_s3(image_url, title):
    try:
        print(f"Downloading image from {image_url}")
        response = requests.get(image_url)
        if response.status_code == 200:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            # Transliterate title to ASCII, removing accents and special characters
            title_snippet = unidecode(''.join(e for e in title if e.isalnum())[:10])
            filename = f"{timestamp}_{title_snippet}.jpg"
            filepath = f"images/{filename}"
            s3_client.upload_fileobj(BytesIO(response.content), BUCKET_NAME, filepath, ExtraArgs={'ContentType': 'image/jpeg'})
            new_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{filepath}"
            print(f"Uploaded image to {new_url}")
            return new_url
        else:
            print(f"Failed to download image from {image_url}")
            return None
    except Exception as e:
        print(f"Exception occurred: {e}")
        return None

# Load the datasets
database_df = pd.read_csv('database.csv')
full_data_df = pd.read_csv('full_data_export.csv')

# Identifying the columns to keep
columns_to_keep = database_df.columns.tolist()

# Preparing a list of image columns
image_columns = ['Auto-Image'] + [f'Image {i}' for i in range(1, 11)] + [f'Online Image {i}' for i in range(1, 11)]
keep_image_columns = [f'Keep Image {i}?' for i in range(1, 11)]

# Filtering the items
items_to_add = []

for _, row in full_data_df.iterrows():
    # Initialize a flag to track if the item has a unique ASIN or FBA code
    unique_asin_or_fba_present = False
    
    # Check for unique ASIN and FBA codes in full_data_export.csv not in database.csv
    for code_type in ['Code-ASIN', 'Code-FBA', 'Code-UPC']:
        if pd.notnull(row[code_type]) and (row[code_type] not in database_df[code_type].values):
            unique_asin_or_fba_present = True
            break

    # Initialize a flag to track if any image is not in S3
    any_image_not_in_s3 = any(pd.notnull(row[col]) and 'aucwarehouse' not in row[col] for col in image_columns)

    # Proceed if the title is new or if there's a unique ASIN or FBA code
    if (unique_asin_or_fba_present or (row['Title'] not in database_df['Title'].values)) and any_image_not_in_s3:
        print(f"Processing item: {row['Title']}")
        # Check if images need to be processed (not all images contain "aucwarehouse")
        s3_image_urls = {}
        for col in image_columns:
            if col.startswith('Online Image'):
                # Extract the number from the column name
                image_number = int(col.split()[-1])
                keep_col = f'Keep Image {image_number}?'
                # For Online Image fields, skip if 'Keep Image X?' is False or if the column doesn't exist
                if isinstance(row[keep_col], bool) and not row[keep_col]:
                    # If 'Keep Image X?' is False, set the 'Online Image X' column to blank
                    row[col] = ''
                    continue
                elif pd.notnull(row[col]) and 'aucwarehouse' not in row[col]:
                    # Handle S3 upload for Online Image fields
                    new_url = upload_image_to_s3(row[col], row['Title'])
                    if new_url:
                        s3_image_urls[col] = new_url
            elif col.startswith('Image'):
                # For Image fields, wipe them completely blank
                row[col] = ''
                continue
            elif col == 'Auto-Image' and pd.notnull(row[col]) and 'aucwarehouse' not in row[col]:
                # Handle S3 upload for Auto-Image fields
                new_url = upload_image_to_s3(row[col], row['Title'])
                if new_url:
                    s3_image_urls[col] = new_url
            else:
                # Keep existing URLs or leave blank
                s3_image_urls[col] = row[col]

        # Update row with S3 image URLs
        for col, url in s3_image_urls.items():
            row[col] = url
        items_to_add.append(row)

# Creating the dataframe to export
items_to_add_df = pd.DataFrame(items_to_add)

if items_to_add_df.empty:
    print("No items to add based on the criteria. Creating an empty database_import.csv.")
    # Create an empty DataFrame with the same columns as database_df
    empty_df = pd.DataFrame(columns=database_df.columns)  # Use the correct DataFrame variable name here
    # Save the empty DataFrame to CSV
    empty_df.to_csv('database_import.csv', index=False)
else:
    # Continue with the rest of your script for non-empty cases
    # Keeping only the necessary columns and marking as manual entry
    items_to_add_df = items_to_add_df[columns_to_keep]
    items_to_add_df['Manual Entry'] = True
    # Save to CSV
    items_to_add_df.to_csv('database_import.csv', index=False)
    print("database_import.csv has been created and saved successfully.")
