import pandas as pd
import boto3
import requests
from io import BytesIO
from datetime import datetime
from unidecode import unidecode
import streamlit as st
import time
from dotenv import load_dotenv
import os


def start_step4(username):
    with st.status("Loading step 4 process...", expanded=True) as status:
        try:
            load_dotenv()
            aws_access_key_id = os.getenv("DB_AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("DB_AWS_SECRET_ACCESS_KEY")
            aws_bucket_name = os.getenv("AWS_BUCKET_NAME")
            st.write("Loading AWS S3...")
            time.sleep(3)
            # AWS S3 Setup
            AWS_ACCESS_KEY_ID = aws_access_key_id
            AWS_SECRET_ACCESS_KEY = aws_secret_access_key
            BUCKET_NAME = aws_bucket_name

            s3_client = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )

            # Function to upload image to S3 and return the new URL
            def upload_image_to_s3(image_url, title):
                try:
                    print(f"Downloading image from {image_url}")
                    st.write(f"Downloading image from {image_url}")
                    response = requests.get(image_url)
                    if response.status_code == 200:
                        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                        title_snippet = unidecode(
                            "".join(e for e in title if e.isalnum())[:10]
                        )
                        filename = f"{timestamp}_{title_snippet}.jpg"
                        filepath = f"images/{filename}"
                        s3_client.upload_fileobj(
                            BytesIO(response.content),
                            BUCKET_NAME,
                            filepath,
                            ExtraArgs={"ContentType": "image/jpeg"},
                        )
                        new_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{filepath}"
                        print(f"Uploaded image to {new_url}")
                        st.write(f"Uploaded image to {new_url}")
                        return new_url
                    else:
                        print(f"Failed to download image from {image_url}")
                        st.write(f"Failed to download image from {image_url}")
                        return None
                except Exception as e:
                    print(f"Exception occurred: {e}")
                    st.write(f"Exception occurred: {e}")
                    return None

            # Load the datasets
            database_df = pd.read_csv(f"./users/{username}/uploads/database.csv")
            full_data_df = pd.read_csv(
                f"./users/{username}/uploads/full_data_export.csv"
            )

            # Identifying the columns to keep
            columns_to_keep = database_df.columns.tolist()

            # Filtering the items
            items_to_add = []

            for _, row in full_data_df.iterrows():
                # Skip items marked as "Manual Entry"
                manual_entry = (
                    str(row.get("Manual Entry", "")).strip().lower()
                )  # Ensure it's treated as a string
                if manual_entry == "true":
                    continue

                # Initialize a flag to track if the item has a unique ASIN, FBA, or UPC code
                unique_code_present = any(
                    pd.notnull(row[code_type])
                    and row[code_type] not in database_df[code_type].values
                    for code_type in ["Code-ASIN", "Code-FBA", "Code-UPC"]
                )

                # Proceed if there's a unique code
                if unique_code_present:
                    print(f"Processing item with unique codes: {row['Title']}")
                    st.write(f"Processing item with unique codes: {row['Title']}")
                    s3_image_urls = {}
                    for col in (
                        ["Auto-Image"]
                        + [f"Image {i}" for i in range(1, 11)]
                        + [f"Online Image {i}" for i in range(1, 11)]
                    ):
                        if pd.notnull(row[col]) and "aucwarehouse" not in row[col]:
                            new_url = upload_image_to_s3(row[col], row["Title"])
                            if new_url:
                                s3_image_urls[col] = new_url
                            else:
                                s3_image_urls[col] = ""
                        else:
                            s3_image_urls[col] = row[col]

                    # Update row with S3 image URLs and add to the list
                    for col, url in s3_image_urls.items():
                        row[col] = url
                    items_to_add.append(row)

            # Creating the dataframe to export
            items_to_add_df = pd.DataFrame(items_to_add, columns=columns_to_keep)

            if items_to_add_df.empty:
                print(
                    "No items to add based on the criteria. Creating an empty database_import.csv."
                )
                st.write(
                    "No items to add based on the criteria. Creating an empty database_import.csv."
                )
                items_to_add_df.to_csv(
                    f"./users/{username}/outputs/database_import.csv", index=False
                )
            else:
                print("database_import.csv has been created and saved successfully.")
                st.write("database_import.csv has been created and saved successfully.")
                items_to_add_df.to_csv(
                    f"./users/{username}/outputs/database_import.csv", index=False
                )
            status.update(label="Step 4 Completed!", state="complete", expanded=False)
            return True
        except Exception as e:
            st.error(f"Something went wrong while running the step 4. Error: {e}")
            status.update(label="Step 4 Failed!", state="error", expanded=True)
            return False
