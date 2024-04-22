import pandas as pd
import requests
import os
from PIL import Image, ImageDraw, ImageFont
import time
import streamlit as st


def start_step3(username):
    st.write("This step is trigrred!")
    with st.status("Loading step 3 process...", expanded=True) as status:
        try:
            st.write("Loading full_data_export.csv file...")
            time.sleep(3)
            # Load the CSV file
            csv_file_path = f"./users/{username}/uploads/full_data_export.csv"
            df = pd.read_csv(csv_file_path)

            # Clone the 'Condition' column
            df["Original Condition"] = df["Condition"]

            # Initialize Description 4 and Description 5 with default values
            df["Description 4"] = ""
            df["Description 5"] = ""

            # Function to trim title without cutting words in half and ensure total length does not exceed 50 characters including "..."
            def smart_trim_title(title, max_length=50):
                if len(title) <= max_length:
                    return title  # Return as is if within max length
                trimmed_title = title[:max_length].rsplit(" ", 1)[
                    0
                ]  # Try to split by last space within limit
                if (
                    len(trimmed_title) + 3 > max_length
                ):  # Check if adding "..." exceeds max length
                    trimmed_title = trimmed_title.rsplit(" ", 1)[
                        0
                    ]  # Remove one more word if it does
                return (
                    trimmed_title + "..." if trimmed_title else title[:47] + "..."
                )  # Add "..." or handle very long word

            def update_descriptions(row):
                prefix = ""
                suffix = ""
                # Ensure the description is treated as a string, handling NaN values and empty strings appropriately
                description = (
                    str(row["Description"]).strip()
                    if not pd.isnull(row["Description"])
                    and row["Description"].strip() != ""
                    else ""
                )

                # Only process non-empty descriptions
                if description:
                    # Check if description is longer than 220 characters
                    if len(description) > 220:
                        # Find the last space before the 220th character to avoid cutting words in half
                        split_index = description.rfind(" ", 0, 220)
                        # If there's no space (e.g., a very long word), just split at 220
                        if split_index == -1:
                            split_index = 220
                        row["Description 4"] = (
                            prefix + description[:split_index] + suffix
                        )
                        # Ensure Description 5 starts with the content right after split_index
                        row["Description 5"] = (
                            prefix + description[split_index:].strip() + suffix
                        )
                    else:
                        # For descriptions shorter than or equal to 220 characters
                        row["Description 4"] = prefix + description + suffix
                        row["Description 5"] = (
                            ""  # Keep Description 5 empty if not needed
                        )
                else:
                    # Keep both Description 4 and Description 5 empty if the original description is empty
                    row["Description 4"] = ""
                    row["Description 5"] = ""

                return row

            # Apply the function to update Description 4 and 5
            df = df.apply(update_descriptions, axis=1)

            # Function to trim text and add ellipsis if over a specified length
            def trim_and_ellipsis(text, max_length):
                if len(text) > max_length:
                    return text[:max_length] + "..."
                return text

            # Before applying 'smart_trim_title', convert each title to a string
            df["Title"] = df["Title"].astype(str)

            # Update 'Title' to 'Description 1' with formatting and trimming if necessary
            df["Description 1"] = df["Title"].apply(
                lambda x: trim_and_ellipsis(str(x), 220)
            )
            df["Title"] = df["Title"].apply(smart_trim_title)

            # Adjust 'Notes' handling to account for N/A
            df.rename(columns={"Notes": "Description 2"}, inplace=True)

            # Rename 'Condition' to 'Description 3' and update formatting
            df.rename(columns={"Condition": "Description 3"}, inplace=True)

            def download_image(image_url, filename, quantity=None, retries=3):
                """Downloads an image, saves it with the specified filename, and optionally adds quantity text if needed.
                Retries a specified number of times with a 3-second pause between attempts.
                """
                if pd.isnull(image_url):
                    return
                attempt = 0
                while attempt < retries:
                    try:
                        response = requests.get(image_url, timeout=30)
                        if response.status_code == 200:
                            with open(filename, "wb") as f:
                                f.write(response.content)

                            img = Image.open(filename)

                            # Convert the image to RGB if it's in P mode (palette-based)
                            if img.mode == "P" or img.mode == "RGBA" or img.mode == "1":
                                img = img.convert("RGB")

                            if quantity and quantity > 1:
                                draw = ImageDraw.Draw(img)
                                font_size = int(img.width * 0.25)
                                try:
                                    font = ImageFont.truetype("arial.ttf", font_size)
                                except IOError:
                                    font = ImageFont.load_default()
                                text = f"x{quantity}"
                                outline_thickness = int(font_size * 0.035)
                                offsets = [
                                    (x, y)
                                    for x in range(
                                        -outline_thickness, outline_thickness + 1
                                    )
                                    for y in range(
                                        -outline_thickness, outline_thickness + 1
                                    )
                                    if x or y
                                ]
                                x, y = 10, 10
                                for offset in offsets:
                                    draw.text(
                                        (x + offset[0], y + offset[1]),
                                        text,
                                        font=font,
                                        fill="black",
                                    )
                                draw.text((x, y), text, font=font, fill="white")

                            img.save(filename)
                            st.write(f"Downloaded {filename}")
                            print(f"Downloaded {filename}", flush=True)

                            return  # Exit function after successful download
                        else:
                            print(
                                f"Failed to download image from {image_url}, status code: {response.status_code}"
                            )
                            st.write(
                                f"Failed to download image from {image_url}, status code: {response.status_code}"
                            )
                    except requests.exceptions.RequestException as e:
                        print(f"Attempt {attempt + 1} failed for {image_url}: {e}")
                        st.write(f"Attempt {attempt + 1} failed for {image_url}: {e}")
                    attempt += 1
                    time.sleep(3)  # Wait for 3 seconds before retrying
                print(f"Failed to download image after {retries} attempts.")
                st.write(f"Failed to download image after {retries} attempts.")

            # Download images according to the new rules with swapped filenames
            for index, row in df.iterrows():
                lot_value = str(row["Lot"])
                quantity = row.get("Quantity", 1)  # Default to 1 if not specified
                first_image_downloaded = (
                    False  # Flag to mark the first image download for quantity text
                )

                # Auto-Image
                if not pd.isnull(row["Auto-Image"]):
                    download_image(
                        row["Auto-Image"],
                        f"./users/{username}/uploads/{lot_value}-1.jpg",
                        quantity if not first_image_downloaded else None,
                    )
                    first_image_downloaded = True

                # Image 12-21 (Swapped to use Online Image numbering: -2.jpg to -11.jpg)
                for i in range(
                    1, 11
                ):  # Adjusting for "Image" to use "Online Image" numbering
                    image_column = f"Image {i}"
                    if not pd.isnull(row[image_column]):
                        download_image(
                            row[image_column],
                            f"./users/{username}/uploads/{lot_value}-{i+11}.jpg",
                            quantity if not first_image_downloaded else None,
                        )
                        first_image_downloaded = True

                # Online Image 1-10 (Swapped to use Image numbering: -13.jpg to -22.jpg)
                for i in range(
                    1, 11
                ):  # Adjusting for "Online Image" to use "Image" numbering
                    online_image_column = f"Online Image {i}"
                    keep_image_column = f"Keep Image {i}?"
                    if row[keep_image_column] == True and not pd.isnull(
                        row[online_image_column]
                    ):
                        download_image(
                            row[online_image_column],
                            f"./users/{username}/uploads/{lot_value}-{i+1}.jpg",
                            quantity if not first_image_downloaded else None,
                        )
                        first_image_downloaded = True

            # Trim the dataframe to keep specified columns
            columns_to_keep = [
                "Lot",
                "Consignor",
                "Title",
                "Description 1",
                "Description 2",
                "Description 3",
                "Description 4",
                "Description 5",
                "Quantity",
                "Original Condition",
                "Start Bid",
                "MSRP CAD",
                "Shelf",
                "Slot",
                "Product Link",
            ]
            trimmed_df = df[columns_to_keep]

            # Save the trimmed dataframe to a new CSV
            trimmed_df.to_csv(
                f"./users/{username}/uploads/Items-To-List.csv", index=False
            )

            print("Script execution completed.")
            st.write("Script execution completed.")

            status.update(label="Step 3 Completed!", state="complete", expanded=False)
            return True
        except Exception as e:
            st.error(f"Something went wrong while running the step 3. Error: {e}")
            status.update(label="Step 3 Failed!", state="error", expanded=True)
            return False
