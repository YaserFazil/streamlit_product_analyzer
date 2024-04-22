import csv
import requests
import pandas as pd
import time
import os
import re
import streamlit as st


def start_step1(username):
    with st.status("Loading step 1 process...", expanded=True) as status:
        try:
            st.write("Loading database_export.csv and manifest.csv files...")
            time.sleep(3)
            # Load 'database_export.csv' and 'manifest.csv'
            df_export = pd.read_csv(f"./users/{username}/uploads/database_export.csv")
            manifest_df = pd.read_csv(
                f"./users/{username}/uploads/manifest.csv"
            ).drop_duplicates(subset=["LPN"])

            # Create a dictionary for LPN lookup from 'manifest.csv'
            manifest_dict = manifest_df.set_index("LPN")[
                ["Asin", "MSRP", "CurrencyCode"]
            ].to_dict("index")

            # Filter necessary columns from 'database_export.csv'
            df_filtered = df_export[["Lot", "Code", "Consignor", "Manual Entry"]]
            df_filtered.to_csv(f"./users/{username}/uploads/database.csv", index=False)

            df = pd.read_csv(f"./users/{username}/uploads/database.csv")

            # Convert columns to strings and remove spaces in 'Code'
            df["Code"] = df["Code"].astype(str).str.replace(" ", "", regex=False)
            df["Manual Entry"] = df["Manual Entry"].astype(str)

            # Determine code type, including handling for LPN
            def determine_code_type(row):
                code_upper = row["Code"].upper()
                if code_upper.startswith("LPN"):
                    return "LPN"
                elif len(code_upper) == 10 and code_upper.startswith("X"):
                    return "FBA"
                elif len(code_upper) == 10 and code_upper.startswith("B"):
                    return "ASIN"
                elif row["Manual Entry"] != "True":
                    return "UPC"
                else:
                    return "MANUAL-ENTRY"

            df["Code Type"] = df.apply(determine_code_type, axis=1)

            # Predefined headers for each CSV type
            csv_headers = {
                "LPN": [
                    "Lot",
                    "Consignor",
                    "Code",
                    "LPNFailed",
                    "ASIN",
                    "MSRP",
                    "CurrencyCode",
                ],
                "FBA": ["Lot", "Code", "Consignor"],
                "ASIN": ["Lot", "Code", "Consignor"],
                "UPC": ["Lot", "Code", "Consignor"],
                "MANUAL-ENTRY": ["Lot", "Code", "Consignor", "Manual Entry"],
            }

            # Process each code type for separate CSV output
            for code_type, headers in csv_headers.items():
                # Create an empty DataFrame with specified headers
                df_empty = pd.DataFrame(columns=headers)

                # Filter rows matching the current code type
                filtered_df = df[df["Code Type"] == code_type].copy()

                if code_type == "LPN" and not filtered_df.empty:
                    filtered_df["LPNFailed"] = filtered_df["Code"].apply(
                        lambda x: x not in manifest_dict
                    )
                    for lpn in filtered_df["Code"]:
                        if lpn in manifest_dict:
                            indices = filtered_df[filtered_df["Code"] == lpn].index
                            manifest_entry = manifest_dict[lpn]
                            for idx in indices:
                                filtered_df.at[idx, "ASIN"] = manifest_entry["Asin"]
                                msrp_value = pd.to_numeric(
                                    manifest_entry["MSRP"].replace("$", "").strip(),
                                    errors="coerce",
                                )
                                filtered_df.at[idx, "MSRP"] = msrp_value
                                filtered_df.at[idx, "CurrencyCode"] = manifest_entry[
                                    "CurrencyCode"
                                ]
                    filtered_df = filtered_df[headers]
                elif not filtered_df.empty:
                    if code_type in ["ASIN", "FBA"]:
                        filtered_df["Code"] = filtered_df["Code"].str.upper()
                    filtered_df = filtered_df[headers]
                else:
                    filtered_df = df_empty

                # Save the DataFrame to CSV
                filtered_df.to_csv(
                    f"./users/{username}/uploads/{code_type}_codes.csv", index=False
                )

            print("CSV files have been updated.")
            st.write("CSV files have been updated.")

            # Correctly placing your actual API key
            api_key = "e65bf319-4246-4413-842a-037da75ab5d8"

            headers = {
                "accept": "application/json",
                "Content-Type": "application/json",
                "api-key": api_key,
            }

            api_url = "https://ato.matterhr.com/api/v1/ScanTask/AddOrGet"

            # Function to convert code to ASIN with retry mechanism
            def convert_code_to_asin(code, attempt=1, max_attempts=2):
                print(f"Converting code: {code}")
                st.write(f"Converting code: {code}")
                payload = {"barCode": code}
                try:
                    response = requests.post(api_url, headers=headers, json=payload)
                    if response.status_code == 200:
                        asin = response.json().get("data", {}).get("asin")
                        if (
                            asin and asin != "None"
                        ):  # Check if ASIN is valid and not 'None'
                            print(f"Successfully converted {code} to ASIN: {asin}")
                            st.write(f"Successfully converted {code} to ASIN: {asin}")
                            return asin
                        else:
                            # Treat 'None' as a failed attempt and retry
                            print(
                                f"Received 'None' for {code}, treating as a failed attempt."
                            )
                            st.write(
                                f"Received 'None' for {code}, treating as a failed attempt."
                            )
                            raise ValueError("Received 'None' for ASIN")
                    else:
                        print(
                            f"Attempt {attempt} failed for {code}. Status code: {response.status_code}, Response: {response.text}"
                        )
                        st.write(
                            f"Attempt {attempt} failed for {code}. Status code: {response.status_code}, Response: {response.text}"
                        )
                        raise Exception(
                            f"API request failed with status code {response.status_code}"
                        )
                except Exception as e:
                    if attempt < max_attempts:
                        wait_time = 2**attempt  # Exponential backoff
                        print(
                            f"Retrying in {wait_time} seconds... (Attempt {attempt} of {max_attempts})"
                        )
                        st.write(
                            f"Retrying in {wait_time} seconds... (Attempt {attempt} of {max_attempts})"
                        )
                        time.sleep(wait_time)
                        return convert_code_to_asin(code, attempt + 1, max_attempts)
                    else:
                        print(
                            f"Failed to convert {code} after {max_attempts} attempts. Giving up."
                        )
                        st.write(
                            f"Failed to convert {code} after {max_attempts} attempts. Giving up."
                        )
                        return None

            # Read the CSV and prepare for conversion
            df = pd.read_csv(
                f"./users/{username}/uploads/FBA_codes.csv"
            )  # Make sure this matches your CSV file name

            # Initialize columns for ASIN and Conversion Status
            df["ASIN"] = None
            df["Conversion Status"] = False

            # Convert codes and track success
            for index, row in df.iterrows():
                asin = convert_code_to_asin(row["Code"])
                if asin:
                    df.at[index, "ASIN"] = asin
                    df.at[index, "Conversion Status"] = True

            # Split the DataFrame based on conversion success and save to separate CSV files
            success_df = df[df["Conversion Status"] == True].drop(
                columns=["Conversion Status"]
            )
            failure_df = df[df["Conversion Status"] == False].drop(
                columns=["ASIN", "Conversion Status"]
            )

            success_df.to_csv(
                f"./users/{username}/uploads/codes_converted_successfully.csv",
                index=False,
            )
            failure_df.to_csv(
                f"./users/{username}/uploads/codes_failed_to_convert.csv", index=False
            )

            print(
                "Conversion process completed. Check the output CSV files for details."
            )
            st.write(
                "Conversion process completed. Check the output CSV files for details."
            )

            # Load the CSV files
            asin_codes_df = pd.read_csv(f"./users/{username}/uploads/ASIN_codes.csv")
            codes_converted_successfully_df = pd.read_csv(
                f"./users/{username}/uploads/codes_converted_successfully.csv"
            )

            # Replace 'Code' with 'ASIN' in the 'codes_converted_successfully' DataFrame
            codes_converted_successfully_df["Code"] = codes_converted_successfully_df[
                "ASIN"
            ]

            # Now drop the 'ASIN' column as it's no longer needed
            codes_converted_successfully_df = codes_converted_successfully_df.drop(
                columns=["ASIN"]
            )

            # Concatenate both DataFrames
            combined_df = pd.concat([asin_codes_df, codes_converted_successfully_df])

            # Save the combined DataFrame to a new CSV, without the index
            combined_df.to_csv(f"./users/{username}/uploads/asins.csv", index=False)

            print("The files have been merged into asins.csv successfully.")
            st.write("The files have been merged into asins.csv successfully.")

            # File path for the CSV
            file_path = f"./users/{username}/uploads/asin_failed_LPN.csv"

            # Column headers in the required order
            headers = ["Lot", "Code", "Consignor"]

            # Writing to the CSV file
            with open(file_path, mode="w", newline="") as file:
                writer = csv.writer(file)
                # Write the header
                writer.writerow(headers)

            print(f"CSV file '{file_path}' created with headers only.")
            st.write(f"CSV file '{file_path}' created with headers only.")

            def fetch_asin_details(
                asin,
                api_key,
                initial_domains=[
                    "amazon.ca",
                    "amazon.com",
                    "amazon.co.uk",
                    "amazon.co.jp",
                ],
                retries=2,
                override_msrp=None,
                override_currency=None,
            ):
                print(f"Fetching details for ASIN: {asin}")
                st.write(f"Fetching details for ASIN: {asin}")
                final_data = {}
                domains_to_try = initial_domains.copy()

                while domains_to_try:
                    current_domain = domains_to_try.pop(0)
                    for attempt in range(1, retries + 1):
                        print(f"Attempt {attempt} for ASIN: {asin} on {current_domain}")
                        st.write(
                            f"Attempt {attempt} for ASIN: {asin} on {current_domain}"
                        )
                        params = {
                            "api_key": api_key,
                            "amazon_domain": current_domain,
                            "asin": asin,
                            "type": "product",
                            "output": "json",
                            "language": "en_US",
                        }
                        response = requests.get(
                            "https://api.asindataapi.com/request", params=params
                        )
                        if response.status_code == 200:
                            data = response.json().get("product", {})
                            final_data["title"] = data.get("title", "")
                            description = data.get("description", "") or data.get(
                                "feature_bullets_flat", ""
                            )
                            final_data["description"] = description

                            # Override MSRP and currency if provided
                            final_data["MSRP"] = (
                                override_msrp
                                if override_msrp is not None
                                else data.get("buybox_winner", {})
                                .get("price", {})
                                .get("value", "")
                            )
                            final_data["currency"] = (
                                override_currency
                                if override_currency is not None
                                else data.get("buybox_winner", {})
                                .get("price", {})
                                .get("currency", "")
                            )

                            final_data["image"] = data.get("main_image", {}).get(
                                "link", ""
                            )
                            final_data["link"] = data.get("link", "")

                            if final_data.get("link", ""):
                                print(
                                    f"Successfully fetched data for ASIN: {asin} on {current_domain}"
                                )
                                st.write(
                                    f"Successfully fetched data for ASIN: {asin} on {current_domain}"
                                )
                                return final_data, True
                        else:
                            print(
                                f"Failed to fetch details for ASIN: {asin} with status code: {response.status_code} on {current_domain}"
                            )
                            st.write(
                                f"Failed to fetch details for ASIN: {asin} with status code: {response.status_code} on {current_domain}"
                            )
                        time.sleep(2 ** (attempt - 1))

                print(
                    f"Returning partial data after exhausting all domains for ASIN: {asin}"
                )
                st.write(
                    f"Returning partial data after exhausting all domains for ASIN: {asin}"
                )
                return final_data, False

            def write_details_to_csv(filename, rows, api_key):
                print(f"Starting to write details to {filename}.")
                st.write(f"Starting to write details to {filename}.")
                failed_asins = []
                with open(filename, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(
                        [
                            "Lot",
                            "Code",
                            "Consignor",
                            "Title",
                            "Description",
                            "MSRP",
                            "Currency",
                            "Auto-Image",
                            "Product Link",
                        ]
                    )
                    for row in rows:
                        asin = row.get(
                            "ASIN", row.get("Code")
                        )  # Use 'ASIN' if present, otherwise fallback to 'Code'
                        override_msrp = row.get("MSRP")
                        override_currency = row.get("CurrencyCode")
                        product_details, success = fetch_asin_details(
                            asin,
                            api_key,
                            override_msrp=override_msrp,
                            override_currency=override_currency,
                        )
                        if success:
                            writer.writerow(
                                [
                                    row["Lot"],
                                    row.get(
                                        "Code", asin
                                    ),  # Use 'Code' if present, otherwise use 'ASIN'
                                    row["Consignor"],
                                    product_details.get("title", ""),
                                    product_details.get("description", ""),
                                    product_details.get("MSRP", ""),
                                    product_details.get("currency", ""),
                                    product_details.get("image", ""),
                                    product_details.get("link", ""),
                                ]
                            )
                        else:
                            failed_asins.append(row)
                print(f"Finished writing details to {filename}.")
                st.write(f"Finished writing details to {filename}.")
                return failed_asins

            def process_lpn_codes(filename, api_key):
                print("Processing LPN codes.")
                st.write("Processing LPN codes.")
                rows = []
                failed_asins = []
                with open(filename, mode="r", newline="", encoding="utf-8") as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        if row["LPNFailed"].lower() == "true":
                            failed_asins.append(row)  # Just append the entire row
                        else:
                            rows.append(row)

                failed_rows = write_details_to_csv(
                    f"./users/{username}/uploads/asin_details_LPN.csv", rows, api_key
                )
                failed_asins.extend(failed_rows)

                if failed_asins:
                    with open(
                        "asin_failed_LPN.csv", mode="w", newline="", encoding="utf-8"
                    ) as file:
                        # Collect all possible keys from failed_asins
                        fieldnames = list({key for d in failed_asins for key in d})
                        writer = csv.DictWriter(file, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(failed_asins)
                    print(
                        f"Failed ASIN details for LPN codes written to asin_failed_LPN.csv"
                    )
                    st.write(
                        f"Failed ASIN details for LPN codes written to asin_failed_LPN.csv"
                    )

            def main():
                input_filename = f"./users/{username}/uploads/asins.csv"
                lpn_filename = f"./users/{username}/uploads/LPN_codes.csv"
                api_key = "E3AC001C04104C3C934A08D204551A04"

                # Process ASINs from asins.csv
                print("Reading ASIN codes from asins.csv.")
                st.write("Reading ASIN codes from asins.csv.")
                rows = []
                with open(
                    input_filename, mode="r", newline="", encoding="utf-8"
                ) as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        rows.append(row)
                print(f"Total ASIN rows read from asins.csv: {len(rows)}")
                st.write(f"Total ASIN rows read from asins.csv: {len(rows)}")
                failed_asins = write_details_to_csv(
                    f"./users/{username}/uploads/asin_details.csv", rows, api_key
                )

                if failed_asins:
                    with open(
                        "asin_failed.csv", mode="w", newline="", encoding="utf-8"
                    ) as file:
                        # Adjust fieldnames accordingly
                        fieldnames = list({key for d in failed_asins for key in d})
                        writer = csv.DictWriter(file, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(failed_asins)
                    print(f"Failed ASIN details written to asin_failed.csv")
                    st.write(f"Failed ASIN details written to asin_failed.csv")

                # Process LPN codes
                process_lpn_codes(lpn_filename, api_key)

            main()

            def prepare_asin_failed_file():
                primary_file = f"./users/{username}/uploads/asin_failed.csv"
                if os.path.exists(primary_file):
                    import pandas as pd

                    df = pd.read_csv(primary_file)
                    df = df[["Lot", "Code", "Consignor"]]
                    df.to_csv(primary_file, index=False)
                else:
                    with open(primary_file, "w", newline="", encoding="utf-8") as file:
                        writer = csv.writer(file)
                        writer.writerow(["Lot", "Code", "Consignor"])

            def merge_csv_files(primary_file, secondary_file):
                # Check if the secondary file exists
                if not os.path.exists(secondary_file):
                    print(f"Skipping merge: {secondary_file} does not exist.")
                    st.write(f"Skipping merge: {secondary_file} does not exist.")
                    return

                # Determine if the primary file already has content (not empty)
                primary_exists = (
                    os.path.exists(primary_file) and os.path.getsize(primary_file) > 0
                )

                with open(
                    secondary_file, mode="r", newline="", encoding="utf-8"
                ) as file:
                    reader = csv.DictReader(file)
                    rows = [row for row in reader]

                with open(primary_file, mode="a", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    # Write headers only if primary file does not exist or is empty
                    if not primary_exists:
                        writer.writerow(reader.fieldnames)

                    if "failed" in primary_file:
                        # Write only selected columns for failed files
                        for row in rows:
                            writer.writerow([row["Lot"], row["Code"], row["Consignor"]])
                    else:
                        # Write all columns for other details files
                        for row in rows:
                            writer.writerow([row[col] for col in reader.fieldnames])

            def main():
                prepare_asin_failed_file()

                merge_csv_files("asin_details.csv", "asin_details_LPN.csv")
                merge_csv_files("asin_failed.csv", "asin_failed_LPN.csv")

                print("Files have been successfully merged.")
                st.write("Files have been successfully merged.")

            if __name__ == "__main__":
                main()

            # Load the CSV file
            df = pd.read_csv(f"./users/{username}/uploads/asin_failed_LPN.csv")

            # Select and reorder the columns
            df = df[["Lot", "Code", "Consignor"]]

            # Save the modified DataFrame back to the same CSV file, overwriting the original
            df.to_csv(f"./users/{username}/uploads/asin_failed_LPN.csv", index=False)

            print("CSV file has been updated and overwritten.")
            st.write("CSV file has been updated and overwritten.")

            # Replace 'your_api_key_here' with your actual BarcodeLookup API key
            api_key = "zhjgq69gsa1e2cwozbgqlf5xz1rdch"
            api_url = "https://api.barcodelookup.com/v3/products"

            print("Loading UPC codes from 'UPC_codes.csv'.")
            st.write("Loading UPC codes from 'UPC_codes.csv'.")
            upc_df = pd.read_csv(
                f"./users/{username}/uploads/UPC_codes.csv", dtype=str
            )  # Ensure codes are read as strings

            # These columns will be filled with the API data
            upc_df["Title"] = ""
            upc_df["Description"] = ""
            upc_df["MSRP"] = ""
            upc_df["Currency"] = ""
            upc_df["Auto-Image"] = ""
            upc_df["Product Link"] = ""

            # Data frame to keep track of failed UPC queries
            failed_codes_df = pd.DataFrame(columns=["Lot", "Code", "Consignor"])

            def query_api(upc_code):
                print(f"Querying API for UPC: {upc_code}")
                st.write(f"Querying API for UPC: {upc_code}")
                params = {"barcode": upc_code, "key": api_key, "formatted": "y"}

                attempt_count = 0  # Track the number of attempts
                while attempt_count < 2:  # Allow for the initial try and one retry
                    response = requests.get(api_url, params=params)
                    attempt_count += 1  # Increment the attempt counter

                    if response.status_code == 200:
                        data = response.json().get("products", [None])[0]
                        if data:
                            print(f"Data found for UPC: {upc_code}")
                            st.write(f"Data found for UPC: {upc_code}")
                            return {
                                "Title": data.get("title", ""),
                                "Description": data.get("description", ""),
                                "MSRP": (
                                    data["stores"][0].get("price", "")
                                    if data.get("stores")
                                    else ""
                                ),
                                "Currency": (
                                    data["stores"][0].get("currency", "")
                                    if data.get("stores")
                                    else ""
                                ),
                                "Auto-Image": (
                                    data["images"][0] if data.get("images") else ""
                                ),
                                "Product Link": (
                                    data["stores"][0].get("link", "")
                                    if data.get("stores")
                                    else ""
                                ),
                                "Success": True,
                            }
                        else:
                            print(f"No data found for UPC: {upc_code}")
                            st.write(f"No data found for UPC: {upc_code}")
                            return {"Success": False}
                    else:
                        print(
                            f"Attempt {attempt_count}: Failed to retrieve data for UPC {upc_code}. Status code: {response.status_code}"
                        )
                        st.write(
                            f"Attempt {attempt_count}: Failed to retrieve data for UPC {upc_code}. Status code: {response.status_code}"
                        )
                        if (
                            attempt_count < 2
                        ):  # If this was the first attempt, wait before retrying
                            print("Waiting for 3 seconds before retrying...")
                            st.write("Waiting for 3 seconds before retrying...")
                            time.sleep(3)  # Wait for 3 seconds before the next attempt

                # If both attempts failed
                return {"Success": False}

            # List to hold indices of failed UPCs
            failed_indices = []

            for index, row in upc_df.iterrows():
                result = query_api(row["Code"])
                if result["Success"]:
                    for key in result.keys() - {"Success"}:
                        upc_df.at[index, key] = result[key]
                else:
                    failed_indices.append(index)

            # Drop the rows that failed from upc_df and add them to failed_codes_df
            failed_codes_df = upc_df.loc[failed_indices]
            upc_df.drop(failed_indices, inplace=True)

            print("Saving details to 'upc_with_details.csv'.")
            st.write("Saving details to 'upc_with_details.csv'.")
            upc_df.to_csv(
                f"./users/{username}/uploads/upc_with_details.csv", index=False
            )

            print("Saving failed UPC codes to 'upc_failed_details.csv'.")
            st.write("Saving failed UPC codes to 'upc_failed_details.csv'.")
            failed_codes_df.to_csv(
                f"./users/{username}/uploads/upc_failed_details.csv",
                index=False,
                columns=["Lot", "Code", "Consignor"],
            )

            print(
                "Processing completed. Check 'upc_with_details.csv' and 'upc_failed_details.csv' for outputs."
            )
            st.write(
                "Processing completed. Check 'upc_with_details.csv' and 'upc_failed_details.csv' for outputs."
            )

            # Load the CSV files for successful ASIN and UPC details
            asin_details_df = pd.read_csv(
                f"./users/{username}/uploads/asin_details.csv", dtype=str
            )
            upc_details_df = pd.read_csv(
                f"./users/{username}/uploads/upc_with_details.csv", dtype=str
            )

            # Drop the 'Code' column from both DataFrames
            asin_details_df.drop(columns=["Code"], inplace=True)
            upc_details_df.drop(columns=["Code"], inplace=True)

            # Concatenate both DataFrames for successful items and mark them as 'Success'
            combined_df = pd.concat(
                [asin_details_df, upc_details_df], ignore_index=True
            )
            combined_df["Status"] = (
                "True"  # Add a 'Status' column indicating these are successful entries
            )
            combined_df["Manual Entry"] = (
                "False"  # Add a 'Manual Entry' column with default 'false'
            )

            # Load the 'Lot' and 'Consignor' columns from each failed CSV file
            codes_failed_df = pd.read_csv(
                f"./users/{username}/uploads/codes_failed_to_convert.csv",
                usecols=["Lot", "Consignor"],
                dtype=str,
            )
            upc_failed_df = pd.read_csv(
                f"./users/{username}/uploads/upc_failed_details.csv",
                usecols=["Lot", "Consignor"],
                dtype=str,
            )

            try:
                # Try to load the existing 'asin_failed.csv' file
                asin_failed_df = pd.read_csv(
                    f"./users/{username}/uploads/asin_failed.csv",
                    usecols=["Lot", "Consignor"],
                    dtype=str,
                )
            except FileNotFoundError:
                # If the file does not exist, initialize an empty DataFrame with specified columns
                print("'asin_failed.csv' not found. Creating an empty file.")
                st.write("'asin_failed.csv' not found. Creating an empty file.")
                asin_failed_df = pd.DataFrame(columns=["Lot", "Consignor"])
                # Save the empty DataFrame to 'asin_failed.csv'
                asin_failed_df.to_csv(
                    f"./users/{username}/uploads/asin_failed.csv", index=False
                )

            # Combine the 'Lot' and 'Consignor' columns from all failed dataframes, drop duplicates, and mark them as 'Failed'
            combined_lot_df = (
                pd.concat(
                    [codes_failed_df, upc_failed_df, asin_failed_df], ignore_index=True
                )
                .drop_duplicates(subset=["Lot"])
                .reset_index(drop=True)
            )
            combined_lot_df["Status"] = (
                "False"  # Add a 'Status' column indicating these are failed entries
            )
            combined_lot_df["Manual Entry"] = (
                "False"  # Indicate these are not manual entries
            )

            # Continue with the script to combine MANUAL-ENTRY lots and others as previously done

            # Define manual_entry_df by loading data from the MANUAL-ENTRY_codes.csv file
            manual_entry_df = pd.read_csv(
                f"./users/{username}/uploads/MANUAL-ENTRY_codes.csv",
                usecols=["Lot", "Consignor"],
                dtype=str,
            )
            manual_entry_df["Status"] = "True"  # Mark these entries as 'Success'
            manual_entry_df["Manual Entry"] = (
                "True"  # Indicate these are manual entries
            )

            # Now, it's safe to check for missing columns since manual_entry_df is defined
            missing_cols = [
                col
                for col in combined_df.columns
                if col not in manual_entry_df.columns
                and col != "Status"
                and col != "Manual Entry"
            ]
            for col in missing_cols:
                manual_entry_df[col] = ""  # Add empty data for missing columns

            # Append MANUAL-ENTRY lots to the combined DataFrame
            final_df = pd.concat(
                [combined_df, combined_lot_df, manual_entry_df], ignore_index=True
            )

            # Save the final DataFrame to a new CSV file, including both successful and failed items with status and manual entry indicated
            final_df.to_csv(f"./users/{username}/uploads/items.csv", index=False)

            print(
                "The files have been combined into 'items.csv' successfully, with status and manual entry indicated for each entry."
            )
            st.write(
                "The files have been combined into 'items.csv' successfully, with status and manual entry indicated for each entry."
            )

            # Load the CSV files into DataFrames
            df_export = pd.read_csv(f"./users/{username}/uploads/database_export.csv")
            df_items = pd.read_csv(f"./users/{username}/uploads/items.csv")

            # Ensure 'Manual Entry' is treated as a string and convert 'Lot' to string in both DataFrames
            df_export["Manual Entry"] = df_export["Manual Entry"].astype(str)
            df_export["Lot"] = df_export["Lot"].astype(str)
            df_items["Lot"] = df_items["Lot"].astype(str)

            # Filter to get only manual entry rows from database_export.csv where Manual Entry is "true"
            manual_entries = df_export[df_export["Manual Entry"].str.lower() == "true"]

            # Select necessary columns for the merge/update
            manual_entries = manual_entries[
                ["Lot", "Title", "Description", "MSRP", "Currency"]
            ]

            # Merge the DataFrames on 'Lot', updating the items DataFrame
            # This assumes 'Lot' is a unique identifier. If 'Lot' can have duplicates, additional logic may be needed
            updated_items = pd.merge(
                df_items, manual_entries, on="Lot", how="left", suffixes=("", "_update")
            )

            # For each of the fields, update the value in df_items if it's present in manual_entries
            for column in ["Title", "Description", "MSRP", "Currency"]:
                updated_items[column] = updated_items[f"{column}_update"].combine_first(
                    updated_items[column]
                )
                updated_items.drop(columns=[f"{column}_update"], inplace=True)

            # Overwrite items.csv with the updated DataFrame
            updated_items.to_csv(f"./users/{username}/uploads/items.csv", index=False)

            print(
                "items.csv has been updated with manual entry details from database_export.csv."
            )
            st.write(
                "items.csv has been updated with manual entry details from database_export.csv."
            )

            # Load the CSV files
            items_df = pd.read_csv(f"./users/{username}/uploads/items.csv")
            asin_codes_df = pd.read_csv(f"./users/{username}/uploads/ASIN_codes.csv")
            codes_converted_df = pd.read_csv(
                f"./users/{username}/uploads/codes_converted_successfully.csv"
            )
            codes_failed_df = pd.read_csv(
                f"./users/{username}/uploads/codes_failed_to_convert.csv"
            )
            upc_codes_df = pd.read_csv(f"./users/{username}/uploads/UPC_codes.csv")
            lpn_codes_df = pd.read_csv(f"./users/{username}/uploads/LPN_codes.csv")

            # Initialize new columns in items_df
            items_df["Code-FBA"] = ""
            items_df["Code-ASIN"] = ""
            items_df["Code-UPC"] = ""

            # Function to update codes for a lot
            def update_codes(lot, consignor, code_fba, code_asin, code_upc):
                condition = (items_df["Lot"] == lot) & (
                    items_df["Consignor"] == consignor
                )
                if code_fba:
                    items_df.loc[condition, "Code-FBA"] = code_fba
                if code_asin:
                    items_df.loc[condition, "Code-ASIN"] = code_asin
                if code_upc:
                    items_df.loc[condition, "Code-UPC"] = code_upc

            # Update items_df with ASIN codes
            for _, row in asin_codes_df.iterrows():
                update_codes(row["Lot"], row["Consignor"], None, row["Code"], None)

            # Update items_df with converted FBA to ASIN codes
            for _, row in codes_converted_df.iterrows():
                update_codes(
                    row["Lot"], row["Consignor"], row["Code"], row["ASIN"], None
                )

            # Update items_df with failed FBA codes
            for _, row in codes_failed_df.iterrows():
                update_codes(row["Lot"], row["Consignor"], row["Code"], None, None)

            # Update items_df with UPC codes
            for _, row in upc_codes_df.iterrows():
                update_codes(row["Lot"], row["Consignor"], None, None, row["Code"])

            # Update items_df based on LPN_codes.csv where LPNFailed is False
            for _, row in lpn_codes_df.iterrows():
                if not row["LPNFailed"]:  # Checks if LPNFailed is False
                    update_codes(row["Lot"], row["Consignor"], None, row["ASIN"], None)

            # Set empty values in the Currency column to "CAD"
            items_df["Currency"] = items_df["Currency"].fillna("CAD")

            # Save the updated DataFrame to a new CSV file
            items_df.to_csv(f"./users/{username}/uploads/items.csv", index=False)

            print("Updated items.csv has been saved as items_updated.csv")
            st.write("Updated items.csv has been saved as items_updated.csv")

            # Load the CSV file
            items_df = pd.read_csv(f"./users/{username}/uploads/items.csv")

            # Function to remove HTML/CSS tags
            def remove_html_css(text):
                if pd.isnull(text) or not isinstance(text, str):
                    return text  # Return the input as is if it's not a string
                text = re.sub(r"<[^>]+>", "", text)  # Remove HTML tags
                text = re.sub(
                    r"{[^}]+}", "", text
                )  # Remove inline CSS and any remaining HTML/CSS artifacts
                text = re.sub(r"&[^;]+;", " ", text)  # Replace HTML entities with space
                return text

            # Function to remove emojis
            def remove_emojis(text):
                if pd.isnull(text) or not isinstance(text, str):
                    return text
                emoji_pattern = re.compile(
                    "["
                    "\U0001F600-\U0001F64F"
                    "\U0001F300-\U0001F5FF"
                    "\U0001F680-\U0001F6FF"
                    "\U0001F700-\U0001F77F"
                    "\U0001F780-\U0001F7FF"
                    "\U0001F800-\U0001F8FF"
                    "\U0001F900-\U0001F9FF"
                    "\U0001FA00-\U0001FA6F"
                    "\U0001FA70-\U0001FAFF"
                    "\U00002702-\U000027B0"
                    "\U000024C2-\U0001F251"
                    "]+",
                    flags=re.UNICODE,
                )
                return emoji_pattern.sub(r"", text)

            # Function to clean text further
            def clean_text(text):
                if pd.isnull(text) or not isinstance(text, str):
                    return text
                text = re.sub(r"\r\n|\r|\n", " ", text)  # Remove line breaks
                text = re.sub(r"\.([A-Za-z])", r". \1", text)  # Add space after periods
                return text.strip()

            # Function to ensure text does not exceed 220 characters
            def trim_text(text):
                if pd.isnull(text) or not isinstance(text, str):
                    return text
                if len(text) > 440:
                    return text[:437] + "..."  # Trim and append ellipsis
                else:
                    return text

            # Applying all functions to clean and trim the Description, Title, and Notes columns
            items_df["Description"] = items_df["Description"].apply(
                lambda x: remove_html_css(x)
            )
            items_df["Description"] = items_df["Description"].apply(
                lambda x: remove_emojis(x)
            )
            items_df["Description"] = items_df["Description"].apply(
                lambda x: clean_text(x)
            )
            items_df["Description"] = items_df["Description"].apply(trim_text)

            # Apply the same cleaning and trimming process for Title and Notes fields
            items_df["Title"] = items_df["Title"].apply(lambda x: remove_html_css(x))
            items_df["Title"] = items_df["Title"].apply(lambda x: remove_emojis(x))
            items_df["Title"] = items_df["Title"].apply(lambda x: clean_text(x))
            items_df["Title"] = items_df["Title"].apply(trim_text)

            # Save the cleaned DataFrame to a new CSV file
            items_df.to_csv(f"./users/{username}/uploads/items.csv", index=False)

            print("Cleaned items.csv has been saved as items.csv")
            st.write("Cleaned items.csv has been saved as items.csv")

            # Load the CSV file
            items_df = pd.read_csv(f"./users/{username}/uploads/items.csv")

            # Add the 'Developer Settings' column with default value False
            # The location is set to be right after the 'Manual Entry' column
            location = items_df.columns.get_loc("Manual Entry") + 1
            items_df.insert(loc=location, column="Developer Settings", value=False)

            # Save the updated DataFrame back to CSV
            items_df.to_csv(f"./users/{username}/uploads/items.csv", index=False)

            print(
                "A new column 'Developer Settings' has been added and saved as items.csv"
            )
            st.write(
                "A new column 'Developer Settings' has been added and saved as items.csv"
            )

            # Load 'database_export.csv' but only the 'Lot' and 'Auto-Image' columns
            database_export_df = pd.read_csv(
                f"./users/{username}/uploads/database_export.csv",
                usecols=["Lot", "Auto-Image"],
                dtype=str,
            )

            # Load 'items.csv' entirely, since it contains other columns as well
            items_df = pd.read_csv(f"./users/{username}/uploads/items.csv", dtype=str)

            # Iterate over the items DataFrame to find rows where 'Auto-Image' is empty
            for index, row in items_df.iterrows():
                # Check if 'Auto-Image' is empty
                if pd.isna(row["Auto-Image"]) or row["Auto-Image"].strip() == "":
                    # Attempt to find a matching 'Lot' in database_export_df and get its 'Auto-Image' value
                    auto_image_value = database_export_df.loc[
                        database_export_df["Lot"] == row["Lot"], "Auto-Image"
                    ].values
                    # If a matching 'Lot' is found and it has a non-empty 'Auto-Image' value, update items_df
                    if (
                        len(auto_image_value) > 0
                        and pd.notna(auto_image_value[0])
                        and auto_image_value[0].strip() != ""
                    ):
                        items_df.at[index, "Auto-Image"] = auto_image_value[0]

            # Save the updated DataFrame back to 'items.csv' (or a new file if you prefer to keep the original intact)
            items_df.to_csv(f"./users/{username}/uploads/items.csv", index=False)

            print(
                "The 'items.csv' file has been updated with 'Auto-Image' values from 'database_export.csv'."
            )
            st.write(
                "The 'items.csv' file has been updated with 'Auto-Image' values from 'database_export.csv'."
            )

            # List of files to be deleted
            files_to_delete = [
                "ASIN_codes.csv",
                "asin_details.csv",
                "asins.csv",
                "asin_failed.csv",
                "codes_converted_successfully.csv",
                "codes_failed_to_convert.csv",
                "FBA_codes.csv",
                "UPC_codes.csv",
                "UPC_codes_trimmed.csv",
                "upc_failed_details.csv",
                "upc_with_details.csv",
                "database.csv",
                "MANUAL-ENTRY_codes.csv",
                "asin_details_LPN.csv",
                "asin_failed_LPN.csv",
                "LPN_codes.csv",
            ]

            # Deleting the specified files
            for file_name in files_to_delete:
                try:
                    os.remove(f"./users/{username}/uploads/{file_name}")
                    print(f"{file_name} has been deleted.")
                    st.write(f"{file_name} has been deleted.")
                except FileNotFoundError:
                    print(f"{file_name} not found, skipping.")
                    st.write(f"{file_name} not found, skipping.")

            print("Cleanup completed.")
            st.write("Cleanup completed.")
            status.update(label="Step 1 Completed!", state="complete", expanded=False)
            return True
        except Exception as e:
            st.error(f"Something went wrong while running the step 1. Error: {e}")
            status.update(label="Step 1 Failed!", state="error", expanded=True)
            return False
