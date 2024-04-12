import csv
import requests
import pandas as pd
import time
import os
import re
from flask_socketio import emit


def start_step1(sid, broadcast):
    try:
        # Load the original 'database_export.csv'
        df_export = pd.read_csv("database_export.csv")

        # Filter out the necessary columns (assuming 'Manual Entry' is also a necessary column)
        df_filtered = df_export[["Lot", "Code", "Consignor", "Manual Entry"]]

        # Save the filtered DataFrame as 'database.csv'
        df_filtered.to_csv("database.csv", index=False)
        print(
            "The 'database.csv' file has been created with only the necessary columns."
        )
        emit(
            "server",
            {
                "data": "The 'database.csv' file has been created with only the necessary columns."
            },
            to=sid,
            broadcast=broadcast,
        )

        # Load the modified CSV file
        df = pd.read_csv("database.csv")

        # Convert codes to strings
        df["Code"] = df["Code"].astype(str)

        # Before applying 'smart_trim_title', convert each Manual Entry Field to a string
        df["Manual Entry"] = df["Manual Entry"].astype(str)

        # Remove spaces from the 'Code' column entries
        df["Code"] = df["Code"].str.replace(" ", "", regex=False)

        # Define a function to determine the code type based on your criteria
        def determine_code_type(row):
            # Check if 'Code' is a string; if not, categorize as 'MANUAL-ENTRY'
            if not isinstance(row["Code"], str):
                return "MANUAL-ENTRY"  # Handle non-string or missing codes as manual entries

            code_upper = row[
                "Code"
            ].upper()  # Convert code to uppercase to simplify checks
            if len(row["Code"]) == 10 and code_upper.startswith("X"):
                return "FBA"
            elif len(row["Code"]) == 10 and code_upper.startswith("B"):
                return "ASIN"
            elif row["Manual Entry"] != "True":
                return "UPC"
            else:
                return "MANUAL-ENTRY"

        # Apply the function to each row to determine the code type
        df["Code Type"] = df.apply(determine_code_type, axis=1)

        # Loop through each code type and create a separate CSV file for it, excluding 'Code Type' column for all
        code_types = ["FBA", "UPC", "ASIN", "MANUAL-ENTRY"]
        for code_type in code_types:
            filtered_df = df[
                df["Code Type"] == code_type
            ].copy()  # Make a copy to avoid SettingWithCopyWarning
            # Check if the current code type needs capitalization
            if code_type in ["ASIN", "FBA"]:
                # Capitalize the 'Code' column
                filtered_df["Code"] = filtered_df["Code"].str.upper()
            # Exclude 'Code Type' column before saving
            filtered_df.drop(columns=["Code Type", "Manual Entry"], inplace=True)
            filtered_df.to_csv(f"{code_type}_codes.csv", index=False)

        print(
            "CSV files have been created for each code type, with 'ASIN' and 'FBA' codes capitalized and without 'Code Type' and 'Manual Entry' columns."
        )
        emit(
            "server",
            {
                "data": "CSV files have been created for each code type, with 'ASIN' and 'FBA' codes capitalized and without 'Code Type' and 'Manual Entry' columns."
            },
            to=sid,
            broadcast=broadcast,
        )

        # Correctly placing your actual API key
        api_key = "35ed928c-dd54-46ee-a6e6-2d42229cd095"

        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "api-key": api_key,
        }

        api_url = "https://ato.matterhr.com/api/v1/ScanTask/AddOrGet"

        # Function to convert code to ASIN with retry mechanism
        def convert_code_to_asin(code, attempt=1, max_attempts=2):
            print(f"Converting code: {code}")

            emit(
                "server",
                {"data": f"Converting code: {code}"},
                to=sid,
                broadcast=broadcast,
            )

            payload = {"barCode": code}
            try:
                response = requests.post(api_url, headers=headers, json=payload)
                if response.status_code == 200:
                    asin = response.json().get("data", {}).get("asin")
                    if asin and asin != "None":  # Check if ASIN is valid and not 'None'
                        print(f"Successfully converted {code} to ASIN: {asin}")
                        emit(
                            "server",
                            {"data": f"Successfully converted {code} to ASIN: {asin}"},
                            to=sid,
                            broadcast=broadcast,
                        )
                        return asin
                    else:
                        # Treat 'None' as a failed attempt and retry
                        print(
                            f"Received 'None' for {code}, treating as a failed attempt."
                        )
                        emit(
                            "server",
                            {
                                "data": f"Received 'None' for {code}, treating as a failed attempt."
                            },
                            to=sid,
                            broadcast=broadcast,
                        )
                        raise ValueError("Received 'None' for ASIN")
                else:
                    print(
                        f"Attempt {attempt} failed for {code}. Status code: {response.status_code}, Response: {response.text}"
                    )
                    emit(
                        "server",
                        {
                            "data": f"Attempt {attempt} failed for {code}. Status code: {response.status_code}, Response: {response.text}"
                        },
                        to=sid,
                        broadcast=broadcast,
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
                    emit(
                        "server",
                        {
                            "data": f"Retrying in {wait_time} seconds... (Attempt {attempt} of {max_attempts})"
                        },
                        to=sid,
                        broadcast=broadcast,
                    )
                    time.sleep(wait_time)
                    return convert_code_to_asin(code, attempt + 1, max_attempts)
                else:
                    print(
                        f"Failed to convert {code} after {max_attempts} attempts. Giving up."
                    )
                    emit(
                        "server",
                        {
                            "data": f"Failed to convert {code} after {max_attempts} attempts. Giving up."
                        },
                        to=sid,
                        broadcast=broadcast,
                    )
                    return None

        # Read the CSV and prepare for conversion
        df = pd.read_csv("FBA_codes.csv")  # Make sure this matches your CSV file name

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

        success_df.to_csv("codes_converted_successfully.csv", index=False)
        failure_df.to_csv("codes_failed_to_convert.csv", index=False)

        print("Conversion process completed. Check the output CSV files for details.")
        emit(
            "server",
            {
                "data": "Conversion process completed. Check the output CSV files for details."
            },
            to=sid,
            broadcast=broadcast,
        )

        # Load the CSV files
        asin_codes_df = pd.read_csv("ASIN_codes.csv")
        codes_converted_successfully_df = pd.read_csv(
            "codes_converted_successfully.csv"
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
        combined_df.to_csv("asins.csv", index=False)

        print("The files have been merged into asins.csv successfully.")
        emit(
            "server",
            {"data": "The files have been merged into asins.csv successfully."},
            to=sid,
            broadcast=broadcast,
        )

        def fetch_asin_details(
            asin,
            api_key,
            initial_domains=["amazon.ca", "amazon.com", "amazon.co.uk", "amazon.co.jp"],
            retries=2,
        ):
            """Fetch details for a given ASIN with retry logic, cycling through specified domains."""
            print(f"Fetching details for ASIN: {asin}")
            emit(
                "server",
                {"data": f"Fetching details for ASIN: {asin}"},
                to=sid,
                broadcast=broadcast,
            )
            final_data = {}
            domains_to_try = (
                initial_domains.copy()
            )  # Copy the list to avoid altering the original

            while domains_to_try:
                current_domain = domains_to_try.pop(0)  # Start with the first domain
                for attempt in range(1, retries + 1):
                    print(f"Attempt {attempt} for ASIN: {asin} on {current_domain}")
                    emit(
                        "server",
                        {
                            "data": f"Attempt {attempt} for ASIN: {asin} on {current_domain}"
                        },
                        to=sid,
                        broadcast=broadcast,
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

                        # Use feature_bullets_flat as fallback if description is not available
                        description = data.get("description", "") or data.get(
                            "feature_bullets_flat", ""
                        )
                        final_data["description"] = description

                        if data.get("buybox_winner", {}).get("price", {}):
                            final_data["MSRP"] = (
                                data.get("buybox_winner", {})
                                .get("price", {})
                                .get("value", "")
                            )
                            final_data["currency"] = (
                                data.get("buybox_winner", {})
                                .get("price", {})
                                .get("currency", "")
                            )

                        if data.get("main_image", {}):
                            final_data["image"] = data.get("main_image", {}).get(
                                "link", ""
                            )

                        final_data["link"] = data.get("link", "")

                        if final_data.get("link", ""):  # Success with a link
                            print(
                                f"Successfully fetched data for ASIN: {asin} on {current_domain}"
                            )
                            emit(
                                "server",
                                {
                                    "data": f"Successfully fetched data for ASIN: {asin} on {current_domain}"
                                },
                                to=sid,
                                broadcast=broadcast,
                            )
                            return final_data, True
                    else:
                        print(
                            f"Failed to fetch details for ASIN: {asin} with status code: {response.status_code} on {current_domain}"
                        )
                        emit(
                            "server",
                            {
                                "data": f"Failed to fetch details for ASIN: {asin} with status code: {response.status_code} on {current_domain}"
                            },
                            to=sid,
                            broadcast=broadcast,
                        )

                    time.sleep(2 ** (attempt - 1))  # Exponential backoff for each retry

                # If retries for the current domain are exhausted, move to the next domain automatically

            print(
                f"Returning partial data after exhausting all domains for ASIN: {asin}"
            )
            emit(
                "server",
                {
                    "data": f"Returning partial data after exhausting all domains for ASIN: {asin}"
                },
                to=sid,
                broadcast=broadcast,
            )
            return final_data, False

        def write_details_to_csv(filename, rows, api_key):
            """Write ASIN details to a CSV file, along with original row data, handling retries across domains."""
            print("Starting to write details to CSV.")
            emit(
                "server",
                {"data": "Starting to write details to CSV."},
                to=sid,
                broadcast=broadcast,
            )
            failed_asins = []
            with open(filename, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                # Define headers including the new data fields
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
                    asin = row["Code"]
                    product_details, success = fetch_asin_details(asin, api_key)
                    if success:
                        writer.writerow(
                            [
                                row["Lot"],
                                asin,
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
            print("Finished writing details to CSV.")
            emit(
                "server",
                {"data": "Finished writing details to CSV."},
                to=sid,
                broadcast=broadcast,
            )
            return failed_asins

        def main():
            input_filename = "asins.csv"
            output_filename = "asin_details.csv"
            failed_output_filename = "asin_failed.csv"
            api_key = "E3AC001C04104C3C934A08D204551A04"  # Replace 'your_api_key_here' with your actual API key

            print("Reading ASIN codes from CSV.")
            emit(
                "server",
                {"data": "Reading ASIN codes from CSV."},
                to=sid,
                broadcast=broadcast,
            )
            rows = []
            with open(input_filename, mode="r", newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    rows.append(row)
            print(f"Total rows read: {len(rows)}")
            emit(
                "server",
                {"data": f"Total rows read: {len(rows)}"},
                to=sid,
                broadcast=broadcast,
            )

            failed_asins = write_details_to_csv(output_filename, rows, api_key)

            # Write failed ASINs to a separate CSV
            if failed_asins:
                with open(
                    failed_output_filename, mode="w", newline="", encoding="utf-8"
                ) as file:
                    writer = csv.DictWriter(
                        file, fieldnames=["Lot", "Code", "Consignor"]
                    )
                    writer.writeheader()
                    writer.writerows(failed_asins)
                print(f"Failed ASIN details written to {failed_output_filename}")
                emit(
                    "server",
                    {
                        "data": f"Failed ASIN details written to {failed_output_filename}"
                    },
                    to=sid,
                    broadcast=broadcast,
                )

        if __name__ == "__main__":
            main()

        # Replace 'your_api_key_here' with your actual BarcodeLookup API key
        api_key = "zhjgq69gsa1e2cwozbgqlf5xz1rdch"
        api_url = "https://api.barcodelookup.com/v3/products"

        print("Loading UPC codes from 'UPC_codes.csv'.")
        emit(
            "server",
            {"data": "Loading UPC codes from 'UPC_codes.csv'."},
            to=sid,
            broadcast=broadcast,
        )
        upc_df = pd.read_csv(
            "UPC_codes.csv", dtype=str
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
            emit(
                "server",
                {"data": f"Querying API for UPC: {upc_code}"},
                to=sid,
                broadcast=broadcast,
            )
            params = {"barcode": upc_code, "key": api_key, "formatted": "y"}

            attempt_count = 0  # Track the number of attempts
            while attempt_count < 2:  # Allow for the initial try and one retry
                response = requests.get(api_url, params=params)
                attempt_count += 1  # Increment the attempt counter

                if response.status_code == 200:
                    data = response.json().get("products", [None])[0]
                    if data:
                        print(f"Data found for UPC: {upc_code}")
                        emit(
                            "server",
                            {"data": f"Data found for UPC: {upc_code}"},
                            to=sid,
                            broadcast=broadcast,
                        )
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
                        emit(
                            "server",
                            {"data": f"No data found for UPC: {upc_code}"},
                            to=sid,
                            broadcast=broadcast,
                        )
                        return {"Success": False}
                else:
                    print(
                        f"Attempt {attempt_count}: Failed to retrieve data for UPC {upc_code}. Status code: {response.status_code}"
                    )
                    emit(
                        "server",
                        {
                            "data": f"Attempt {attempt_count}: Failed to retrieve data for UPC {upc_code}. Status code: {response.status_code}"
                        },
                        to=sid,
                        broadcast=broadcast,
                    )
                    if (
                        attempt_count < 2
                    ):  # If this was the first attempt, wait before retrying
                        print("Waiting for 3 seconds before retrying...")
                        emit(
                            "server",
                            {"data": "Waiting for 3 seconds before retrying..."},
                            to=sid,
                            broadcast=broadcast,
                        )
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
        emit(
            "server",
            {"data": "Saving details to 'upc_with_details.csv'."},
            to=sid,
            broadcast=broadcast,
        )
        upc_df.to_csv("upc_with_details.csv", index=False)

        print("Saving failed UPC codes to 'upc_failed_details.csv'.")
        emit(
            "server",
            {"data": "Saving failed UPC codes to 'upc_failed_details.csv'."},
            to=sid,
            broadcast=broadcast,
        )
        failed_codes_df.to_csv(
            "upc_failed_details.csv", index=False, columns=["Lot", "Code", "Consignor"]
        )

        print(
            "Processing completed. Check 'upc_with_details.csv' and 'upc_failed_details.csv' for outputs."
        )
        emit(
            "server",
            {
                "data": "Processing completed. Check 'upc_with_details.csv' and 'upc_failed_details.csv' for outputs."
            },
            to=sid,
            broadcast=broadcast,
        )

        # Load the CSV files for successful ASIN and UPC details
        asin_details_df = pd.read_csv("asin_details.csv", dtype=str)
        upc_details_df = pd.read_csv("upc_with_details.csv", dtype=str)

        # Drop the 'Code' column from both DataFrames
        asin_details_df.drop(columns=["Code"], inplace=True)
        upc_details_df.drop(columns=["Code"], inplace=True)

        # Concatenate both DataFrames for successful items and mark them as 'Success'
        combined_df = pd.concat([asin_details_df, upc_details_df], ignore_index=True)
        combined_df["Status"] = (
            "True"  # Add a 'Status' column indicating these are successful entries
        )
        combined_df["Manual Entry"] = (
            "False"  # Add a 'Manual Entry' column with default 'false'
        )

        # Load the 'Lot' and 'Consignor' columns from each failed CSV file
        codes_failed_df = pd.read_csv(
            "codes_failed_to_convert.csv", usecols=["Lot", "Consignor"], dtype=str
        )
        upc_failed_df = pd.read_csv(
            "upc_failed_details.csv", usecols=["Lot", "Consignor"], dtype=str
        )

        try:
            # Try to load the existing 'asin_failed.csv' file
            asin_failed_df = pd.read_csv(
                "asin_failed.csv", usecols=["Lot", "Consignor"], dtype=str
            )
        except FileNotFoundError:
            # If the file does not exist, initialize an empty DataFrame with specified columns
            print("'asin_failed.csv' not found. Creating an empty file.")
            emit(
                "server",
                {"data": "'asin_failed.csv' not found. Creating an empty file."},
                to=sid,
                broadcast=broadcast,
            )
            asin_failed_df = pd.DataFrame(columns=["Lot", "Consignor"])
            # Save the empty DataFrame to 'asin_failed.csv'
            asin_failed_df.to_csv("asin_failed.csv", index=False)

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
            "MANUAL-ENTRY_codes.csv", usecols=["Lot", "Consignor"], dtype=str
        )
        manual_entry_df["Status"] = "True"  # Mark these entries as 'Success'
        manual_entry_df["Manual Entry"] = "True"  # Indicate these are manual entries

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
        final_df.to_csv("items.csv", index=False)

        print(
            "The files have been combined into 'items.csv' successfully, with status and manual entry indicated for each entry."
        )
        emit(
            "server",
            {
                "data": "The files have been combined into 'items.csv' successfully, with status and manual entry indicated for each entry."
            },
            to=sid,
            broadcast=broadcast,
        )

        # Load the CSV files into DataFrames
        df_export = pd.read_csv("database_export.csv")
        df_items = pd.read_csv("items.csv")

        # Ensure 'Manual Entry' is treated as a string
        df_export["Manual Entry"] = df_export["Manual Entry"].astype(str)

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
        updated_items.to_csv("items.csv", index=False)

        print(
            "items.csv has been updated with manual entry details from database_export.csv."
        )
        emit(
            "server",
            {
                "data": "items.csv has been updated with manual entry details from database_export.csv."
            },
            to=sid,
            broadcast=broadcast,
        )

        # Load the CSV files
        items_df = pd.read_csv("items.csv")
        asin_codes_df = pd.read_csv("ASIN_codes.csv")
        codes_converted_df = pd.read_csv("codes_converted_successfully.csv")
        codes_failed_df = pd.read_csv("codes_failed_to_convert.csv")
        upc_codes_df = pd.read_csv("UPC_codes.csv")

        # Initialize new columns in items_df
        items_df["Code-FBA"] = ""
        items_df["Code-ASIN"] = ""
        items_df["Code-UPC"] = ""

        # Function to update codes for a lot
        def update_codes(lot, consignor, code_fba, code_asin, code_upc):
            condition = (items_df["Lot"] == lot) & (items_df["Consignor"] == consignor)
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
            update_codes(row["Lot"], row["Consignor"], row["Code"], row["ASIN"], None)

        # Update items_df with failed FBA codes
        for _, row in codes_failed_df.iterrows():
            update_codes(row["Lot"], row["Consignor"], row["Code"], None, None)

        # Update items_df with UPC codes
        for _, row in upc_codes_df.iterrows():
            update_codes(row["Lot"], row["Consignor"], None, None, row["Code"])

        # Set empty values in the Currency column to "CAD"
        items_df["Currency"] = items_df["Currency"].fillna("CAD")

        # Save the updated DataFrame to a new CSV file
        items_df.to_csv("items.csv", index=False)

        print("Updated items.csv has been saved as items.csv")
        emit(
            "server",
            {"data": "Updated items.csv has been saved as items.csv"},
            to=sid,
            broadcast=broadcast,
        )

        # Load the CSV file
        items_df = pd.read_csv("items.csv")

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
        items_df["Description"] = items_df["Description"].apply(lambda x: clean_text(x))
        items_df["Description"] = items_df["Description"].apply(trim_text)

        # Apply the same cleaning and trimming process for Title and Notes fields
        items_df["Title"] = items_df["Title"].apply(lambda x: remove_html_css(x))
        items_df["Title"] = items_df["Title"].apply(lambda x: remove_emojis(x))
        items_df["Title"] = items_df["Title"].apply(lambda x: clean_text(x))
        items_df["Title"] = items_df["Title"].apply(trim_text)

        # Save the cleaned DataFrame to a new CSV file
        items_df.to_csv("items.csv", index=False)

        print("Cleaned items.csv has been saved as items.csv")
        emit(
            "server",
            {"data": "Cleaned items.csv has been saved as items.csv"},
            to=sid,
            broadcast=broadcast,
        )

        # Load the CSV file
        items_df = pd.read_csv("items.csv")

        # Add the 'Developer Settings' column with default value False
        # The location is set to be right after the 'Manual Entry' column
        location = items_df.columns.get_loc("Manual Entry") + 1
        items_df.insert(loc=location, column="Developer Settings", value=False)

        # Save the updated DataFrame back to CSV
        items_df.to_csv("items.csv", index=False)

        print("A new column 'Developer Settings' has been added and saved as items.csv")
        emit(
            "server",
            {
                "data": "A new column 'Developer Settings' has been added and saved as items.csv"
            },
            to=sid,
            broadcast=broadcast,
        )

        # Load 'database_export.csv' but only the 'Lot' and 'Auto-Image' columns
        database_export_df = pd.read_csv(
            "database_export.csv", usecols=["Lot", "Auto-Image"], dtype=str
        )

        # Load 'items.csv' entirely, since it contains other columns as well
        items_df = pd.read_csv("items.csv", dtype=str)

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
        items_df.to_csv("items.csv", index=False)

        print(
            "The 'items.csv' file has been updated with 'Auto-Image' values from 'database_export.csv'."
        )
        emit(
            "server",
            {
                "data": "The 'items.csv' file has been updated with 'Auto-Image' values from 'database_export.csv'."
            },
            to=sid,
            broadcast=broadcast,
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
        ]

        # Deleting the specified files
        for file_name in files_to_delete:
            try:
                os.remove(file_name)
                print(f"{file_name} has been deleted.")
                emit(
                    "server",
                    {"data": f"{file_name} has been deleted."},
                    to=sid,
                    broadcast=broadcast,
                )
            except FileNotFoundError:
                print(f"{file_name} not found, skipping.")
                emit(
                    "server",
                    {"data": f"{file_name} not found, skipping."},
                    to=sid,
                    broadcast=broadcast,
                )

        print("Cleanup completed.")
        emit(
            "server",
            {"data": "Cleanup completed."},
            to=sid,
            broadcast=broadcast,
        )

        return True
    except Exception as e:
        print("Something went wrong, error: ", e)
        emit(
            "server",
            {"data": f"Something went wrong, error: {e}"},
            to=sid,
            broadcast=broadcast,
        )
        return False
