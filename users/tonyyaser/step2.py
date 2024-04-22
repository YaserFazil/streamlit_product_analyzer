import pandas as pd
import requests
import os
import time
import streamlit as st


def start_step2(username):
    with st.status("Loading step 2 process...", expanded=True) as status:
        try:
            st.write("Loading items.csv and database_export.csv files...")
            time.sleep(3)
            # Load the CSV files
            items_df = pd.read_csv(f"./users/{username}/uploads/items.csv")
            database_export_df = pd.read_csv(
                f"./users/{username}/uploads/database_export.csv"
            )

            # Specify the columns to move, including 'Lot' for matching
            columns_to_move = [
                "Condition",
                "Shelf",
                "Slot",
                "Quantity",
                "Start Bid",
                "Lot",
                "Notes",
                "Manual Images",
                "Image 1",
                "Image 2",
                "Image 3",
                "Image 4",
                "Image 5",
                "Image 6",
                "Image 7",
                "Image 8",
                "Image 9",
                "Image 10",
                "Grabbed Online Images",
                "Online Image 1",
                "Keep Image 1?",
                "Online Image 2",
                "Keep Image 2?",
                "Online Image 3",
                "Keep Image 3?",
                "Online Image 4",
                "Keep Image 4?",
                "Online Image 5",
                "Keep Image 5?",
                "Online Image 6",
                "Keep Image 6?",
                "Online Image 7",
                "Keep Image 7?",
                "Online Image 8",
                "Keep Image 8?",
                "Online Image 9",
                "Keep Image 9?",
                "Online Image 10",
                "Keep Image 10?",
            ]

            # Create a subset of database_export_df with the specified columns
            database_export_subset = database_export_df[columns_to_move]

            # Merge this subset with items_df based on the 'Lot' column
            merged_df = pd.merge(items_df, database_export_subset, on="Lot", how="left")

            # Save the merged DataFrame to a new CSV file
            merged_df.to_csv(f"./users/{username}/uploads/items.csv", index=False)

            print("Updated items.csv has been saved as updated_items.csv")
            st.write("Updated items.csv has been saved as updated_items.csv")

            # Optionally delete the original 'database_export.csv'
            os.remove(f"./users/{username}/uploads/database_export.csv")

            # Commented out the deletion message since it might be better to manually verify before deleting.
            print("'database_export.csv' has been deleted.")
            st.write("'database_export.csv' has been deleted.")

            # Replace with your actual ExchangeRate-API key
            currency_api_key = "a18fb76d10f60072e22f0064"
            currency_api_url = "https://v6.exchangerate-api.com/v6/{}/latest/".format(
                currency_api_key
            )

            # Load the CSV file
            df = pd.read_csv(
                f"./users/{username}/uploads/items.csv",
                dtype={"MSRP": float, "Currency": str},
            )  # Ensure the file name is correct

            # Function to fetch conversion rates
            def get_conversion_rate(from_currency):
                response = requests.get(f"{currency_api_url}{from_currency}")
                if response.status_code == 200:
                    data = response.json()
                    if data["result"] == "success":
                        # Assuming we want to convert to CAD
                        rate_to_cad = data["conversion_rates"]["CAD"]
                        return rate_to_cad
                return None

            # Function to convert MSRP to CAD
            def convert_to_cad(row):
                if row["Currency"] == "CAD":
                    return row["MSRP"]  # No conversion needed if it's already in CAD
                rate = get_conversion_rate(row["Currency"])
                if rate is not None:
                    return round(row["MSRP"] * rate, 2)
                else:
                    print(f"Could not get conversion rate for {row['Currency']}.")
                    st.write(f"Could not get conversion rate for {row['Currency']}.")
                    return None

            # Apply the conversion to each row and create a new column for MSRP in CAD
            df["MSRP CAD"] = df.apply(convert_to_cad, axis=1)

            # Drop the original MSRP and Currency columns
            df.drop(columns=["MSRP", "Currency"], inplace=True)

            # Reorder the DataFrame columns
            column_order = [
                "Lot",
                "Consignor",
                "Condition",
                "Title",
                "Description",
                "Notes",
                "Quantity",
                "Start Bid",
                "MSRP CAD",
                "Shelf",
                "Slot",
                "Auto-Image",
                "Manual Images",
                "Image 1",
                "Image 2",
                "Image 3",
                "Image 4",
                "Image 5",
                "Image 6",
                "Image 7",
                "Image 8",
                "Image 9",
                "Image 10",
                "Grabbed Online Images",
                "Online Image 1",
                "Keep Image 1?",
                "Online Image 2",
                "Keep Image 2?",
                "Online Image 3",
                "Keep Image 3?",
                "Online Image 4",
                "Keep Image 4?",
                "Online Image 5",
                "Keep Image 5?",
                "Online Image 6",
                "Keep Image 6?",
                "Online Image 7",
                "Keep Image 7?",
                "Online Image 8",
                "Keep Image 8?",
                "Online Image 9",
                "Keep Image 9?",
                "Online Image 10",
                "Keep Image 10?",
                "Product Link",
                "Status",
                "Manual Entry",
                "Developer Settings",
                "Code-FBA",
                "Code-ASIN",
                "Code-UPC",
            ]
            df = df[column_order]

            # Save the updated DataFrame directly to 'items.csv', overwriting the existing file
            items_csv = df.to_csv(f"./users/{username}/uploads/items.csv", index=False)
            full_data_export_csv = df.to_csv(
                f"./users/{username}/uploads/full_data_export.csv", index=False
            )

            print(
                "Conversion and reordering completed. The 'items.csv' file has been updated."
            )
            st.write(
                "Conversion and reordering completed. The 'items.csv' file has been updated."
            )
            status.update(label="Step 2 Completed!", state="complete", expanded=False)
            return True
        except Exception as e:
            st.error(f"Something went wrong while running the step 2. Error: {e}")
            status.update(label="Step 2 Failed!", state="error", expanded=True)
            return False
