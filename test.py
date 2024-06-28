import streamlit as st
import logging
import pandas as pd
import os


def start_script_Database_Export_Merging_guide():
    st.write("__Database CSV File Merging Tool Instructions:__")
    st.video("https://www.youtube.com/watch?v=LN_8CsLRmsc")
    st.write(
        "- Upload Database Export csv file/s and Raw Invoices Export csv file to get one merged INVOICES_EXPORT_PICKING.csv file"
    )


def start_script_Database_Export_Merging(username):
    logger = logging.getLogger("logger")
    logger.setLevel(logging.DEBUG)
    log_file_path = f"././users/{username}/logs/logs.log"
    logging.basicConfig(
        filename=log_file_path,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    with st.status(
        "Loading Database CSV File Merging Tool...", expanded=True
    ) as status:
        try:
            uploads_path = f"././users/{username}/uploads/"
            logger.info("Starting Database CSV File Merging Tool...")

            def identify_database_export_files(directory):
                """Identify DATABASE_EXPORT files based on their columns."""
                database_export_files = []
                for filename in os.listdir(directory):
                    if filename.endswith(".csv"):
                        df = read_csv_with_encoding(os.path.join(directory, filename))
                        if all(
                            column in df.columns for column in ["Lot", "Shelf", "Slot"]
                        ):
                            database_export_files.append(
                                os.path.join(directory, filename)
                            )
                return database_export_files

            def identify_raw_invoices_export_files(directory):
                """Identify RAW INVOICES_EXPORT files based on their columns."""
                raw_invoices_export_files = []
                for filename in os.listdir(directory):
                    if filename.endswith(".csv"):
                        df = read_csv_with_encoding(os.path.join(directory, filename))
                        if all(
                            column in df.columns
                            for column in [
                                "invoicenum",
                                "fname",
                                "lname",
                                "clotnum",
                                "lead",
                            ]
                        ) or all(
                            column in df.columns
                            for column in [
                                "Invoice",
                                "First Name",
                                "Last Name",
                                "Lot",
                                "Title",
                            ]
                        ):
                            raw_invoices_export_files.append(
                                os.path.join(directory, filename)
                            )
                return raw_invoices_export_files

            def read_csv_with_encoding(file_path):
                """Read CSV with different encodings."""
                encodings = ["utf-8", "ISO-8859-1", "latin1"]
                for encoding in encodings:
                    try:
                        return pd.read_csv(file_path, encoding=encoding)
                    except UnicodeDecodeError:
                        continue
                raise UnicodeDecodeError(
                    f"Cannot decode file {file_path} with any of the tested encodings."
                )

            def merge_files(data_files_directory, output_path):
                """Merge raw invoices with database exports and save the result."""
                # Identify RAW INVOICES_EXPORT files
                raw_invoices_export_files = identify_raw_invoices_export_files(
                    data_files_directory
                )

                if not raw_invoices_export_files:
                    raise ValueError("No RAW INVOICES_EXPORT files found.")

                # Load and concatenate all RAW INVOICES_EXPORT files
                raw_invoices = [
                    read_csv_with_encoding(file) for file in raw_invoices_export_files
                ]
                raw_invoices_combined = pd.concat(raw_invoices, ignore_index=True)

                # Rename columns to match expected format
                raw_invoices_combined = raw_invoices_combined.rename(
                    columns={
                        "invoicenum": "Invoice",
                        "fname": "First Name",
                        "lname": "Last Name",
                        "clotnum": "Lot",
                        "lead": "Title",
                        # Include the new names themselves in case they already exist
                        "Invoice": "Invoice",
                        "First Name": "First Name",
                        "Last Name": "Last Name",
                        "Lot": "Lot",
                        "Title": "Title",
                    }
                )

                # Ensure 'Lot' column is of type str
                raw_invoices_combined["Lot"] = raw_invoices_combined["Lot"].astype(str)

                # Identify DATABASE_EXPORT files
                database_export_files = identify_database_export_files(
                    data_files_directory
                )

                if not database_export_files:
                    raise ValueError("No DATABASE_EXPORT files found.")

                # Load and concatenate all DATABASE_EXPORT files
                db_exports = []
                duplicate_info = []
                for file in database_export_files:
                    df = read_csv_with_encoding(file)
                    df["source_file"] = file  # Add the filename as a column
                    duplicates = df[df.duplicated("Lot", keep=False)]
                    if not duplicates.empty:
                        duplicate_info.append(duplicates)
                    db_exports.append(df)

                if duplicate_info:
                    conflict_details = pd.concat(duplicate_info, ignore_index=True)
                    raise ValueError(
                        f"Conflicting lot values found in DATABASE_EXPORT files:\n{conflict_details}"
                    )

                db_exports_combined = pd.concat(db_exports, ignore_index=True)

                # Ensure 'Lot' column is of type str
                db_exports_combined["Lot"] = db_exports_combined["Lot"].astype(str)

                # Trim RAW INVOICES_EXPORT to match INVOICES_EXPORT format
                invoices_export = raw_invoices_combined[
                    ["Invoice", "First Name", "Last Name", "Lot", "Title"]
                ]

                # Merge INVOICES_EXPORT with DATABASE_EXPORT on 'Lot'
                merged_df = pd.merge(
                    invoices_export, db_exports_combined, on="Lot", how="left"
                )

                # Save the merged dataframe to INVOICES_EXPORT_PICKING.CSV
                merged_df.to_csv(output_path, index=False)

            # Execute merge
            merge_files(uploads_path, uploads_path + "INVOICES_EXPORT_PICKING.CSV")

            logger.info("Script execution completed.")
            st.write("Script execution completed.")
            st.success("Database CSV File Merging Tool Completed!")
            status.update(
                label="Database CSV File Merging Tool Completed!",
                state="complete",
                expanded=False,
            )
            return True
        except Exception as e:
            st.error(
                f"Error occured while running Database CSV File Merging Tool! Error: {e}"
            )
            logger.error(
                f"Error occured while running Database CSV File Merging Tool! Error: {e}"
            )
            status.update(
                label="Database CSV File Merging Tool Failed!",
                state="error",
                expanded=True,
            )
            return False
