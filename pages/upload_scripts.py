import streamlit as st
import pandas as pd
import importlib
import os
import shutil
import zipfile
import io
from dynamodb_data import UserNew


def zip_folder(folder_path):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                zipf.write(
                    os.path.join(root, file),
                    os.path.relpath(os.path.join(root, file), folder_path),
                )
    zip_buffer.seek(0)
    return zip_buffer


def get_profile_dataset(pd_output=True):
    response = UserNew.scan()  # Retrieve all items from the DynamoDB table
    if pd_output == False:
        return response
    return pd.DataFrame(response)


def hide_sidebar():
    st.markdown(
        """
    <style>
        section[data-testid="stSidebar"][aria-expanded="true"]{
            display: none;
        }
    </style>
    """,
        unsafe_allow_html=True,
    )


def create_folders_for_user(username):
    uploads_folder = f"././users/{username}/uploads"
    scripts_folder = f"././users/{username}/scripts"
    if not os.path.exists(uploads_folder):
        os.makedirs(uploads_folder)
        print(f"Folder '{uploads_folder}' created successfully.")

    if not os.path.exists(scripts_folder):
        os.makedirs(scripts_folder)
        print(f"Folder '{scripts_folder}' created successfully.")


# Function to check for files and folders in the user folder
def check_for_contents_old(username):
    files_and_folders = {}

    user_folder = f"././users/{username}"
    if os.path.exists(user_folder):
        for root, dirs, files in os.walk(user_folder):
            folder_name = root.split("/")[-1]
            files_and_folders[folder_name] = {"files": files, "folders": dirs}

    return files_and_folders


# Function to check for files and folders in the user folder
def check_for_contents(username):
    files_and_folders = {}

    user_folder = f"././users/{username}"
    if os.path.exists(user_folder):
        for root, dirs, files in os.walk(user_folder):
            folder_name = root.split("/")[-1]
            # Exclude __pycache__ folders
            if "__pycache__" in dirs:
                dirs.remove("__pycache__")
            files_and_folders[folder_name] = {"files": files, "folders": dirs}

    return files_and_folders


def delete_file_or_directory(path):
    path = "././users/" + path
    print("Here is the path: ", path)
    try:
        if os.path.isfile(path):
            os.remove(path)
            print(f"File '{path}' deleted successfully.")
            st.success(f"File '{path}' deleted successfully.")
        elif os.path.isdir(path):
            os.rmdir(path)
            print(f"Directory '{path}' deleted successfully.")
            st.success(f"Directory '{path}' deleted successfully.")
        else:
            print(f"No such file or directory: '{path}'")
            st.warning(f"No such file or directory: '{path}'")
    except Exception as e:
        print(f"An error occurred: {e}")
        st.error(f"An error occurred: {e}")


def protected_page():
    if (
        "is_logged_in" in st.session_state
        and st.session_state["is_logged_in"] == True
        and "user_role" in st.session_state
        and st.session_state["user_role"] == "admin"
    ):
        user_role = st.session_state["user_role"]
        username = st.session_state["username"]
        st.title("Manage Py Scripts")
        if user_role == "admin":
            usernames = []
            for user in list(get_profile_dataset(pd_output=False)):
                usernames.append(user.username)
            username = st.selectbox(
                label="Select User",
                help="Select the user to upload or check his scripts",
                options=usernames,
            )
            create_folders_for_user(username)
            st.divider()
            # Delete file or folder field
            delete_file_or_dir = st.text_input(
                label=f"Delete file or folder in {username} folder",
                placeholder=f"E. g. step1.py or scripts/custom_script.py etc.",
                help=f"Enter path for the file or the folder you wanna delete in {username} folder.",
            )
            if delete_file_or_dir:
                delete_file_or_directory(username + "/" + delete_file_or_dir)
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                steps_uploaded = st.file_uploader(
                    label=f"Upload your Python step/s to '{username}' user",
                    help=f"Upload Py step/s to {username} folder",
                    accept_multiple_files=True,
                    type="py",
                )
                show_step_instr = st.button(label="Show Step Template")
                if steps_uploaded is not None and len(steps_uploaded) > 0:
                    for uploaded_step in steps_uploaded:
                        # Save the uploaded step to the specific user folder
                        file_path = os.path.join(
                            f"././users/{username}", uploaded_step.name
                        )
                        with open(file_path, "wb") as f:
                            f.write(uploaded_step.getbuffer())
            with col2:
                custom_scripts_uploaded = st.file_uploader(
                    label=f"Upload your custom Python script/s to '{username}' user",
                    help=f"Upload custom Py script/s to {username}/scripts folder",
                    accept_multiple_files=True,
                    type="py",
                )
                show_cus_script_instr = st.button(label="Show Custom Script Template")
                if (
                    custom_scripts_uploaded is not None
                    and len(custom_scripts_uploaded) > 0
                ):
                    for uploaded_script in custom_scripts_uploaded:
                        # Save the uploaded custom py script to user's scripts folder
                        file_path = os.path.join(
                            f"././users/{username}/scripts", uploaded_script.name
                        )
                        with open(file_path, "wb") as f:
                            f.write(uploaded_script.getbuffer())

            if show_step_instr:
                with open("././steps_template.py", "r+t") as code:
                    st.code(code.read(), language="python")

            if show_cus_script_instr:
                with open("././custom_scripts_template.py", "r+t") as code:
                    st.code(code.read(), language="python")

            st.divider()
            # Display the new files and provide download buttons
            user_folder = f"././users/{username}"
            # Zip the folder
            zip_buffer = zip_folder(user_folder)
            # Provide a download button for the zip file
            st.download_button(
                label=f"Download {username} Zip",
                data=zip_buffer.getvalue(),
                file_name=f"{username}.zip",
                mime="application/zip",
            )

            # Check for new files every time the app runs
            files_and_folders_dict = check_for_contents(username)

            st.write("Files and folders in user's directory:")
            for folder, contents in files_and_folders_dict.items():
                if contents["files"] or contents["folders"]:
                    st.write(f"📁 {folder}:")
                    if contents["files"]:
                        st.write("Files:")
                    for file in contents["files"]:
                        st.write(f"   - {file}")
                    if contents["folders"]:
                        st.write("Subfolders:")
                    for subfolder in contents["folders"]:
                        st.write(f"📁 {subfolder}")
                    st.divider()

    else:
        hide_sidebar()
        st.session_state["is_logged_in"] = False
        st.error("You're not logged in or you're not authorized to access this page!")
        st.page_link(page="login.py", label="Click here to login")


protected_page()
