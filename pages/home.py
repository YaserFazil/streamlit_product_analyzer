import streamlit as st
import pandas as pd
import importlib
import os
from dynamodb_data import UserNew


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


# Count Python Scripts/Steps
def count_python_scripts(folder_path):
    # Initialize the count
    count = 0

    # Iterate over all files in the folder
    for file_name in os.listdir(folder_path):
        # Check if the file has a .py extension
        if file_name.endswith(".py"):
            count += 1

    return count


def import_user_steps(username):
    folder_path = f"./users/{username}"
    num_steps = count_python_scripts(folder_path)
    step_functions = {}
    for step_number in range(1, num_steps + 1):
        # Construct the module path dynamically based on username and step number
        module_path = f"users.{username}.step{step_number}"

        # Import the module dynamically
        try:
            module = importlib.import_module(module_path)
            # Now you can access the desired function or attribute
            step_functions[f"start_step{step_number}"] = getattr(
                module, f"start_step{step_number}"
            )
        except ImportError:
            step_functions[f"start_step{step_number}"] = None  # Module doesn't exist
    return step_functions


def get_profile_dataset(pd_output=True):
    response = UserNew.scan()  # Retrieve all items from the DynamoDB table
    if pd_output == False:
        return response
    return pd.DataFrame(response)


# Function to check for new files in a folder
def check_for_new_files(username):
    new_files = set(os.listdir(f"././users/{username}/outputs"))
    return new_files


def protected_page():
    if "is_logged_in" in st.session_state and st.session_state["is_logged_in"] == True:
        user_role = st.session_state["user_role"]
        username = st.session_state["username"]
        st.title("Home Page")
        st.write("Welcome to the main page of this tool!")
        if user_role == "admin":
            usernames = []
            for user in list(get_profile_dataset(pd_output=False)):
                usernames.append(user.username)
            username = st.selectbox(
                label="Select User",
                help="Select the user to check his scripts",
                options=usernames,
            )
            st.write(
                f"Because you're an Admin, you selected {username}'s scripts to apply in this page"
            )

        else:
            hide_sidebar()

        files_uploaded = st.file_uploader(
            label="Upload your CSV file/s", accept_multiple_files=True, type="csv"
        )
        print("FIles uploaded: ", files_uploaded)
        if files_uploaded is not None and len(files_uploaded) > 0:
            user_steps = import_user_steps(username)
            print("Here are the user steps: ", user_steps)
            for uploaded_file in files_uploaded:
                # Save the uploaded file to a temporary location
                file_path = os.path.join(
                    f"././users/{username}/uploads", uploaded_file.name
                )
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())

                # Display the uploaded file
                st.write(f"Uploaded file: {uploaded_file.name}")
                st.write("Preview:")
                dataframe = pd.read_csv(file_path)
                st.write(dataframe)
            # Start the scripts one by one to work
            if user_steps is not None:
                for step_number, step_function in user_steps.items():
                    if step_function is not None:
                        print(f"Executing step {step_number} for user {username}")
                        if not step_function(username):
                            break
                    else:
                        print(
                            f"No step {step_number} module found for user {username}."
                        )
            else:
                print(f"No steps found for user {username}.")

            # Check for new files every time the app runs
            new_files = check_for_new_files(username)
            # Display the new files and provide download buttons
            if new_files:
                st.write("Your New output files:")
                for filename in new_files:
                    with open(
                        f"././users/{username}/outputs/{filename}", "rb"
                    ) as file_data:
                        download_button = st.download_button(
                            f"Download {filename}",
                            data=file_data,
                            file_name=filename,
                        )
                        if download_button:
                            # Logic to download the file
                            st.write(f"Downloading {filename}...")
    else:
        hide_sidebar()
        st.session_state["is_logged_in"] = False
        st.error("You're not logged in or you're not authorized to access this page!")
        st.page_link(
            page="login.py", label="Click here to login"
        )  # Rerun the app to show login page


protected_page()
