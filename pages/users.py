import streamlit as st
from datetime import datetime
import boto3
import pandas as pd
import os
from dotenv import load_dotenv
from user_manager import create_user_account, update_user_account, delete_user_account
import time


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


def protected_page():
    if (
        "is_logged_in" in st.session_state
        and st.session_state["is_logged_in"] == True
        and st.session_state["user_role"] == "admin"
    ):
        load_dotenv()

        endpoint_url = os.getenv("DB_ENDPOINT")
        region_name = os.getenv("DB_AWS_REGION")
        aws_access_key_id = os.getenv("DB_AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("DB_AWS_SECRET_ACCESS_KEY")
        users_table = os.getenv("USERS_TABLE")

        # DynamoDB Solution:
        dynamodb = boto3.resource(
            "dynamodb",
            # endpoint_url=os.getenv("DB_ENDPOINT"), # Uncomment this line to use a local DynamoDB instance
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        table = dynamodb.Table(
            users_table
        )  # Assuming 'users' is your DynamoDB table name

        def get_profile_dataset(pd_output=True):
            response = table.scan()  # Retrieve all items from the DynamoDB table
            items = response["Items"]
            if pd_output == False:
                return items
            return pd.DataFrame(items)
        st.title("Users Manager")
        st.write("Welcome to Users Management Page!")
        # Define column configurations
        column_configuration = {
            "username": st.column_config.TextColumn(
                "Username", help="The username", max_chars=100, required=True
            ),
            "memento_lib_id": st.column_config.TextColumn("Memento Lib ID", help="Paste user's Memento DB Library ID", max_chars=200, required=False),
            "updated_at": st.column_config.TextColumn(
                "Updated At",
                help="The last time when the user updated!",
            ),
            "date_joined": st.column_config.TextColumn(
                "Date Joined",
                help="User Join Date",
            ),
            "last_login": st.column_config.TextColumn(
                "User Last Login",
            ),
            "id": st.column_config.TextColumn(
                "User ID",
            ),
            "user_role": st.column_config.SelectboxColumn(
                "User Role", options=["admin", "regular"], required=True
            ),
            "is_active": st.column_config.CheckboxColumn("Is Active", default=True),
            "password": st.column_config.TextColumn(
                "User Password", help="User password to login", required=True
            ),
            "requests_made": st.column_config.NumberColumn("Requests Made", help="Number of requests this user made through the project."),
            "allowed_requests": st.column_config.NumberColumn("Allowed Requests", help="Enter number of allowed requests for the user", required=True),
            "email": st.column_config.TextColumn(
                "Email", help="The user's email address", required=True
            ),
        }

        # Display the data editor
        st.data_editor(
            get_profile_dataset(),
            key="users_manager",
            column_order=(
                "username",
                "email",
                "memento_lib_id",
                "is_active",
                "user_role",
                "password",
                "requests_made",
                "allowed_requests",
                "last_login",
                "updated_at",
                "date_joined",
                "id",
            ),
            disabled=("id", "last_login", "updated_at", "date_joined"),
            column_config=column_configuration,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
        )
        added_users = st.session_state["users_manager"]["added_rows"]
        edited_users = st.session_state["users_manager"]["edited_rows"]
        deleted_users = st.session_state["users_manager"]["deleted_rows"]
        if added_users:
            for user in added_users:
                account_is_created = create_user_account(
                    **user
                )
                with st.status("Loading user creation process...", expanded=True) as status:
                    st.write("Searching for exact user...")
                    time.sleep(3)
                    if account_is_created["success"] == True:
                        del st.session_state["users_manager"]
                        st.write("Good News! No Match Found.")
                        time.sleep(2)
                        st.write("Creating User Account...")
                        time.sleep(2)
                        st.success(f"{user["username"]} User created!")
                        time.sleep(3)
                        status.update(label="User Account Creation Completed!", state="complete", expanded=True)
                        st.switch_page("./pages/users.py")
                    else:
                        st.error(account_is_created["message"])
                        status.update(label="User Account Creation Failed!", state="error", expanded=True)
        elif edited_users:
            updated_user_data = edited_users
            user_index = list(updated_user_data.keys())[0]
            all_users = get_profile_dataset(pd_output=False)
            old_user_data = all_users[user_index]
            account_is_updated = update_user_account(old_user_data["id"], **updated_user_data[user_index])
            with st.status("Loading user management process...", expanded=True) as status:
                st.write("Searching for exact user with username or email...")
                if account_is_updated["success"] == True:
                    del st.session_state["users_manager"]
                    st.write("Good News! No Match Found.")
                    st.write("Updating User Account...")
                    st.success(f"User Updated!")
                    status.update(label="User Account Update Completed!", state="complete", expanded=True)
                    time.sleep(3)
                    st.switch_page("./pages/users.py")
                else:
                    st.error(account_is_updated["message"])
                    status.update(label="User Account Update Failed!", state="error", expanded=True)
        elif deleted_users:
            all_users = get_profile_dataset(pd_output=False)
            for user in deleted_users:
                old_user_data = all_users[user]
                user_deleted = delete_user_account(old_user_data["id"])
                with st.status("Loading user management process...", expanded=True) as status:
                    if user_deleted["success"] == True:
                        st.write("Deleting User Account...")
                        st.success("User Deleted!")
                        status.update(label="User Account Deletion Completed!", state="complete", expanded=True)
                    else:
                        st.error(user_deleted["message"])
                        status.update(label="User Account Deletion Failed!", state="error", expanded=True)

    else:
        hide_sidebar()
        st.session_state["is_logged_in"] = False
        st.error("You're not logged in or you're not authorized to access this page!")
        st.page_link(
            page="login.py", label="Click here to login"
        )  # Rerun the app to show login page
        st.page_link(
            page="./pages/home.py", label="Click here to go to home"
        )  # Rerun the app to show login page


protected_page()
