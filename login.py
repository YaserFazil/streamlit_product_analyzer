import streamlit as st
from functools import wraps
from streamlit_local_storage import LocalStorage
from dynamodb_data import UserNew
from datetime import datetime


# localS = LocalStorage()


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


hide_sidebar()


# This function is used to verify the email and password sent by login endpoint (credential_login) view
def new_login_auth(user_email, password):
    # Fetch user from the DB
    result = UserNew.scan(email=user_email)

    # Iterate through the items in the result
    user = list(result)  # Convert the iterator to a list
    org_password = ""
    if user:
        for item in user:
            org_password = item.password
            org_user_id = item.id
            org_user_role = item.user_role
            org_username = item.username
            is_active = item.is_active
        if password == org_password and is_active == True:
            return {
                "status": "success",
                "id": org_user_id,
                "role": org_user_role,
                "username": org_username,
            }

    return {"status": "failed"}


st.title("Login")

# Email input
email = st.text_input("Email")
email = str(email).lower()

# Password input
password = st.text_input("Password", type="password")

# Login button
if st.button("Login"):
    # Check credentials
    user = new_login_auth(email, password)
    if user["status"] == "success":
        user_id = user["id"]
        user_role = user["role"]
        username = user["username"]
        user = UserNew.get(id=user_id)
        # Update user's last login time and date in DynamoDB
        user.update(last_login=datetime.now())
        st.success("Login successful!")
        st.session_state["is_logged_in"] = True
        st.session_state["user_role"] = user_role
        st.session_state["username"] = username
        if user_role == "admin":
            st.switch_page("./pages/users.py")
        else:
            st.switch_page("./pages/home.py")
    else:
        st.error(
            "Invalid email, password or your account isn't active anymore. Please try again or contact the admin of this website."
        )
