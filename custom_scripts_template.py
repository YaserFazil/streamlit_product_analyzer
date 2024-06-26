# Import required packages for the program
import streamlit as st
import logging

# Import required packages for your Python steps scripts
# import your_script_package


# Function to be trigerred when the user want to understand what does this step do
def start_script_todo_guide():
    st.write("__Script todo Instructions:__")
    st.video("https://www.youtube.com/watch?v=LN_8CsLRmsc")
    st.write("3 Instructions for user to understand what does this step do")


def start_script_todo(username):
    # Create a custom logger named 'logger'
    logger = logging.getLogger("logger")
    logger.setLevel(logging.DEBUG)

    # Configure a file handler for logging
    log_file_path = f"././users/{username}/logs/logs.log"
    # Configure logging to save to a file named 'logs.log'
    logging.basicConfig(
        filename=log_file_path,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    with st.status("Loading Script todo...", expanded=True) as status:
        try:
            uploads_path = f"././users/{username}/uploads/"

            logger.info("Starting user Script todo...")

            st.success("Custom PY Script todo Completed!")
            status.update(
                label="Script todo Completed!", state="complete", expanded=False
            )
            return True
        except Exception as e:
            st.error(f"Error occured while running Script todo! Error: {e}")
            logger.error(f"Error occured while running Script todo! Error: {e}")
            status.update(label="Script todo Failed!", state="error", expanded=True)
            return False
