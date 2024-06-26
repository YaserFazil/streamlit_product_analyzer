# Import required packages for the program
import streamlit as st
import os
import logging

# Import required packages for your Python steps scripts
# import your_script_package


# Function to be trigerred when the user want to understand what does this step do
def step1_instr_for_user():
    st.title("Step 1 Instructions:")
    st.video("https://www.youtube.com/watch?v=LN_8CsLRmsc")
    st.write("Instructions for user to understand what does this step do")


# From this function where the step starts when trigered by user in tool home page
def start_step1(username):
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
    with st.status("Loading step 1...", expanded=True) as status:
        try:
            uploads_path = f"././users/{username}/uploads/"

            logger.info("Starting user script 1...")

            st.success("Step 1 Completed!")
            status.update(label="Step 1 Completed!", state="complete", expanded=False)
            return True
        except Exception as e:
            st.error(f"Error occured while running step 1! Error: {e}")
            logger.error(f"Error occured while running step 1! Error: {e}")
            status.update(label="Step 1 Failed!", state="error", expanded=True)
            return False
