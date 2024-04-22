import streamlit as st
import time


def start_step1(username):
    with st.status("Loading step 1 process...", expanded=True) as status:
        try:
            time.sleep(10)
            st.write(f"Step 1 reached")
            status.update(label="Step 1 Completed!", state="complete", expanded=False)
            return True
        except Exception as e:
            st.error(f"Something went wrong while running the step 1. Error: {e}")
            status.update(label="Step 1 Failed!", state="error", expanded=True)
            return False
