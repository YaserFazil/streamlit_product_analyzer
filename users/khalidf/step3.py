import streamlit as st
import time


def start_step3(username):
    with st.status("Loading step 3 process...", expanded=True) as status:
        try:
            time.sleep(10)
            st.write(f"Step 3 reached")
            status.update(label="Step 3 Completed!", state="complete", expanded=False)
            return True
        except Exception as e:
            st.error(f"Something went wrong while running the step 3. Error: {e}")
            status.update(label="Step 3 Failed!", state="error", expanded=True)
            return False
