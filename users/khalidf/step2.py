import streamlit as st
import time 

def start_step2(username):
    with st.status("Loading step 2 process...", expanded=True) as status:
        try:
            time.sleep(10)
            st.write(f"Step 2 reached")
            status.update(label="Step 2 Completed!", state="complete", expanded=False)
            return True
        except Exception as e:
            st.error(f"Something went wrong while running the step 2. Error: {e}")
            status.update(label="Step 2 Failed!", state="error", expanded=True)
            return False
