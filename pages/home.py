import streamlit as st
import pandas as pd
import importlib
import os
from dynamodb_data import UserNew
import zipfile
import io
import re
from user_scripts_run import run_user_script
from imgs_manager_s3 import *


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


def count_python_scripts(folder_path):
    count = 0
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".py"):
            count += 1
    return count


def check_log_file(username):
    log_file_path = os.path.join(f"././users/{username}/logs/logs.log")
    return os.path.isfile(log_file_path) and os.path.getsize(log_file_path) > 0


def import_user_script_guide(username, file_name):
    py_functions = {}

    def import_pyfiles_from_user_dir(user_folder_path, scripts_folder_path, file_name):
        if os.path.exists(scripts_folder_path) and any(
            filename.endswith(".py") for filename in os.listdir(scripts_folder_path)
        ):
            if os.path.exists(os.path.join(scripts_folder_path, file_name + ".py")):
                module_path = f"users.{username}.scripts.{file_name}"
                try:
                    module = importlib.import_module(module_path)
                    py_functions[f"start_script_{file_name}_guide"] = getattr(
                        module, f"start_script_{file_name}_guide"
                    )
                except Exception as e:
                    py_functions[f"start_script_{file_name}_guide"] = None
        if os.path.exists(user_folder_path) and any(
            filename.endswith(".py") for filename in os.listdir(user_folder_path)
        ):
            if os.path.exists(os.path.join(user_folder_path, file_name + ".py")):
                module_path = f"users.{username}.{file_name}"
                try:
                    module = importlib.import_module(module_path)
                    py_functions[
                        f"step{re.findall(r'\d+$', file_name)[0]}_instr_for_user"
                    ] = getattr(
                        module,
                        f"step{re.findall(r'\d+$', file_name)[0]}_instr_for_user",
                    )
                except Exception as e:
                    py_functions[
                        f"step{re.findall(r'\d+$', file_name)[0]}_instr_for_user"
                    ] = None

    user_folder_path = f"././users/{username}"
    scripts_folder_path = f"././users/{username}/scripts"

    if file_name != [] and os.path.exists(scripts_folder_path):
        import_pyfiles_from_user_dir(user_folder_path, scripts_folder_path, file_name)

    return py_functions


def get_user_py_files(username):
    custom_py_scripts = []
    py_steps_scripts = []
    folders = [f"././users/{username}", f"././users/{username}/scripts"]

    if os.path.exists(folders[0]):
        for file in os.listdir(folders[0]):
            if file.endswith(".py"):
                file_name = os.path.splitext(file)[0]
                py_steps_scripts.append(file_name)

    if os.path.exists(folders[1]):
        for file in os.listdir(folders[1]):
            if file.endswith(".py"):
                file_name = os.path.splitext(file)[0]
                custom_py_scripts.append(file_name)
    py_steps_scripts.sort()
    custom_py_scripts.sort()
    return {
        "user_py_steps": py_steps_scripts,
        "user_custom_py_scripts": custom_py_scripts,
    }


def create_folders_for_user(username):
    folder_name = f"././users/{username}/uploads"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Folder '{folder_name}' created successfully.")


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
    response = UserNew.scan()
    if pd_output == False:
        return response
    return pd.DataFrame(response)


def check_for_new_files(username):
    new_files = set(os.listdir(f"././users/{username}/uploads"))
    return new_files


def check_user_requests_limits(username):
    user_data = list(UserNew.scan(username=username))
    user_requests = {}
    for user in user_data:
        user_requests["allowed"] = user.allowed_requests
        user_requests["made"] = user.requests_made
    if user_requests["made"] >= user_requests["allowed"]:
        return {"is_allowed": False}
    return {"is_allowed": True, "requests_made": user_requests["made"]}


def remove_saved_files(username):
    folder_path = f"././users/{username}/uploads"
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")


if "selected_scripts_values" not in st.session_state.keys():
    st.session_state["selected_scripts_values"] = []

if "go_button_clicked" not in st.session_state.keys():
    st.session_state["go_button_clicked"] = False

if "no_selected_scripts_from_start" not in st.session_state.keys():
    st.session_state["no_selected_scripts_from_start"] = True

scripts_run = {"success": False}


def go_button_clicked():
    st.session_state["go_button_clicked"] = True


import threading


def execute_async(func, *args):
    thread = threading.Thread(target=func, args=args)
    thread.start()


def protected_page():
    global scripts_run
    if "is_logged_in" in st.session_state and st.session_state["is_logged_in"] == True:
        user_role = st.session_state["user_role"]
        username = st.session_state["username"]
        st.title("Home Page")
        st.write("Welcome to SonicLister!")
        if user_role == "admin":
            usernames = []
            for user in list(get_profile_dataset(pd_output=False)):
                usernames.append(user.username)
            username = st.selectbox(
                label="Select User",
                help="Select the user to check his scripts",
                options=usernames,
            )
            create_folders_for_user(username)

            st.write(
                f"Because you're an Admin, you selected {username}'s scripts to apply in this page"
            )

        else:
            hide_sidebar()
            create_folders_for_user(username)
        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            is_email_notification_required = st.checkbox(
                label="Send Email notification after completion"
            )

        user_py_files = get_user_py_files(username)
        while "custom_logger" in user_py_files["user_custom_py_scripts"]:
            user_py_files["user_custom_py_scripts"].remove("custom_logger")

        if "user_select_scripts_key" not in st.session_state:
            st.session_state["user_select_scripts_key"] = 10

        user_selected_script = st.selectbox(
            label="Select Script you want to run.",
            options=user_py_files["user_py_steps"]
            + user_py_files["user_custom_py_scripts"],
            key=st.session_state["user_select_scripts_key"],
        )
        st.session_state["selected_scripts_values"] = [user_selected_script]
        st.divider()

        def reset_file_upload_section():
            st.session_state["file_uploader_key"] += 1
            st.rerun()

        if "file_uploader_key" not in st.session_state:
            st.session_state["file_uploader_key"] = 0

        files_uploaded = st.file_uploader(
            label="Upload your CSV file/s",
            accept_multiple_files=True,
            type="csv",
            key=st.session_state["file_uploader_key"],
        )

        if files_uploaded is not None and len(files_uploaded) > 0:
            for uploaded_file in files_uploaded:
                file_path = os.path.join(
                    f"././users/{username}/uploads", uploaded_file.name
                )
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())

                st.write(f"Uploaded file: {uploaded_file.name}")
                st.write("Preview:")
                dataframe = pd.read_csv(file_path)
                st.write(dataframe)

            if st.button("GO"):
                go_button_clicked()

            from user_manager import update_user_account

            if st.session_state["go_button_clicked"]:
                user_requests_allowed = check_user_requests_limits(username)
                if user_requests_allowed["is_allowed"] == True or user_role == "admin":
                    user_selected_scripts = st.session_state["selected_scripts_values"]
                    if user_selected_scripts == []:
                        st.session_state["no_selected_scripts_from_start"] = True
                        user_selected_scripts = user_py_files["user_py_steps"]
                    else:
                        st.session_state["no_selected_scripts_from_start"] = False
                    scripts_run = run_user_script(
                        username,
                        user_selected_scripts,
                        is_email_notification_required,
                        st.session_state["email"],
                    )
                    if scripts_run["success"] == True and user_role == "regular":
                        update_user_account(
                            st.session_state["user_id"],
                            **{
                                "requests_made": user_requests_allowed["requests_made"]
                                + 1
                            },
                        )
                    elif scripts_run["success"] == False and user_role == "regular":
                        st.error(scripts_run["msg"])

                    if scripts_run["success"] == True:
                        st.session_state["user_select_scripts_key"] += 10
                        st.session_state["file_uploader_key"] += 1
                else:
                    st.session_state["file_uploader_key"] += 1
                    st.error("You reached your requests limits! Please contact Admin.")

        new_files = check_for_new_files(username)
        if new_files:
            st.divider()
            st.write(
                "List of files you can download or keep to use it for the program:"
            )
            st.write(new_files)
            user_folder = f"././users/{username}/uploads"
            zip_buffer = zip_folder(user_folder)
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label=f"Download your files in Zip",
                    help="Download all of your files listed above in one Zip file.",
                    data=zip_buffer.getvalue(),
                    file_name=f"{username}_uploads.zip",
                    mime="application/zip",
                )
            with col2:
                if st.button("Remove Saved Files"):
                    remove_saved_files(username)
                    reset_file_upload_section()
                    st.success("All saved files have been removed successfully.")

        images_stats = imgs_stats_in_s3(username)
        if (
            images_stats
            and "object_count" in images_stats
            and images_stats["object_count"] > 0
        ):
            st.divider()
            with st.expander("Images"):
                st.write(
                    f"Min est mins to zip images: {images_stats['estimated_time_to_zip_minutes']}"
                )
                st.write(f"Images Count: {images_stats['object_count']}")
                st.write(f"Total Size (MB): {int(images_stats['total_size_mb'])}")
                st.write(
                    f"Total Size (GB): {'{:.3f}'.format(images_stats['total_size_gb'])}"
                )
                if images_stats != []:
                    st.text_area(
                        "Images List:",
                        value="\n".join(images_stats["images_list"]),
                        disabled=True,
                    )
            st.button(
                "Empty Images Folder",
                help="Removes all saved images!",
                on_click=empty_images_folder,
                kwargs={"username": username},
            )
            zip_file_availabality = check_zip(username)
            if (
                "is_available" in zip_file_availabality
                and zip_file_availabality["is_available"] == True
            ):
                st.markdown(
                    f"[Download Images ZIP]({zip_file_availabality['download_link']})",
                    help="There is already an images zip folder saved, you can download it by clicking the link",
                )
            zip_imgs_btn_clicked = st.button(
                "Zip Images",
                help="Zip a new images.zip file. You'll get an email when the images zipping process finishes.",
            )
            if zip_imgs_btn_clicked:
                execute_async(zip_imgs_in_s3, username, st.session_state["email"])
                st.info(
                    "We'll email you after the images zipping process completes! :)"
                )
                if (
                    "is_available" in zip_file_availabality
                    and zip_file_availabality["is_available"] == True
                ):
                    st.markdown(
                        f"[Download images.zip file]({zip_file_availabality['download_link']})",
                        help="Here is your new images.zip file!",
                    )

        log_exists = check_log_file(username)
        if log_exists:
            st.divider()
            with open(f"././users/{username}/logs/logs.log", "r") as file:
                log_content = file.read()
            st.text_area(
                "Log Content For Previous running", value=log_content, disabled=True
            )
            st.download_button(
                label="Download logs.log file", data=log_content, file_name="log.txt"
            )
        st.divider()
        st.title("Guide & Instructions")
        user_selected_scripts_guide = st.selectbox(
            label="Select Script/Step guide.",
            options=user_py_files["user_py_steps"]
            + user_py_files["user_custom_py_scripts"],
        )
        user_script_guide = import_user_script_guide(
            username, user_selected_scripts_guide
        )
        if user_script_guide is not None:
            for function_name, script_function in user_script_guide.items():
                if script_function is not None:
                    script_function()
                else:
                    print(
                        f"No script {function_name} module found for user {username}."
                    )
        else:
            print(f"No steps or scripts found for user {username}.")

    else:
        hide_sidebar()
        st.session_state["is_logged_in"] = False
        st.error("You're not logged in or you're not authorized to access this page!")
        st.page_link(page="login.py", label="Click here to login")


protected_page()
