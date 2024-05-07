from email_notification import send_email
import os
import importlib
import re


# Function to run user's script and capture logs
def run_user_script(
    username,
    user_selected_scripts,
    pause_after_completion=False,
    send_email_notification=False,
    recipient=None,
):
    try:
        user_scripts = import_user_steps(username, user_selected_scripts)
        # Start the scripts one by one to work
        if user_scripts is not None:
            send_email_permission = False
            for function_name, script_function in user_scripts.items():
                if script_function is not None:
                    print(f"Executing step {function_name} for user {username}")
                    if script_function(username):
                        send_email_permission = True
                        if pause_after_completion:
                            break
                    else:
                        return {
                            "success": False,
                            "msg": f"Something went wrong in function for {username}!",
                        }
                else:
                    print(
                        f"No script {function_name} module found for user {username}."
                    )
                    return {
                        "success": False,
                        "msg": f"No script {function_name} module found for user {username}.",
                    }
            if send_email_notification and send_email_permission:
                send_email(
                    "Your Products Status",
                    "Congrats! Jeff's program finished the process you started earlier.",
                    recipient,
                )
            return {
                "success": True,
                "msg": f"Program Completed!",
            }
        else:
            print(f"No steps or scripts found for user {username}.")
            return {
                "success": False,
                "msg": f"No steps or scripts found for user {username}.",
            }
    except Exception as e:
        return {
            "success": False,
            "msg": f"Something went wrong while running the program for {username}! Error: {e}",
        }


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


def import_user_steps(username, files_names=[]):
    py_functions = {}

    # Function to import python scripts from users/{username}/scripts folder
    def import_pyfiles_from_scripts_dir(
        user_folder_path, scripts_folder_path, files_names
    ):
        for file_name in files_names:
            if os.path.exists(os.path.join(scripts_folder_path, file_name + ".py")):
                # Construct the module path dynamically based on username and py file name in scripts folder
                module_path = f"users.{username}.scripts.{file_name}"
                try:
                    module = importlib.import_module(module_path)
                    # Now you can access the function inside the py file or attribute
                    py_functions[f"start_script_{file_name}"] = getattr(
                        module, f"start_script_{file_name}"
                    )
                except Exception as e:
                    py_functions[f"start_script_{file_name}"] = (
                        None  # Module doesn't exist
                    )
            elif os.path.exists(os.path.join(user_folder_path, file_name + ".py")):
                # Construct the module path dynamically based on username and py file name in scripts folder
                module_path = f"users.{username}.{file_name}"
                try:
                    module = importlib.import_module(module_path)
                    # Now you can access the function inside the py file or attribute
                    py_functions[f"start_step{re.findall(r'\d+$', file_name)[0]}"] = (
                        getattr(
                            module, f"start_step{re.findall(r'\d+$', file_name)[0]}"
                        )
                    )
                except Exception as e:
                    py_functions[f"start_step{re.findall(r'\d+$', file_name)[0]}"] = (
                        None  # Module doesn't exist
                    )

    # Function to import steps from a folder
    def import_steps_from_folder(folder_path):
        num_steps = count_python_scripts(folder_path)
        for step_number in range(1, num_steps + 1):
            # Construct the module path dynamically based on username and step number
            module_path = f"users.{username}.step{step_number}"

            # Import the module dynamically
            try:
                module = importlib.import_module(module_path)
                # Now you can access the desired function or attribute
                py_functions[f"start_step{step_number}"] = getattr(
                    module, f"start_step{step_number}"
                )
            except ImportError:
                py_functions[f"start_step{step_number}"] = None  # Module doesn't exist

    # Import steps from both user folder and scripts folder
    user_folder_path = f"././users/{username}"
    scripts_folder_path = f"././users/{username}/scripts"

    if files_names == [] and os.path.exists(user_folder_path):
        import_steps_from_folder(user_folder_path)

    if files_names != [] and os.path.exists(scripts_folder_path):
        import_pyfiles_from_scripts_dir(
            user_folder_path, scripts_folder_path, files_names
        )

    return py_functions
