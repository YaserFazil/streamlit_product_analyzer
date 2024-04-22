import uuid
from datetime import datetime
from dynamodb_data import UserNew
import os


# Check the user email in DynamoDB if it's exist or not
def check_user(email):
    user = list(UserNew.scan(email=email))
    if user:
        return True
    else:
        return False


def validate_user(username):
    user = list(UserNew.scan(username=username))
    if user:
        return True
    else:
        return False


def delete_user_account(user_id):
    try:
        user = UserNew.get(id=user_id)
        user.delete()
        return {"success": True, "message": "User Account Deleted!"}
    except Exception as e:
        return {
            "success": False,
            "message": f"Something went wrong while deleting the user! Error: {e}",
        }


def update_user_account(user_id, **kwargs):
    try:
        timestamp = datetime.now()
        user = UserNew.get(id=user_id)
        if "username" in kwargs:
            os.rename(f"./users/{user.username}", f"./users/{kwargs["username"]}")
        user.update(updated_at=timestamp, **kwargs)
        return {"success": True, "message": "User Updated!"}

    except Exception as e:
        return {
            "success": False,
            "message": f"Something went wrong while updating the user! Error: {e}",
        }


def create_user_account(username, email, user_role, user_password, is_active=True):
    try:
        timestamp = datetime.now().isoformat()
        user_id = str(uuid.uuid4())
        username = username
        email = email
        user_role = user_role
        password = user_password
        date_joined = timestamp
        updated_at = timestamp
        is_active = is_active
        last_login = None

        # Retrun a fail msg if the user is already exist in the DB
        user_exist = check_user(email)
        username_exist = validate_user(username)
        if user_exist:
            return {
                "success": False,
                "message": f"User with the {email} email is already exist!",
            }
        elif username_exist:
            return {
                "success": False,
                "message": f"User with the {username} username is already exist!",
            }

        # New user data to be saved on database:
        record = UserNew(
            id=user_id,
            username=username,
            email=email,
            user_role=user_role,
            password=password,
            date_joined=date_joined,
            updated_at=updated_at,
            is_active=is_active,
            last_login=last_login,
        )
        # Save the data gathered for new user on DynamoDB
        record.save()
        # Define the root directory where the users' folders will be created
        root_dir = "users"  # Assuming "users" is the name of your root folder

        # Create the user's folder if it doesn't exist
        user_folder = os.path.join(root_dir, username)
        os.makedirs(user_folder, exist_ok=True)

        # Create subfolders for uploads, outputs, and logs inside the user's folder
        uploads_folder = os.path.join(user_folder, "uploads")
        os.makedirs(uploads_folder, exist_ok=True)

        outputs_folder = os.path.join(user_folder, "outputs")
        os.makedirs(outputs_folder, exist_ok=True)

        logs_folder = os.path.join(user_folder, "logs")
        os.makedirs(logs_folder, exist_ok=True)
        return {"success": True, "message": "User created successfully"}

    except Exception as e:
        return {"success": False, "message": f"Bad request: {str(e)}"}
