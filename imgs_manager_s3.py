import requests
from dotenv import load_dotenv
import os
import json

load_dotenv




def upload_img_to_aws_s3(username, img_file_name, img_path):
    try:
        url = f"{os.getenv("FLASK_API_ENDPOINT")}upload"

        payload = {'username': username}
        files=[
        ('files',(img_file_name,open(img_path,'rb'))),
        ]
        headers = {}

        response = requests.request("POST", url, headers=headers, data=payload, files=files)

        print(response.text)
        return {"status": True, "msg": "Img '{img_file_name}' uploaded to Cloud"}
    except Exception as e:
        return {"status": False, "msg": f"{e}"}




def check_zip(username):
    try:

        url = f"{os.getenv("FLASK_API_ENDPOINT")}check_zip?username={username}"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        print(response.text)
        return json.loads(response.text)
    except Exception as e:
        return False


def imgs_stats_in_s3(username):
    try:

        url = f"{os.getenv("FLASK_API_ENDPOINT")}stats?username={username}"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        print(response.text)
        return json.loads(response.text)
    except Exception as e:
        return False
    

def zip_imgs_in_s3(username, email):
    try:

        url = f"{os.getenv("FLASK_API_ENDPOINT")}zip?username={username}&email={email}"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        print(response.text)
        return True
    except Exception as e:
        return False
