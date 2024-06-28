from dynamorm import DynaModel
from marshmallow import fields
import os
from dotenv import load_dotenv

load_dotenv()

endpoint_url = os.getenv("DB_ENDPOINT")
region_name = os.getenv("DB_AWS_REGION")
aws_access_key_id = os.getenv("DB_AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("DB_AWS_SECRET_ACCESS_KEY")
users_table = os.getenv("USERS_TABLE")


# This is User model for users
class UserNew(DynaModel):
    class Table:
        resource_kwargs = {
            # "endpoint_url": endpoint_url, # this is used for localhost DynamoDB
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }
        name = users_table
        hash_key = "id"
        read = 25
        write = 5

    class Schema:
        id = fields.UUID(required=True)
        username = fields.String(required=True)
        email = fields.String(required=True)
        user_role = fields.String(required=True)
        memento_lib_id = fields.String(required=False)
        password = fields.String(required=True)
        requests_made = fields.Integer(default=0)
        allowed_requests = fields.Integer(default=10)
        date_joined = fields.DateTime(format="iso")
        updated_at = fields.DateTime(format="iso")
        is_active = fields.Boolean(required=True)
        last_login = fields.DateTime(allow_none=True, format="iso")
