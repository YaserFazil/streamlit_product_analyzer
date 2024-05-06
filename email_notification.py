import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os

load_dotenv()


def send_email(subject, body, recipient):
    sender = os.getenv("SENDER_GMAIL")
    password = os.getenv("SENDER_GMAIL_APP_PASSWORD")
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = recipient
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
            smtp_server.login(sender, password)
            smtp_server.sendmail(sender, recipient, msg.as_string())
            smtp_server.quit()
        print("Message sent!")
    except Exception as e:
        print(e)
