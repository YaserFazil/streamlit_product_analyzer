from flask import Flask, render_template, request, flash, redirect, session
from flask_socketio import SocketIO, emit
import subprocess
from functools import wraps
import os

UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"csv"}

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.secret_key = "sfjlasksecrt"
socketio = SocketIO(app, debug=True, cors_allowed_origins="*", async_mode="eventlet")


# Define a decorator to check if user is logged in
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "logged_in" not in session:
            flash("Please log in first.")
            return redirect("/login")
        return f(*args, **kwargs)

    return decorated_function


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        # Check credentials (replace with your authentication logic)
        if username == "admin" and password == "password":
            session["logged_in"] = True
            return redirect("/")
        else:
            flash("Invalid credentials. Please try again.")

    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    session.pop("logged_in", None)
    return redirect("/login")


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/run_script", methods=["POST"])
def run_script():
    if "database_export" not in request.files:
        flash("No file part")
        print("It's here")
        return redirect(request.url)

    database_export = request.files["database_export"]

    if database_export.filename == "":
        flash("No selected file")
        print("Step 2")
        return redirect(request.url)

    if database_export and allowed_file(database_export.filename):
        print("step 3")
        database_export.save(
            os.path.join(app.config["UPLOAD_FOLDER"], "database_export.csv")
        )

        # Call your script functions here

        return "Scripts executed successfully!"
    else:
        return "Invalid file format"


@socketio.on("my_event")
def checkping():
    for x in range(5):
        cmd = "ping -c 1 8.8.8.8|head -2|tail -1"
        listing1 = subprocess.run(cmd, stdout=subprocess.PIPE, text=True, shell=True)
        sid = request.sid
        emit("server", {"data1": x, "data": listing1.stdout}, room=sid)
        socketio.sleep(1)


if __name__ == "__main__":
    app.run(debug=True)
