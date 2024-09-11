from flask import Flask

app = Flask(__name__)

from app import routes  # Move this import to the bottom
