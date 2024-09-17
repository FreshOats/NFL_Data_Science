from flask import render_template, jsonify
from app import app
from app.scraping import scrape_injuries

@app.route("/", methods=["GET", "POST"])
def index():
    return render_template('index.html')

@app.route("/api/injuries")
def get_injuries():
    data = scrape_injuries()
    return jsonify(data)
