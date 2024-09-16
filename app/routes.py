from flask import render_template, jsonify
from app import app
from .scraping import scrape_injuries

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/injuries')
def get_injuries():
    try:
        injuries = scrape_injuries()
        if injuries.empty:
            return jsonify({"error": "Failed to scrape injuries"}), 500
        return jsonify(injuries.to_dict(orient='records'))
    except Exception as e:
        return jsonify({"error": str(e)}), 500
