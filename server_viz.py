    
from flask import Flask
from flask import send_file
import os
import json
from flask import Response
from flask import jsonify
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/odsComuna')
def ods_comuna():
	with open('comuna_vs_ods.json', 'r') as f:
		datastore = json.load(f)
		return jsonify(datastore)


if __name__ == "__main__":
    app.run(debug=True, port=5000, host='0.0.0.0')
