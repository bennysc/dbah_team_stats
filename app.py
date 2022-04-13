from flask import Flask
from flask import jsonify
import refresh_stats
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home_page():
    return 'dbah stats api home page'

@app.route('/queue_stats_refresh')
def queue_stats_refresh():
    thread = Thread(target=refresh_stats.refresh_stats)
    thread.start()
    return jsonify({"task":"queue_stats_refresh","status":"success"})
