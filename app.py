from flask import Flask
import refresh_stats
app = Flask(__name__)

@app.route('/')
def home_page():
    return 'dbah stats api home page'

@app.route('/queue_stats_refresh')
async def queue_stats_refresh():
    await refresh_stats.refresh_stats()
    return "stats refresh queued"
