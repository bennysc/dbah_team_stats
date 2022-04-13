from flask import Flask
import refresh_stats
app = Flask(__name__)

@app.route('/')
def home_page():
    return 'dbah stats api home page'

@app.route('/refresh_stats')
async def refresh_stats():
    await refresh_stats.refresh_stats()
    return "stats refresh queued"
