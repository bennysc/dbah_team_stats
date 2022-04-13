import os
import http.server
import socketserver
import refresh_stats

from http import HTTPStatus


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        i
        self.send_response(HTTPStatus.OK)
        self.end_headers()
        refresh_stats.refresh_stats()
        msg = f'Refreshed data'
        self.wfile.write(msg.encode())


port = int(os.getenv('PORT', 80))
print('Listening on port %s' % (port))
httpd = socketserver.TCPServer(('', port), Handler)
httpd.serve_forever()


from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "dbah stats api home"}