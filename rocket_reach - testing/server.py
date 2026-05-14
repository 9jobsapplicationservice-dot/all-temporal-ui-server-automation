import cgi
import json
import pathlib
from http.server import BaseHTTPRequestHandler, HTTPServer

from rocketreach_bulk import (
    INDEX_PATH,
    build_rocketreach_headers,
    lookup_then_search,
    normalize_linkedin_url,
    process_csv_bytes,
)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ('/', '/index.html'):
            html = INDEX_PATH.read_text(encoding='utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(html.encode('utf-8'))
            return

        self.send_json(404, {'message': 'Not found'})

    def do_POST(self):
        if self.path == '/lookup':
            self.handle_single_lookup()
            return
        if self.path == '/bulk-enrich':
            self.handle_bulk_enrich()
            return
        self.send_json(404, {'message': 'Not found'})

    def handle_single_lookup(self):
        try:
            content_length = int(self.headers.get('Content-Length', '0'))
            raw_body = self.rfile.read(content_length).decode('utf-8')
            body = json.loads(raw_body or '{}')
        except json.JSONDecodeError:
            self.send_json(400, {'message': 'Invalid JSON body.'})
            return

        linkedin_url = normalize_linkedin_url(body.get('linkedin_url', ''))
        if not linkedin_url:
            self.send_json(400, {'message': 'Please provide a valid LinkedIn profile URL.'})
            return

        try:
            headers = build_rocketreach_headers()
        except RuntimeError as error:
            self.send_json(500, {'message': str(error)})
            return

        result = lookup_then_search(linkedin_url, headers)
        self.send_json(result['status_code'], result['body'])

    def handle_bulk_enrich(self):
        try:
            headers = build_rocketreach_headers()
        except RuntimeError as error:
            self.send_json(500, {'message': str(error)})
            return

        content_type = self.headers.get('Content-Type', '')
        if 'multipart/form-data' not in content_type:
            self.send_json(400, {'message': 'Please upload the CSV as multipart/form-data.'})
            return

        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                'REQUEST_METHOD': 'POST',
                'CONTENT_TYPE': content_type,
            },
            keep_blank_values=True,
        )

        if 'file' not in form:
            self.send_json(400, {'message': 'CSV file field "file" is required.'})
            return

        file_item = form['file']
        if not getattr(file_item, 'file', None):
            self.send_json(400, {'message': 'Uploaded CSV file could not be read.'})
            return

        file_bytes = file_item.file.read()
        if not file_bytes:
            self.send_json(400, {'message': 'Uploaded CSV file is empty.'})
            return

        try:
            csv_text, stats = process_csv_bytes(file_bytes, headers)
        except ValueError as error:
            self.send_json(400, {'message': str(error)})
            return
        except Exception as error:
            self.send_json(500, {'message': f'CSV enrichment failed: {error}'})
            return

        original_name = pathlib.Path(getattr(file_item, 'filename', '') or 'jobs.csv').stem
        download_name = f'{original_name}_rocketreach_clean_recruiters.csv'
        self.send_csv(csv_text, download_name, stats)

    def send_json(self, status_code: int, payload: dict):
        encoded = json.dumps(payload).encode('utf-8')
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def send_csv(self, csv_text: str, filename: str, stats: dict):
        encoded = csv_text.encode('utf-8-sig')
        self.send_response(200)
        self.send_header('Content-Type', 'text/csv; charset=utf-8')
        self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
        self.send_header('Content-Length', str(len(encoded)))
        self.send_header('X-RR-Total', str(stats['total']))
        self.send_header('X-RR-Matched', str(stats['matched']))
        self.send_header('X-RR-Failed', str(stats['failed']))
        self.send_header('X-RR-Skipped', str(stats['skipped']))
        self.send_header('X-RR-No-Match', str(stats['no_match']))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, fmt, *args):
        return


if __name__ == '__main__':
    server = HTTPServer(('127.0.0.1', 8080), Handler)
    print('Serving on http://127.0.0.1:8080')
    server.serve_forever()
