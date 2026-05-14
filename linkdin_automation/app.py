from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import csv
from datetime import datetime
import os

app = Flask(__name__)
CORS(app)

PATH = 'all excels/'
APPLIED_JOBS_CSV = os.path.join(PATH, 'all_applied_applications_history.csv')
APPLIED_JOBS_FIELDS = ['Date', 'Company Name', 'Position', 'Job Link', 'Submitted', 'HR Name', 'HR Position', 'HR Profile Link']
##> ------ Karthik Sarode : karthik.sarode23@gmail.com - UI for excel files ------
@app.route('/')
def home():
    """Displays the home page of the application."""
    return render_template('index.html')

@app.route('/applied-jobs', methods=['GET'])
def get_applied_jobs():
    '''
    Retrieve clean applied-jobs rows from the applications history CSV file.
    '''
    try:
        jobs = []
        with open(APPLIED_JOBS_CSV, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                jobs.append({
                    'Date': row.get('Date', ''),
                    'Company_Name': row.get('Company Name', ''),
                    'Position': row.get('Position', ''),
                    'Job_Link': row.get('Job Link', ''),
                    'Submitted': row.get('Submitted', ''),
                    'HR_Name': row.get('HR Name', ''),
                    'HR_Position': row.get('HR Position', ''),
                    'HR_Profile_Link': row.get('HR Profile Link', ''),
                })
        return jsonify(jobs)
    except FileNotFoundError:
        return jsonify({"error": "No applications history found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/applied-jobs/mark-submitted', methods=['PUT'])
def update_applied_date():
    """
    Mark a job as submitted using its Job Link as the stable identifier.
    """
    try:
        payload = request.get_json(silent=True) or {}
        job_link = (payload.get('job_link') or '').strip()

        if not job_link:
            return jsonify({"error": "job_link is required"}), 400

        if not os.path.exists(APPLIED_JOBS_CSV):
            return jsonify({"error": f"CSV file not found at {APPLIED_JOBS_CSV}"}), 404

        data = []
        with open(APPLIED_JOBS_CSV, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            field_names = reader.fieldnames or APPLIED_JOBS_FIELDS
            found = False
            for row in reader:
                if (row.get('Job Link') or '').strip() == job_link:
                    row['Submitted'] = 'Applied'
                    row['Date'] = datetime.now().strftime('%d/%m/%Y')
                    found = True
                data.append(row)

        if not found:
            return jsonify({"error": "Job link not found"}), 404

        with open(APPLIED_JOBS_CSV, 'w', encoding='utf-8', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(data)

        return jsonify({"message": "Submitted status updated successfully"}), 200
    except Exception as e:
        print(f"Error updating applied date: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

##<
