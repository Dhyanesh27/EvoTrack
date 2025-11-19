# EvoTrack

EvoTrack extracts Git commit-level data from a GitHub repository, cleans it, stores it in a MySQL database, and displays analytics in a web dashboard.

## Structure

- backend/
  - app.py - Flask app and APIs
  - db.py - SQLAlchemy models and DB helpers
  - extract_repo.py - cloning + extracting + storing logic
- frontend/
  - index.html
  - scripts.js
  - styles.css

## Setup

1. Create a MySQL database and user. Example variables:

   DB_USER=root
   DB_PASSWORD=yourpassword
   DB_HOST=127.0.0.1
   DB_PORT=3306
   DB_NAME=evotrack

2. Install dependencies (prefer a venv):

   pip install -r requirements.txt

3. Run the app from the `backend` folder:

   set DB_USER=...
   set DB_PASSWORD=...
   set DB_HOST=...
   set DB_NAME=...
   python -m backend.app

4. Open http://localhost:5000 in your browser. Enter a GitHub repository URL (git clone URL or https URL) and click Extract. The server will clone the repo, extract commits, store them in MySQL, and display analytics.

Notes:
- The extractor uses GitPython and may need network access.
- The DB models are simple; adapt them to match an ER diagram if you have one provided.
