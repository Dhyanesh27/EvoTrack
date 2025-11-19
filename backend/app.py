
import os
import sys
import time
import json
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory, session as flask_session
from flask_cors import CORS
from sqlalchemy import func, text, exc as sqlalchemy_exc
from werkzeug.security import generate_password_hash, check_password_hash

# Import DB models and helpers
from db import (
    init_db, init_engine, get_session, Repository, Author, Commit, File, Diff, Bug, Test, User, UserRepo, Report
)
from extract_repo import extract_and_store

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'NLP'))
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'NLP'))
import importlib
enhanced_text_to_sql = importlib.import_module('enhanced_text_to_sql')
GitHubQueryAnalyzer = enhanced_text_to_sql.GitHubQueryAnalyzer

# Ensure this script can be run directly (not as a package) by adding
# the backend directory to sys.path so imports like `import db` work.
basedir = os.path.dirname(__file__)
def create_app():
    app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), '..', 'frontend_new'), static_url_path='')
    CORS(app)
    app.secret_key = os.getenv('SECRET_KEY', 'dev')

    # initialize DB
    print("Initializing database...")
    engine = init_engine()
    init_db(engine)
    print("Database tables created successfully!")

    @app.route('/api/file-lifecycle')
    def api_file_lifecycle():
        session = get_session(engine)
        try:
            latest_repo = session.query(Repository).order_by(Repository.repo_id.desc()).first()
            if not latest_repo:
                return jsonify([])
            # For each file, get the first and last commit timestamps
            files = session.query(File).filter(File.repo_id == latest_repo.repo_id).all()
            out = []
            for f in files:
                # Get all commit timestamps for this file via diffs
                commit_times = session.query(Commit.timestamp).join(Diff, Diff.commit_id == Commit.commit_id).filter(Diff.file_id == f.file_id).order_by(Commit.timestamp.asc()).all()
                if commit_times:
                    first_commit = commit_times[0][0].isoformat()
                    last_commit = commit_times[-1][0].isoformat()
                else:
                    first_commit = last_commit = None
                out.append({
                    'path': f.path,
                    'first_commit': first_commit,
                    'last_commit': last_commit
                })
            return jsonify(out)
        finally:
            session.close()
    print("Initializing database...")
    engine = init_engine()
    init_db(engine)
    print("Database tables created successfully!")

    @app.route('/api/collaboration-network')
    def api_collaboration_network():
        """
        Returns a collaboration network based on co-commits to the same file.
        Each node is a developer (author), and each link is the number of files both developers have committed to.
        """
        session = get_session(engine)
        try:
            # Get the latest repository
            latest_repo = session.query(Repository).order_by(Repository.repo_id.desc()).first()
            if not latest_repo:
                return jsonify({'nodes': [], 'links': []})

            # Get all files in the repo
            files = session.query(File).filter(File.repo_id == latest_repo.repo_id).all()
            file_id_to_path = {f.file_id: f.path for f in files}

            # For each file, get all authors who committed to it
            from collections import defaultdict, Counter
            file_authors = defaultdict(set)  # file_id -> set of author_ids
            author_id_to_name = {}
            author_id_to_email = {}

            # Get all diffs for this repo, join with commit and author
            diffs = session.query(Diff, Commit, Author).join(Commit, Diff.commit_id == Commit.commit_id).join(Author, Commit.author_id == Author.author_id).join(File, Diff.file_id == File.file_id).filter(File.repo_id == latest_repo.repo_id).all()
            for diff, commit, author in diffs:
                file_authors[diff.file_id].add(author.author_id)
                author_id_to_name[author.author_id] = author.name or author.email
                author_id_to_email[author.author_id] = author.email

            # Count co-commit pairs
            pair_counter = Counter()
            for authors in file_authors.values():
                authors = list(authors)
                for i in range(len(authors)):
                    for j in range(i + 1, len(authors)):
                        a, b = sorted([authors[i], authors[j]])
                        pair_counter[(a, b)] += 1

            # Build nodes (developers)
            nodes = []
            for author_id, name in author_id_to_name.items():
                nodes.append({
                    'id': author_id,
                    'name': name,
                    'email': author_id_to_email.get(author_id, '')
                })

            # Build links (collaborations)
            links = []
            for (a, b), weight in pair_counter.items():
                links.append({
                    'source': a,
                    'target': b,
                    'weight': weight
                })

            return jsonify({'nodes': nodes, 'edges': links})
        finally:
            session.close()
    @app.route('/')
    def serve_frontend():
        if not flask_session.get('user_id'):
            return send_from_directory(app.static_folder, 'login.html')
        return send_from_directory(app.static_folder, 'index.html')

    @app.route('/login')
    def serve_login():
        return send_from_directory(app.static_folder, 'login.html')

    @app.route('/login.html')
    def serve_login_html():
        return send_from_directory(app.static_folder, 'login.html')

    @app.route('/index.html')
    def serve_index_html():
        if not flask_session.get('user_id'):
            return send_from_directory(app.static_folder, 'login.html')
        return send_from_directory(app.static_folder, 'index.html')

    @app.errorhandler(404)
    def handle_404(e):
        if not flask_session.get('user_id'):
            return send_from_directory(app.static_folder, 'login.html'), 200
        return send_from_directory(app.static_folder, 'index.html'), 200

    # initialize DB
    print("Initializing database...")
    engine = init_engine()
    init_db(engine)
    print("Database tables created successfully!")

    @app.route('/api/extract', methods=['POST'])
    def api_extract():
        data = request.get_json() or {}
        repo_url = data.get('repo_url')
        if not repo_url:
            return jsonify({'error': 'repo_url required'}), 400
        if not flask_session.get('user_id'):
            return jsonify({'error': 'login required'}), 401

        session = get_session(engine)

        max_retries = 3
        retry_count = 0
        result = None
        while retry_count < max_retries:
            try:
                session.begin()
                existing_repo = session.query(Repository).filter_by(url=repo_url).first()
                if existing_repo:
                    uid = flask_session.get('user_id')
                    if uid and not session.query(UserRepo).filter_by(user_id=uid, repo_id=existing_repo.repo_id).first():
                        session.add(UserRepo(user_id=uid, repo_id=existing_repo.repo_id))
                        session.commit()
                    session.rollback()
                    return jsonify({'message': 'Repository already extracted', 'repo_id': existing_repo.repo_id}), 200
                summary = extract_and_store(repo_url, session)
                # Save report for the current user
                uid = flask_session.get('user_id')
                repo_obj = session.query(Repository).filter_by(url=repo_url).first()
                if uid and repo_obj:
                    session.add(Report(user_id=uid, repo_id=repo_obj.repo_id, created_at=datetime.utcnow(), summary=json.dumps(summary)))
                session.commit()
                session.close()
                return jsonify({
                    **summary,
                    'message': 'Repository data extracted and stored successfully'
                }), 200
            except Exception as e:
                session.rollback()
                if isinstance(e, sqlalchemy_exc.OperationalError) and "Deadlock found" in str(e):
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Deadlock detected, retrying ({retry_count}/{max_retries})...")
                        import time
                        time.sleep(1)
                        continue
                print(f"Error extracting repository: {str(e)}")
                session.close()
                return jsonify({'error': str(e)}), 500
        # If the loop exits without returning, return a generic error
        session.close()
        return jsonify({'error': 'Extraction failed after retries.'}), 500

    @app.route('/api/commits')
    def api_commits():
        limit = int(request.args.get('limit', 50))
        session = get_session(engine)
        try:
            # Get the latest repository
            latest_repo = session.query(Repository).order_by(Repository.repo_id.desc()).first()
            if not latest_repo:
                return jsonify([])
            rows = session.query(Commit)\
                .filter_by(repo_id=latest_repo.repo_id)\
                .order_by(Commit.timestamp.desc())\
                .limit(limit)\
                .all()
            out = []
            for r in rows:
                # Get total lines changed from diffs (using explicit join condition)
                diffs = session.query(Diff).filter(Diff.commit_id == r.commit_id).all()
                total_added = sum(d.lines_added for d in diffs)
                total_deleted = sum(d.lines_deleted for d in diffs)
                out.append({
                    'hash': r.hash,
                    'author_name': r.author.name if r.author else None,
                    'author_email': r.author.email if r.author else None,
                    'date': r.timestamp.isoformat() if r.timestamp else None,
                    'message': r.message,
                    'insertions': total_added,
                    'deletions': total_deleted,
                })
            return jsonify(out)
        finally:
            session.close()

    @app.route('/api/developers')
    def api_developers():
        session = get_session(engine)
        try:
            # Get the latest repository
            latest_repo = session.query(Repository).order_by(Repository.repo_id.desc()).first()
            if not latest_repo:
                return jsonify([])
            # Get authors who have commits in this repository
            authors = session.query(Author)\
                .join(Commit, Author.author_id == Commit.author_id)\
                .filter(Commit.repo_id == latest_repo.repo_id)\
                .distinct()
            out = []
            for a in authors:
                commit_count = session.query(Commit)\
                    .filter(Commit.author_id == a.author_id, Commit.repo_id == latest_repo.repo_id)\
                    .count()
                out.append({'name': a.name, 'email': a.email, 'commits': commit_count})
            return jsonify(out)
        finally:
            session.close()

    @app.route('/api/bug-trends')
    def api_bug_trends():
        import random
        # Return 6 months of random bug data
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
        introduced = [random.randint(5, 30) for _ in months]
        resolved = [random.randint(5, 30) for _ in months]
        return jsonify({
            'labels': months,
            'introduced': introduced,
            'resolved': resolved
        })
        try:
            latest_repo = session.query(Repository).order_by(Repository.repo_id.desc()).first()
            if not latest_repo:
                return jsonify({'labels': [], 'introduced': [], 'resolved': []})
            bugs = session.query(Bug).join(
                Commit, Bug.fixed_commit == Commit.commit_id
            ).filter(Commit.repo_id == latest_repo.repo_id).all()
            from collections import defaultdict
            introduced_by_month = defaultdict(int)
            resolved_by_month = defaultdict(int)
            for bug in bugs:
                if bug.introduced_in and bug.introduced_in.timestamp:
                    month = bug.introduced_in.timestamp.strftime('%b %Y')
                    introduced_by_month[month] += 1
                if bug.fixed_in and bug.fixed_in.timestamp:
                    month = bug.fixed_in.timestamp.strftime('%b %Y')
                    resolved_by_month[month] += 1
            all_months = sorted(set(list(introduced_by_month.keys()) + list(resolved_by_month.keys())))
            labels = []
            introduced = []
            resolved = []
            for month in all_months:
                labels.append(month)
                introduced.append(introduced_by_month.get(month, 0))
                resolved.append(resolved_by_month.get(month, 0))
            return jsonify({
                'labels': labels,
                'introduced': introduced,
                'resolved': resolved
            })
        finally:
            session.close()

    @app.route('/api/file-evolution')
    def api_file_evolution():
        session = get_session(engine)
        try:
            latest_repo = session.query(Repository).order_by(Repository.repo_id.desc()).first()
            if not latest_repo:
                return jsonify([])
            file_stats = session.query(
                File.path.label('path'),
                File.type.label('type'),
                func.coalesce(func.sum(Diff.lines_added), 0).label('total_added'),
                func.coalesce(func.sum(Diff.lines_deleted), 0).label('total_deleted'),
                func.count(Diff.diff_id).label('changes')
            ).outerjoin(Diff, Diff.file_id == File.file_id).filter(
                File.repo_id == latest_repo.repo_id
            ).group_by(File.path, File.type).order_by(func.count(Diff.diff_id).desc(), func.coalesce(func.sum(Diff.lines_added), 0).desc()).limit(10).all()
            return jsonify([{
                'path': stat.path,
                'type': stat.type,
                'total_added': int(stat.total_added or 0),
                'total_deleted': int(stat.total_deleted or 0),
                'changes': int(stat.changes or 0)
            } for stat in file_stats])
        finally:
            session.close()

    @app.route('/api/stats')
    def api_stats():
        session = get_session(engine)
        try:
            latest_repo = session.query(Repository).order_by(Repository.repo_id.desc()).first()
            if latest_repo:
                total_commits = session.query(Commit).filter_by(repo_id=latest_repo.repo_id).count()
                total_contributors = session.query(Author).join(Commit, Author.author_id == Commit.author_id).filter(Commit.repo_id == latest_repo.repo_id).distinct().count()
                total_files = session.query(File).filter_by(repo_id=latest_repo.repo_id).count()
                open_issues = 0  # If you track issues, update this
                stars = latest_repo.stars
            else:
                total_commits = 0
                total_contributors = 0
                total_files = 0
                open_issues = 0
                stars = 0
            return jsonify({
                'total_commits': total_commits,
                'contributors': total_contributors,
                'open_issues': open_issues,
                'stars': stars,
                'total_files': total_files
            })
        finally:
            session.close()
    
    @app.route('/api/reports')
    def api_reports():
        uid = flask_session.get('user_id')
        if not uid:
            return jsonify([])
        s = get_session(engine)
        try:
            rows = s.query(Report, Repository).join(Repository, Report.repo_id == Repository.repo_id).filter(Report.user_id == uid).order_by(Report.created_at.desc()).all()
            out = []
            for rep, repo in rows:
                try:
                    summ = json.loads(rep.summary or '{}')
                except Exception:
                    summ = {}
                out.append({'report_id': rep.report_id, 'created_at': rep.created_at.isoformat(), 'repo': {'repo_id': repo.repo_id, 'name': repo.name, 'url': repo.url}, 'summary': summ})
            return jsonify(out)
        finally:
            s.close()
    @app.route('/api/query', methods=['POST'])
    def api_query():
        data = request.get_json() or {}
        user_query = data.get('query', '')
        repo_url = data.get('repo_url', None)
        session = get_session(engine)
        try:
            repo_query = session.query(Repository)
            if repo_url:
                repo_obj = repo_query.filter_by(url=repo_url.strip()).first()
            else:
                repo_obj = repo_query.order_by(Repository.repo_id.desc()).first()
            if not repo_obj:
                return jsonify({'error': 'No repository found. Please extract the repository first.'}), 400

            from enhanced_text_to_sql import GitHubQueryAnalyzer
            analyzer = GitHubQueryAnalyzer()
            sql_query = analyzer.build_sql_query(user_query, repo_id=repo_obj.repo_id)
            if sql_query.startswith('Error'):
                return jsonify({'error': sql_query}), 400

            print(f"\n\033[96m==== Executed SQL ====\033[0m\n{sql_query}\n\033[96m======================\033[0m")
            exec_result = session.execute(text(sql_query))
            rows = [dict(r) for r in exec_result.mappings().all()]
            return jsonify({'sql': sql_query, 'result': rows})
        except Exception as e:
            return jsonify({'error': f'SQL execution error: {str(e)}', 'sql': sql_query}), 500
        finally:
            session.close()
            
    

    @app.route('/api/register', methods=['POST'])
    def api_register():
        data = request.get_json() or {}
        name = data.get('name')
        email = data.get('email')
        password = data.get('password')
        if not email or not password:
            return jsonify({'error': 'email and password required'}), 400
        s = get_session(engine)
        try:
            if s.query(User).filter_by(email=email).first():
                return jsonify({'error': 'email already registered'}), 400
            user = User(name=name, email=email, password_hash=generate_password_hash(password))
            s.add(user)
            s.commit()
            flask_session['user_id'] = user.user_id
            return jsonify({'user_id': user.user_id, 'name': user.name, 'email': user.email})
        finally:
            s.close()

    @app.route('/api/login', methods=['POST'])
    def api_login():
        data = request.get_json() or {}
        email = data.get('email')
        password = data.get('password')
        s = get_session(engine)
        try:
            user = s.query(User).filter_by(email=email).first()
            if not user or not check_password_hash(user.password_hash, password or ''):
                return jsonify({'error': 'invalid credentials'}), 401
            flask_session['user_id'] = user.user_id
            return jsonify({'user_id': user.user_id, 'name': user.name, 'email': user.email})
        finally:
            s.close()

    @app.route('/api/logout', methods=['POST'])
    def api_logout():
        flask_session.pop('user_id', None)
        return jsonify({'message': 'logged out'})

    @app.route('/api/me')
    def api_me():
        uid = flask_session.get('user_id')
        session = get_session(engine)
        u = None
        if uid:
            u = session.query(User).filter_by(user_id=uid).first()
        if u is None:
            return jsonify({'user': None})
        return jsonify({'user': {'user_id': u.user_id, 'name': u.name, 'email': u.email}})
        s = get_session(engine)
        try:
            u = s.query(User).filter_by(user_id=uid).first()
            return jsonify({'user': {'user_id': u.user_id, 'name': u.name, 'email': u.email}})
        finally:
            s.close()

    @app.route('/api/my-repos')
    def api_my_repos():
        uid = flask_session.get('user_id')
        if not uid:
            return jsonify([])
        s = get_session(engine)
        try:
            rows = s.query(Repository).join(UserRepo, UserRepo.repo_id == Repository.repo_id).filter(UserRepo.user_id == uid).order_by(UserRepo.id.desc()).all()
            return jsonify([{'repo_id': r.repo_id, 'name': r.name, 'url': r.url, 'stars': r.stars} for r in rows])
        finally:
            s.close()

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='127.0.0.1', port=int(os.getenv('PORT', 5000)), debug=True)

