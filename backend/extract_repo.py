import os
import shutil
import tempfile
import os
import re
import requests
from git import Repo
import pandas as pd
from datetime import datetime
import pathlib
import pytz
from urllib.parse import urlparse

# Using absolute imports since we modified app.py to handle this
from db import Repository, Author, Commit, File, Diff, Bug, Test
from bug_detection import analyze_bug_fix

def get_github_repo_info(repo_url):
    """Extract repository information from GitHub API"""
    try:
        # Parse GitHub URL to get owner/repo
        parsed = urlparse(repo_url)
        if 'github.com' not in parsed.netloc:
            return {'error': 'Not a GitHub repository URL', 'stars': 0, 'forks': 0, 'name': ''}
        
        path_parts = parsed.path.strip('/').split('/')
        if len(path_parts) < 2:
            return {'error': 'Invalid repository URL format', 'stars': 0, 'forks': 0, 'name': ''}
        
        owner, repo = path_parts[:2]
        # Remove .git extension if present
        repo = repo.replace('.git', '')
        
        api_url = f"https://api.github.com/repos/{owner}/{repo}"
        
        # Use GitHub token if available
        headers = {'Accept': 'application/vnd.github.v3+json'}
        github_token = os.getenv('GITHUB_TOKEN')
        if github_token:
            headers['Authorization'] = f'token {github_token}'
            print(f"Using GitHub token for {owner}/{repo}")
            
        # Make API request
        response = requests.get(api_url, headers=headers, timeout=10)
        
        # Handle different response status codes
        if response.status_code == 200:
            data = response.json()
            return {
                'name': data.get('name', repo),
                'stars': data.get('stargazers_count', 0),
                'forks': data.get('forks_count', 0),
                'description': data.get('description', ''),
                'default_branch': data.get('default_branch', 'main')
            }
        elif response.status_code == 401:
            print("GitHub API Error: Invalid or expired token")
            return {'error': 'Invalid GitHub token', 'stars': 0, 'forks': 0, 'name': repo}
        elif response.status_code == 403:
            print("GitHub API Error: Rate limit exceeded")
            return {'error': 'Rate limit exceeded', 'stars': 0, 'forks': 0, 'name': repo}
        elif response.status_code == 404:
            print(f"GitHub API Error: Repository {owner}/{repo} not found")
            return {'error': 'Repository not found', 'stars': 0, 'forks': 0, 'name': repo}
        else:
            print(f"GitHub API Error: Unexpected status {response.status_code}")
            return {'error': f'API error {response.status_code}', 'stars': 0, 'forks': 0, 'name': repo}
            
    except requests.exceptions.RequestException as e:
        print(f"GitHub API Network Error: {e}")
        return {'error': 'Network error', 'stars': 0, 'forks': 0, 'name': repo}
    except Exception as e:
        print(f"Error fetching GitHub repo info: {e}")
        return {'error': str(e), 'stars': 0, 'forks': 0, 'name': repo}
    
    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return {
                'name': data.get('name', repo),
                'stars': data.get('stargazers_count', 0),
                'forks': data.get('forks_count', 0),
                'description': data.get('description', ''),
                'default_branch': data.get('default_branch', 'main')
            }
    except Exception as e:
        print(f"Error fetching GitHub repo info: {e}")
    
    return {'stars': 0, 'forks': 0, 'name': repo}


def extract_commits_from_repo_local(path):
    repo = Repo(path)
    commits = []
    diffs = []
    files_seen = set()
    files = []

    for commit in repo.iter_commits('--all'):
        try:
            stats = commit.stats
            total = stats.total
            files_changed = list(stats.files.keys())
        except Exception:
            total = {'insertions': 0, 'deletions': 0, 'lines': 0}
            files_changed = []

        # Basic commit info with UTC timestamp conversion
        commit_record = {
            'hash': commit.hexsha,
            'author_name': commit.author.name if commit.author else None,
            'author_email': (commit.author.email.lower() if commit.author and commit.author.email else None),
            'timestamp': commit.committed_datetime.astimezone(pytz.UTC),
            'message': commit.message,
        }
        commits.append(commit_record)

        # Process each file change
        for fpath, fstats in stats.files.items():
            # Track unique files
            if fpath not in files_seen:
                files_seen.add(fpath)
                files.append({
                    'path': fpath,
                    'type': pathlib.Path(fpath).suffix,
                    'status': 'modified'  # Default status, could be refined
                })

            # Record the diff
            diffs.append({
                'commit_hash': commit.hexsha,
                'file_path': fpath,
                'lines_added': fstats.get('insertions', 0),
                'lines_deleted': fstats.get('deletions', 0),
                'change_type': 'modification'  # Default type, could be refined
            })

    commits_df = pd.DataFrame(commits)
    diffs_df = pd.DataFrame(diffs)
    files_df = pd.DataFrame(files)
    
    return commits_df, diffs_df, files_df


def extract_and_store(repo_url, session, repo_name=None):
    # Clean and validate repo_url
    repo_url = repo_url.strip()
    if not (repo_url.startswith('http://') or repo_url.startswith('https://')):
        raise RuntimeError(f"Invalid repository URL: {repo_url}")
    if ' ' in repo_url:
        raise RuntimeError(f"Repository URL contains spaces: {repo_url}")

    # Create temp dir in current working directory for better permissions
    tmpdir = os.path.join(os.getcwd(), 'tmp_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
    # If temp dir exists, remove it to ensure a clean clone
    if os.path.exists(tmpdir):
        import shutil
        try:
            shutil.rmtree(tmpdir)
        except Exception as e:
            print(f"[extract_repo] Failed to remove existing temp dir {tmpdir}: {e}")
    os.makedirs(tmpdir, exist_ok=True)

    try:
        # Clone with specific git options for Windows compatibility
        repo = Repo.clone_from(
            repo_url,
            tmpdir,
            env={'GIT_CONFIG_PARAMETERS': "'core.longpaths=true'"},
            allow_unsafe_options=True
        )
    except Exception as e:
        # Clean up safely
        if os.path.exists(tmpdir):
            for root, dirs, files in os.walk(tmpdir, topdown=False):
                for name in files:
                    try:
                        os.chmod(os.path.join(root, name), 0o666)
                    except: pass
                for name in dirs:
                    try:
                        os.chmod(os.path.join(root, name), 0o777)
                    except: pass
            shutil.rmtree(tmpdir, ignore_errors=True)
        raise RuntimeError(f"Failed to clone repository: {str(e)}")

    try:
        commits_df, diffs_df, files_df = extract_commits_from_repo_local(tmpdir)

        # Basic cleaning
        if not commits_df.empty:
            commits_df['author_email'] = commits_df['author_email'].str.lower()
            commits_df['timestamp'] = pd.to_datetime(commits_df['timestamp'], utc=True)
            commits_df.drop_duplicates(subset=['hash'], inplace=True)

        # Upsert repository
        repo_obj = session.query(Repository).filter_by(url=repo_url).first()
        if not repo_obj:
            # Get repository information from GitHub API
            github_info = get_github_repo_info(repo_url)
            if 'error' in github_info:
                print(f"Warning: {github_info['error']}")
                print("Continuing with limited repository information...")
            
            repo_obj = Repository(
                url=repo_url,
                name=repo_name or github_info.get('name', '') or os.path.basename(repo_url),
                stars=github_info.get('stars', 0),
                forks=github_info.get('forks', 0)
            )
            session.add(repo_obj)
            session.commit()

        # Upsert authors
        author_map = {}
        if not commits_df.empty:
            for _, row in commits_df[['author_name', 'author_email']].drop_duplicates().iterrows():
                email = row['author_email']
                if not email or pd.isna(email):
                    continue
                author = session.query(Author).filter_by(email=email).first()
                if not author:
                    author = Author(name=row['author_name'], email=email)
                    session.add(author)
                    session.commit()
                author_map[email] = author

        # Insert files first
        file_map = {}  # path -> File object
        for _, frow in files_df.iterrows():
            path = frow['path']
            existing = session.query(File).filter_by(repo_id=repo_obj.repo_id, path=path).first()
            if not existing:
                file_obj = File(
                    repo_id=repo_obj.repo_id,
                    path=path,
                    type=frow['type'],
                    status=frow['status']
                )
                session.add(file_obj)
                session.commit()
                file_map[path] = file_obj
            else:
                file_map[path] = existing

            # Process commits
        inserted_commits = 0
        processed_commits = set()
        
        for _, crow in commits_df.iterrows():
            commit_hash = crow['hash']
            if commit_hash in processed_commits:
                continue

            # Check if commit exists in any repository
            existing_commit = session.query(Commit).filter_by(hash=commit_hash).first()
            
            author = None
            if pd.notna(crow['author_email']):
                author = author_map.get(crow['author_email'])

            if not author:
                continue  # Skip commits without valid authors

            if existing_commit:
                # Update the existing commit with the new repository
                if existing_commit.repo_id != repo_obj.repo_id:
                    existing_commit.repo_id = repo_obj.repo_id
                    session.merge(existing_commit)
                commit_obj = existing_commit
            else:
                # Create new commit
                commit_obj = Commit(
                    hash=commit_hash,
                    repo_id=repo_obj.repo_id,
                    author_id=author.author_id,
                    timestamp=crow['timestamp'].to_pydatetime() if pd.notna(crow['timestamp']) else None,
                    message=crow['message']
                )
                session.add(commit_obj)
                inserted_commits += 1
            
            try:
                session.commit()

                # Analyze commit for bug fixes
                if commit_obj.message:
                    print('Analyzing commit:', commit_obj.hash[:7])
                    print('Message:', commit_obj.message.split('\n')[0])
                    
                    # Get all diffs for this commit
                    commit_diffs = [d for d in commit_obj.diffs]
                    bug_info = analyze_bug_fix(commit_obj.message, commit_diffs)
                    
                    if bug_info:
                        print(f"Found bug fix! Description: {bug_info['description']}")
                        # Create new bug entry
                        bug = Bug(
                            description=bug_info['description'],
                            fixed_commit=commit_obj.commit_id  # This is a fix commit
                        )
                        
                        # Try to find the commit that introduced the bug
                        # For now, we'll leave it as None but this could be enhanced
                        # with more sophisticated bug origin analysis
                        session.add(bug)
                        session.commit()
                        print("Bug entry saved to database")

            except Exception as e:
                session.rollback()
                print(f"Error processing commit {commit_hash}: {str(e)}")
                continue
                
            processed_commits.add(commit_hash)

            # Process diffs for this commit
            diff_rows = diffs_df[diffs_df['commit_hash'] == commit_hash]
            for _, drow in diff_rows.iterrows():
                file_obj = file_map.get(drow['file_path'])
                if not file_obj:
                    continue

                # Check if diff already exists
                existing_diff = session.query(Diff).filter_by(
                    commit_id=commit_obj.commit_id,
                    file_id=file_obj.file_id
                ).first()
                
                if not existing_diff:
                    diff = Diff(
                        commit_id=commit_obj.commit_id,
                        file_id=file_obj.file_id,
                        lines_added=int(drow['lines_added']),
                        lines_deleted=int(drow['lines_deleted']),
                        change_type=drow['change_type']
                    )
                    session.add(diff)
            
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                print(f"Error processing diffs for commit {commit_hash}: {str(e)}")
                continue
 
        summary = {
            'repo_url': repo_url,
            'commits_found': int(commits_df.shape[0]) if not commits_df.empty else 0,
            'commits_inserted': inserted_commits,
            'files_inserted': len(file_map),
            'diffs_inserted': session.query(Diff).join(Commit, Diff.commit_id == Commit.commit_id).filter(Commit.repo_id == repo_obj.repo_id).count()
        }
        return summary
    finally:
        # Safe cleanup with permission fixes
        if os.path.exists(tmpdir):
            for root, dirs, files in os.walk(tmpdir, topdown=False):
                for name in files:
                    try:
                        os.chmod(os.path.join(root, name), 0o666)
                    except: pass
                for name in dirs:
                    try:
                        os.chmod(os.path.join(root, name), 0o777)
                    except: pass
            shutil.rmtree(tmpdir, ignore_errors=True)
