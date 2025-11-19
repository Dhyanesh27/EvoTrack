from typing import Dict, List, Optional, Tuple
import spacy
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk

class GitHubQueryAnalyzer:
    def __init__(self):
        # Load spaCy model
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            # If model not found, download it
            import subprocess
            print("Downloading required spaCy model...")
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
            self.nlp = spacy.load("en_core_web_sm")
            
        # Initialize stopwords
        try:
            self.stop_words = set(stopwords.words('english'))
        except LookupError:
            print("Downloading required NLTK data...")
            nltk.download('stopwords')
            nltk.download('punkt')
            self.stop_words = set(stopwords.words('english'))
            
        # GitHub-specific keywords with error checking
        self.keywords = {
            'show': 'SELECT',
            'display': 'SELECT',
            'find': 'SELECT',
            'search': 'SELECT',
            'list': 'SELECT',
            'count': 'SELECT COUNT(*)',
            'analyze': 'SELECT',
            'top': 'SELECT',
            'most': 'SELECT',
            'track': 'SELECT',
            'monitor': 'SELECT'
        }
        
        # Map entities to actual MySQL table names and columns
        self.entity_mapping = {
            'repository': {
                'patterns': ['repository', 'repositories', 'repo', 'repos'],
                'table': 'repositories',
                'columns': ['repo_id', 'name', 'url', 'stars', 'forks']
            },
            'author': {
                'patterns': ['author', 'authors', 'contributor', 'contributors', 'developer', 'developers'],
                'table': 'authors',
                'columns': ['author_id', 'name', 'email']
            },
            'commit': {
                'patterns': ['commit', 'commits', 'change', 'changes'],
                'table': 'commits',
                'columns': ['commit_id', 'hash', 'author_id', 'repo_id', 'timestamp', 'message']
            },
            'file': {
                'patterns': ['file', 'files', 'document', 'documents', 'code'],
                'table': 'files',
                'columns': ['file_id', 'repo_id', 'path', 'type', 'status']
            },
            'diff': {
                'patterns': ['diff', 'difference', 'changes', 'modification', 'modifications'],
                'table': 'diffs',
                'columns': ['diff_id', 'commit_id', 'file_id', 'lines_added', 'lines_deleted', 'change_type']
            }
        }

        # Time patterns
        self.time_patterns = {
            'today': r'today|current day|this day',
            'yesterday': r'yesterday|previous day|last day',
            'this_week': r'this week|current week',
            'last_week': r'last week|previous week',
            'this_month': r'this month|current month',
            'last_month': r'last month|previous month'
        }

    def preprocess_text(self, text: str) -> List[str]:
        """Preprocess text with error handling"""
        try:
            # Convert to lowercase
            text = text.lower()
            
            # Tokenize the text
            tokens = word_tokenize(text)
            
            # Remove stopwords and punctuation
            tokens = [word for word in tokens if word.isalnum() and word not in self.stop_words]
            
            return tokens
        except Exception as e:
            print(f"Error in preprocess_text: {str(e)}")
            return []

    def identify_action(self, tokens: List[str]) -> str:
        """Identify the SQL action with error handling"""
        try:
            # Check tokens against keywords
            for token in tokens:
                if token in self.keywords:
                    return self.keywords[token]
                    
            # Handle special cases
            if any(token in ['count', 'total', 'sum'] for token in tokens):
                return 'SELECT COUNT(*)'
            
            return 'SELECT'  # Default action
        except Exception as e:
            print(f"Error in identify_action: {str(e)}")
            return 'SELECT'  # Safe default

    def identify_table(self, words: List[str]) -> Optional[str]:
        """Identify table with error handling"""
        try:
            # Convert words to set for faster lookup
            words_set = set(words)
            
            # Find GitHub entity in the query
            for entity, info in self.entity_mapping.items():
                if any(syn in words_set for syn in info['patterns']):
                    return info['table']
                    
            return None
        except Exception as e:
            print(f"Error in identify_table: {str(e)}")
            return None

    def identify_columns(self, words: List[str], table: Optional[str]) -> List[str]:
        """Identify columns with error handling"""
        try:
            # Handle COUNT operations
            if any(word in ['count', 'how many', 'total'] for word in words):
                return ['COUNT(*)']
                
            # Get table info
            table_info = next((info for entity, info in self.entity_mapping.items() 
                             if info['table'] == table), None)
                             
            if not table_info:
                return ['*']
                
            # Check for specific column mentions
            columns = []
            for col in table_info['columns']:
                col_name = col.split('_')[-1]  # Extract base column name
                if col_name in words:
                    columns.append(col)
                    
            if columns:
                return columns
            if table == 'repositories':
                return ['repo_id', 'name', 'stars', 'forks']
            if table == 'authors':
                return ['author_id', 'name', 'email']
            if table == 'commits':
                return ['commit_id', 'hash', 'timestamp']
            if table == 'files':
                return ['file_id', 'path', 'type', 'status']
            if table == 'diffs':
                return ['diff_id', 'lines_added', 'lines_deleted']
            return ['*']
            
        except Exception as e:
            print(f"Error in identify_columns: {str(e)}")
            return ['*']

    def identify_conditions(self, text: str) -> List[str]:
        """Identify conditions with error handling"""
        try:
            conditions = []
            text = text.lower()
            
            # Handle star count conditions
            if 'stars' in text:
                star_match = re.search(r'(\d+)\s*stars', text)
                if star_match:
                    num = star_match.group(1)
                    if 'more than' in text or 'greater than' in text:
                        conditions.append(f"stars >= {num}")
                    elif 'less than' in text:
                        conditions.append(f"stars <= {num}")
                    else:
                        conditions.append(f"stars = {num}")

            # Handle fork count conditions
            if 'forks' in text:
                fork_match = re.search(r'(\d+)\s*forks', text)
                if fork_match:
                    num = fork_match.group(1)
                    if 'more than' in text or 'greater than' in text:
                        conditions.append(f"forks >= {num}")
                    elif 'less than' in text:
                        conditions.append(f"forks <= {num}")
                    else:
                        conditions.append(f"forks = {num}")

            # Handle time-based conditions
            if 'this month' in text:
                conditions.append("EXTRACT(MONTH FROM created_at) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM created_at) = EXTRACT(YEAR FROM CURRENT_DATE)")
            elif 'last month' in text:
                conditions.append("created_at >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND created_at < DATE_TRUNC('month', CURRENT_DATE)")
                    
            return conditions
        except Exception as e:
            print(f"Error in identify_conditions: {str(e)}")
            return []

    def build_sql_query(self, text: str, repo_id: int = None) -> str:
        """Build SQL query with error handling"""
        try:
            # Preprocess the text
            tokens = self.preprocess_text(text)
            if not tokens:
                return "Error: Could not process the input text"
            
            text_lower = text.lower()
            
            # Special case: which author introduced the most bugs
            if ("author" in text_lower or "contributor" in text_lower or "developer" in text_lower) and "introduc" in text_lower and "bug" in text_lower:
                if repo_id is not None:
                    return (
                        f"SELECT authors.name, COUNT(bugs.bug_id) AS bugs_introduced "
                        f"FROM bugs "
                        f"JOIN commits ON bugs.introduced_commit = commits.commit_id "
                        f"JOIN authors ON commits.author_id = authors.author_id "
                        f"WHERE commits.repo_id = {repo_id} "
                        f"GROUP BY authors.author_id, authors.name "
                        f"ORDER BY bugs_introduced DESC "
                        f"LIMIT 1;"
                    )
                else:
                    return (
                        "SELECT authors.name, COUNT(bugs.bug_id) AS bugs_introduced "
                        "FROM bugs "
                        "JOIN commits ON bugs.introduced_commit = commits.commit_id "
                        "JOIN authors ON commits.author_id = authors.author_id "
                        "GROUP BY authors.author_id, authors.name "
                        "ORDER BY bugs_introduced DESC "
                        "LIMIT 1;"
                    )

            # Handle other special cases first
            # Most changed file (sum of additions + deletions)
            if ("most" in text_lower and "file" in text_lower and ("change" in text_lower or "changes" in text_lower or "lines" in text_lower)):
                base = (
                    "SELECT files.path AS most_changed_file, SUM(diffs.lines_added + diffs.lines_deleted) AS total_changes "
                    "FROM files JOIN diffs ON files.file_id = diffs.file_id "
                    "JOIN commits ON diffs.commit_id = commits.commit_id "
                )
                where = f"WHERE commits.repo_id = {repo_id} " if repo_id is not None else ""
                return (base + where + "GROUP BY files.file_id, files.path ORDER BY total_changes DESC LIMIT 1;")

            # Most additions
            if ("most" in text_lower and "file" in text_lower and ("addition" in text_lower or "added" in text_lower)):
                base = (
                    "SELECT files.path AS most_added_file, SUM(diffs.lines_added) AS total_additions "
                    "FROM files JOIN diffs ON files.file_id = diffs.file_id JOIN commits ON diffs.commit_id = commits.commit_id "
                )
                where = f"WHERE commits.repo_id = {repo_id} " if repo_id is not None else ""
                return (base + where + "GROUP BY files.file_id, files.path ORDER BY total_additions DESC LIMIT 1;")

            # Most deletions
            if ("most" in text_lower and "file" in text_lower and ("deletion" in text_lower or "deleted" in text_lower)):
                base = (
                    "SELECT files.path AS most_deleted_file, SUM(diffs.lines_deleted) AS total_deletions "
                    "FROM files JOIN diffs ON files.file_id = diffs.file_id JOIN commits ON diffs.commit_id = commits.commit_id "
                )
                where = f"WHERE commits.repo_id = {repo_id} " if repo_id is not None else ""
                return (base + where + "GROUP BY files.file_id, files.path ORDER BY total_deletions DESC LIMIT 1;")

            # Top N files by changes
            top_match = re.search(r"\btop\s+(\d+)\b", text_lower)
            if top_match and "file" in text_lower:
                n = top_match.group(1)
                base = (
                    "SELECT files.path, SUM(diffs.lines_added + diffs.lines_deleted) AS total_changes "
                    "FROM files JOIN diffs ON files.file_id = diffs.file_id JOIN commits ON diffs.commit_id = commits.commit_id "
                )
                where = f"WHERE commits.repo_id = {repo_id} " if repo_id is not None else ""
                return (base + where + f"GROUP BY files.file_id, files.path ORDER BY total_changes DESC LIMIT {n};")

            # Top contributor (single)
            if ("which developer contributed the most" in text_lower or ("most" in text_lower and any(w in text_lower for w in ["developer", "author", "contributor"]) and any(w in text_lower for w in ["commit", "commits", "contributed"]))):
                base = (
                    "SELECT authors.name AS top_contributor, COUNT(commits.commit_id) AS commit_count "
                    "FROM commits JOIN authors ON commits.author_id = authors.author_id "
                )
                where = f"WHERE commits.repo_id = {repo_id} " if repo_id is not None else ""
                return (base + where + "GROUP BY authors.author_id, authors.name ORDER BY commit_count DESC LIMIT 1;")

            # Top contributors by commit (default 10)
            if "top contributors by commit" in text_lower:
                return ("SELECT authors.name, COUNT(commits.commit_id) as commit_count FROM authors JOIN commits ON authors.author_id = commits.author_id GROUP BY authors.author_id, authors.name ORDER BY commit_count DESC LIMIT 10;")

            # Top N contributors by commit
            if top_match and "contributor" in text_lower:
                n = top_match.group(1)
                return (f"SELECT authors.name, COUNT(commits.commit_id) as commit_count FROM authors JOIN commits ON authors.author_id = commits.author_id GROUP BY authors.author_id, authors.name ORDER BY commit_count DESC LIMIT {n};")

            # Bottom N contributors by commit
            bottom_match = re.search(r"\bbottom\s+(\d+)\b", text_lower)
            if (bottom_match or "bottom" in text_lower) and any(w in text_lower for w in ["contributor", "contributors", "author", "authors", "developer", "developers"]):
                n = bottom_match.group(1) if bottom_match else "3"
                base = (
                    "SELECT authors.name AS name, COUNT(commits.commit_id) AS commit_count "
                    "FROM commits JOIN authors ON commits.author_id = authors.author_id "
                )
                where = f"WHERE commits.repo_id = {repo_id} " if repo_id is not None else ""
                return (base + where + f"GROUP BY authors.author_id, authors.name ORDER BY commit_count ASC LIMIT {n};")

            # Commits from last 30 days
            if "commits from last 30 days" in text_lower:
                return ("SELECT commits.commit_id, authors.name, repositories.name as repo_name, commits.message, commits.timestamp FROM commits JOIN authors ON commits.author_id = authors.author_id JOIN repositories ON commits.repo_id = repositories.repo_id WHERE commits.timestamp >= CURRENT_DATE - INTERVAL 30 DAY ORDER BY commits.timestamp DESC;")

            # Most active developer in last 30 days
            if ("last 30 days" in text_lower and any(w in text_lower for w in ["developer", "author", "contributor"]) and ("most" in text_lower or "top" in text_lower)):
                return ("SELECT authors.name AS top_contributor, COUNT(commits.commit_id) AS commit_count FROM commits JOIN authors ON commits.author_id = authors.author_id WHERE commits.timestamp >= CURRENT_DATE - INTERVAL 30 DAY GROUP BY authors.author_id, authors.name ORDER BY commit_count DESC LIMIT 1;")

            # Count commits
            if (("how many" in text_lower or "count" in text_lower) and "commit" in text_lower):
                where = f"WHERE repo_id = {repo_id} " if repo_id is not None else ""
                return ("SELECT COUNT(*) AS total_commits FROM commits " + where + ";")

            # Count contributors
            if (("how many" in text_lower or "count" in text_lower) and any(w in text_lower for w in ["contributor", "author", "developer"])):
                if repo_id is not None:
                    return (f"SELECT COUNT(DISTINCT commits.author_id) AS contributors FROM commits WHERE commits.repo_id = {repo_id};")
                return ("SELECT COUNT(DISTINCT authors.author_id) AS contributors FROM authors;")

            # Commit with most changes
            if ("which commit" in text_lower and ("most" in text_lower and "change" in text_lower)):
                where = f"WHERE commits.repo_id = {repo_id} " if repo_id is not None else ""
                return ("SELECT commits.hash AS top_commit, SUM(diffs.lines_added + diffs.lines_deleted) AS total_changes FROM commits JOIN diffs ON commits.commit_id = diffs.commit_id " + where + "GROUP BY commits.commit_id, commits.hash ORDER BY total_changes DESC LIMIT 1;")

            # Developer who fixed the most bugs
            if (any(w in text_lower for w in ["developer", "author", "contributor"]) and ("fix" in text_lower or "fixed" in text_lower) and "bug" in text_lower):
                base = ("SELECT authors.name AS top_fixer, COUNT(bugs.bug_id) AS bugs_fixed FROM bugs JOIN commits ON bugs.fixed_commit = commits.commit_id JOIN authors ON commits.author_id = authors.author_id ")
                where = f"WHERE commits.repo_id = {repo_id} " if repo_id is not None else ""
                return (base + where + "GROUP BY authors.author_id, authors.name ORDER BY bugs_fixed DESC LIMIT 1;")

            # Repository with most changes
            if ("repository" in text_lower and "most" in text_lower and ("change" in text_lower or "changes" in text_lower)):
                return ("SELECT repositories.name AS top_repository, SUM(diffs.lines_added + diffs.lines_deleted) AS total_changes FROM repositories JOIN commits ON repositories.repo_id = commits.repo_id JOIN diffs ON commits.commit_id = diffs.commit_id GROUP BY repositories.repo_id, repositories.name ORDER BY total_changes DESC LIMIT 1;")

            # Count commits per repository
            if "count commits per repository" in text_lower:
                return ("SELECT repositories.name, COUNT(commits.commit_id) as commit_count FROM repositories LEFT JOIN commits ON repositories.repo_id = commits.repo_id GROUP BY repositories.repo_id, repositories.name ORDER BY commit_count DESC;")
                
            # Extract components
            action = self.identify_action(tokens)
            table = self.identify_table(tokens)
            if not table:
                return "Error: Could not identify the table"
                
            columns = self.identify_columns(tokens, table)
            conditions = self.identify_conditions(text)
            
            # Check for aggregations
            needs_group_by = any(word in text_lower for word in ["per", "group by", "count by"])
            
            # Build the query
            query = f"{action} {', '.join(columns)} FROM {table}"
            
            # Add joins if needed (basic, schema-aware)
            if "author" in text_lower and table == "commits":
                query += " JOIN authors ON commits.author_id = authors.author_id"
            if "repository" in text_lower and table == "commits":
                query += " JOIN repositories ON commits.repo_id = repositories.repo_id"
            
            # Add WHERE clause if conditions exist
            if conditions:
                query += f" WHERE {' AND '.join(conditions)}"
            
            # Add GROUP BY if needed
            if needs_group_by:
                if table == "Repository":
                    query += " GROUP BY Repository.repo_id, Repository.name"
                elif table == "Author":
                    query += " GROUP BY Author.author_id, Author.name"
            
            # Add ORDER BY for certain keywords
            if any(word in text_lower for word in ["top", "most", "highest"]):
                if "commit" in text_lower:
                    query += " ORDER BY commit_count DESC"
                elif "stars" in text_lower:
                    query += " ORDER BY stars DESC"
                elif "forks" in text_lower:
                    query += " ORDER BY forks DESC"
            
            return query + ";"
            
        except Exception as e:
            print(f"Error building SQL query: {str(e)}")
            return "Error: Could not generate SQL query"

def main():
    """Main function with error handling"""
    try:
        analyzer = GitHubQueryAnalyzer()
        
        print("GitHub Query Analyzer")
        print("--------------------")
        print("Enter your natural language queries to get SQL. Type 'exit' to quit.")
        print("\nExample queries:")
        print("- Show repositories with more than 1000 stars")
        print("- List repositories created this month")
        print("- Find repositories with more than 10 forks")
        print("- Display repositories created this month with more than 100 stars")
        
        while True:
            try:
                print("\n" + "="*50)
                user_input = input("\nEnter your query (or 'exit' to quit): ")
                
                if user_input.lower() == 'exit':
                    print("\nThank you for using GitHub Query Analyzer!")
                    break
                    
                sql = analyzer.build_sql_query(user_input)
                print(f"\nGenerated SQL:")
                print(f"{sql}")
                
            except Exception as e:
                print(f"Error processing query: {str(e)}")
                
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())
