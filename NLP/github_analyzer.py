import spacy
import datetime
from typing import Dict, List, Optional, Tuple
import re

class GitHubQueryParser:
    def __init__(self):
        # Load spaCy model
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            # If model not found, download it
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
            self.nlp = spacy.load("en_core_web_sm")

        # Define intent patterns
        self.intent_patterns = {
            'SELECT': ['show', 'find', 'display', 'list', 'get', 'what', 'which'],
            'COUNT': ['count', 'how many', 'total number'],
            'UPDATE': ['update', 'modify', 'change'],
            'DELETE': ['delete', 'remove'],
            'INSERT': ['add', 'insert', 'create']
        }

        # Define entity mappings
        self.entity_mappings = {
            'repository': {
                'patterns': ['repo', 'repository', 'repositories', 'project'],
                'table': 'repositories',
                'columns': {
                    'default': ['id', 'name', 'full_name', 'description'],
                    'stats': ['stargazers_count', 'forks_count', 'watchers_count'],
                    'details': ['language', 'created_at', 'updated_at', 'archived']
                }
            },
            'commit': {
                'patterns': ['commit', 'commits', 'change', 'changes'],
                'table': 'commits',
                'columns': {
                    'default': ['id', 'sha', 'message'],
                    'stats': ['additions', 'deletions', 'total'],
                    'details': ['author_id', 'committer_id', 'created_at']
                }
            },
            'issue': {
                'patterns': ['issue', 'issues', 'bug', 'bugs', 'ticket'],
                'table': 'issues',
                'columns': {
                    'default': ['id', 'number', 'title', 'state'],
                    'stats': ['comments_count', 'reactions_count'],
                    'details': ['body', 'created_at', 'updated_at', 'closed_at']
                }
            },
            'pull_request': {
                'patterns': ['pr', 'pull request', 'merge request'],
                'table': 'pull_requests',
                'columns': {
                    'default': ['id', 'number', 'title', 'state'],
                    'stats': ['comments_count', 'commits_count'],
                    'details': ['body', 'merged_at', 'created_at']
                }
            }
        }

        # Time-related patterns
        self.time_patterns = {
            'today': r'today',
            'yesterday': r'yesterday',
            'this week': r'this week',
            'last week': r'last week',
            'this month': r'this month',
            'last month': r'last month',
            'this year': r'this year',
            'last year': r'last year',
            'days': r'(\d+)\s*days?(?:\s*ago)?',
            'weeks': r'(\d+)\s*weeks?(?:\s*ago)?',
            'months': r'(\d+)\s*months?(?:\s*ago)?',
            'years': r'(\d+)\s*years?(?:\s*ago)?'
        }

        # Comparison patterns
        self.comparison_patterns = {
            'greater': r'(more|greater|higher|over|above) than (\d+)',
            'less': r'(less|fewer|lower|under|below) than (\d+)',
            'equal': r'(equal|equals|exactly) (\d+)',
            'between': r'between (\d+) and (\d+)'
        }

    def parse_query(self, query: str) -> Dict:
        """Main method to parse the English query into structured data"""
        # Process the query with spaCy
        doc = self.nlp(query.lower())
        
        # Extract components
        intent = self.detect_intent(doc)
        entity = self.detect_entity(doc)
        filters = self.detect_filters(doc)
        columns = self.detect_columns(doc, entity)
        
        return {
            'intent': intent,
            'entity': entity,
            'columns': columns,
            'filters': filters
        }

    def detect_intent(self, doc) -> str:
        """Detect the SQL intent (SELECT, COUNT, etc.)"""
        text = doc.text.lower()
        
        # Check for COUNT intent first
        for pattern in self.intent_patterns['COUNT']:
            if pattern in text:
                return 'COUNT'
        
        # Check other intents
        for intent, patterns in self.intent_patterns.items():
            for pattern in patterns:
                if pattern in text:
                    return intent
        
        return 'SELECT'  # Default intent

    def detect_entity(self, doc) -> Optional[str]:
        """Detect the main entity (table) the query is about"""
        text = doc.text.lower()
        
        for entity, info in self.entity_mappings.items():
            for pattern in info['patterns']:
                if pattern in text:
                    return entity
        
        return None

    def detect_columns(self, doc, entity: str) -> List[str]:
        """Detect which columns should be returned"""
        text = doc.text.lower()
        
        if not entity or entity not in self.entity_mappings:
            return ['*']
            
        entity_info = self.entity_mappings[entity]
        
        # Check for statistics request
        if any(word in text for word in ['count', 'number', 'stats', 'statistics']):
            return entity_info['columns']['stats']
            
        # Check for detailed information request
        if any(word in text for word in ['details', 'information', 'full', 'all']):
            return entity_info['columns']['details']
            
        return entity_info['columns']['default']

    def detect_filters(self, doc) -> List[Dict]:
        """Detect filter conditions from the query"""
        filters = []
        text = doc.text.lower()
        
        # Detect time-based filters
        for time_key, pattern in self.time_patterns.items():
            matches = re.search(pattern, text)
            if matches:
                filter_dict = self.create_time_filter(time_key, matches)
                if filter_dict:
                    filters.append(filter_dict)
        
        # Detect comparison filters
        for comp_key, pattern in self.comparison_patterns.items():
            matches = re.search(pattern, text)
            if matches:
                filter_dict = self.create_comparison_filter(comp_key, matches)
                if filter_dict:
                    filters.append(filter_dict)
        
        # Detect state/status filters
        state_match = re.search(r'(open|closed|merged|pending)', text)
        if state_match:
            filters.append({
                'column': 'state',
                'operator': '=',
                'value': state_match.group(1)
            })
        
        return filters

    def create_time_filter(self, time_key: str, matches) -> Optional[Dict]:
        """Create a time-based filter"""
        now = datetime.datetime.now()
        
        if time_key == 'today':
            date = now.date()
        elif time_key == 'yesterday':
            date = (now - datetime.timedelta(days=1)).date()
        elif time_key == 'this week':
            date = (now - datetime.timedelta(days=now.weekday())).date()
        elif time_key == 'last week':
            date = (now - datetime.timedelta(days=now.weekday() + 7)).date()
        elif time_key == 'this month':
            date = now.replace(day=1).date()
        elif time_key == 'last month':
            last_month = now.replace(day=1) - datetime.timedelta(days=1)
            date = last_month.replace(day=1).date()
        else:
            # Handle numeric time periods
            if matches and matches.group(1):
                number = int(matches.group(1))
                if time_key == 'days':
                    date = (now - datetime.timedelta(days=number)).date()
                elif time_key == 'weeks':
                    date = (now - datetime.timedelta(weeks=number)).date()
                elif time_key == 'months':
                    date = (now - datetime.timedelta(days=number * 30)).date()
                elif time_key == 'years':
                    date = (now - datetime.timedelta(days=number * 365)).date()
                else:
                    return None
            else:
                return None
                
        return {
            'column': 'created_at',
            'operator': '>=',
            'value': date.isoformat()
        }

    def create_comparison_filter(self, comp_key: str, matches) -> Optional[Dict]:
        """Create a comparison filter"""
        if not matches:
            return None
            
        if comp_key == 'between':
            return {
                'column': 'count',
                'operator': 'BETWEEN',
                'value': (int(matches.group(1)), int(matches.group(2)))
            }
        else:
            value = int(matches.group(2))
            operator = '>=' if comp_key == 'greater' else '<=' if comp_key == 'less' else '='
            return {
                'column': 'count',
                'operator': operator,
                'value': value
            }

    def build_sql_query(self, parsed_data: Dict) -> str:
        """Build the final SQL query from parsed components"""
        intent = parsed_data['intent']
        entity = parsed_data['entity']
        columns = parsed_data['columns']
        filters = parsed_data['filters']
        
        if not entity:
            return "Could not determine the table to query"
            
        table = self.entity_mappings[entity]['table']
        
        # Start building the query
        if intent == 'COUNT':
            query = f"SELECT COUNT(*) FROM {table}"
        else:
            columns_str = ', '.join(columns)
            query = f"SELECT {columns_str} FROM {table}"
        
        # Add WHERE clause if there are filters
        if filters:
            where_conditions = []
            for filter_dict in filters:
                column = filter_dict['column']
                operator = filter_dict['operator']
                value = filter_dict['value']
                
                if operator == 'BETWEEN':
                    where_conditions.append(f"{column} BETWEEN {value[0]} AND {value[1]}")
                elif isinstance(value, str) and not value.isdigit():
                    where_conditions.append(f"{column} {operator} '{value}'")
                else:
                    where_conditions.append(f"{column} {operator} {value}")
                    
            query += " WHERE " + " AND ".join(where_conditions)
        
        return query + ";"

def main():
    parser = GitHubQueryParser()
    
    # Test queries
    test_queries = [
        "Show all repositories with more than 1000 stars",
        "Count open issues from last week",
        "Find pull requests created in the last 30 days",
        "Show commits by author in this month",
        "Display repositories with most forks",
        "List open pull requests with more than 5 comments",
        "Show issues created today",
        "Count total commits in the last 24 hours",
        "Find repositories updated in the last month",
        "Show pull requests in pending state"
    ]
    
    print("GitHub Query Parser")
    print("-" * 50)
    for query in test_queries:
        print(f"\nEnglish Query: {query}")
        parsed_data = parser.parse_query(query)
        sql_query = parser.build_sql_query(parsed_data)
        print(f"SQL Query: {sql_query}")
    
    print("\nInteractive Mode (type 'exit' to quit)")
    print("-" * 50)
    while True:
        user_input = input("\nEnter your query: ")
        if user_input.lower() == 'exit':
            break
        parsed_data = parser.parse_query(user_input)
        sql_query = parser.build_sql_query(parsed_data)
        print(f"SQL Query: {sql_query}")

if __name__ == "__main__":
    main()
