import difflib
from github_analyzer import GitHubAnalyzer
from typing import List, Dict, Tuple

class AccuracyTester:
    def __init__(self):
        self.analyzer = GitHubAnalyzer()
        # Test cases with known correct SQL outputs
        self.test_cases = [
            # Basic Queries
            {
                "query": "Show repositories with more than 1000 stars",
                "expected_sql": "SELECT repo_id, name, url FROM Repository WHERE Repository.stars >= 1000;"
            },
            {
                "query": "Display all python files in repository",
                "expected_sql": "SELECT file_id, path FROM File WHERE type = '.py';"
            },
            {
                "query": "List all authors",
                "expected_sql": "SELECT author_id, name, email FROM Author;"
            },
            
            # Time-based Queries
            {
                "query": "Find commits by author in the last 30 days",
                "expected_sql": "SELECT commit_id, message, timestamp FROM Commit JOIN Author ON Commit.author_id = Author.author_id WHERE timestamp >= '2025-10-01';"
            },
            {
                "query": "Show bugs fixed in the last week",
                "expected_sql": "SELECT bug_id, description FROM Bug JOIN Commit fix ON Bug.fixed_commit = fix.commit_id WHERE timestamp >= '2025-10-24';"
            },
            {
                "query": "Display repositories created this month",
                "expected_sql": "SELECT repo_id, name, url FROM Repository WHERE created_at >= '2025-10-01';"
            },
            {
                "query": "Show authors who made commits today",
                "expected_sql": "SELECT DISTINCT Author.name FROM Author JOIN Commit ON Author.author_id = Commit.author_id WHERE DATE(timestamp) = '2025-10-31';"
            },
            
            # Aggregation Queries
            {
                "query": "Count total number of files modified in each repository",
                "expected_sql": "SELECT Repository.name, COUNT(File.file_id) as file_count FROM Repository JOIN File ON Repository.repo_id = File.repo_id GROUP BY Repository.name;"
            },
            {
                "query": "Show average number of commits per author",
                "expected_sql": "SELECT Author.name, COUNT(Commit.commit_id) as commit_count FROM Author LEFT JOIN Commit ON Author.author_id = Commit.author_id GROUP BY Author.name;"
            },
            {
                "query": "Calculate total lines of code added and deleted per repository",
                "expected_sql": "SELECT Repository.name, SUM(Diff.lines_added) as total_additions, SUM(Diff.lines_deleted) as total_deletions FROM Repository JOIN File ON Repository.repo_id = File.repo_id JOIN Diff ON File.file_id = Diff.file_id GROUP BY Repository.name;"
            },
            
            # Complex Joins
            {
                "query": "Find authors who fixed the most bugs",
                "expected_sql": "SELECT Author.name, COUNT(Bug.bug_id) as bugs_fixed FROM Author JOIN Commit ON Author.author_id = Commit.author_id JOIN Bug ON Bug.fixed_commit = Commit.commit_id GROUP BY Author.name ORDER BY bugs_fixed DESC;"
            },
            {
                "query": "Show files with the most bug fixes",
                "expected_sql": "SELECT File.path, COUNT(DISTINCT Bug.bug_id) as bug_fixes FROM File JOIN Diff ON File.file_id = Diff.file_id JOIN Commit ON Diff.commit_id = Commit.commit_id JOIN Bug ON Bug.fixed_commit = Commit.commit_id GROUP BY File.path ORDER BY bug_fixes DESC;"
            },
            
            # Status and Metrics
            {
                "query": "List test cases that failed with runtime more than 5 seconds",
                "expected_sql": "SELECT test_id, status, runtime, error_log FROM Test WHERE status = 'failed' AND runtime > 5;"
            },
            {
                "query": "Show commits that introduced bugs",
                "expected_sql": "SELECT Commit.commit_id, Commit.message, Bug.description FROM Commit JOIN Bug ON Bug.introduced_commit = Commit.commit_id;"
            },
            
            # Advanced Analytics
            {
                "query": "Find repositories with highest test success rate",
                "expected_sql": "SELECT Repository.name, COUNT(CASE WHEN Test.status = 'passed' THEN 1 END) * 100.0 / COUNT(*) as success_rate FROM Repository JOIN Commit ON Repository.repo_id = Commit.repo_id JOIN Test ON Test.commit_id = Commit.commit_id GROUP BY Repository.name ORDER BY success_rate DESC;"
            },
            {
                "query": "Show authors with most consistent commit patterns",
                "expected_sql": "SELECT Author.name, COUNT(DISTINCT DATE(Commit.timestamp)) as days_with_commits FROM Author JOIN Commit ON Author.author_id = Commit.author_id GROUP BY Author.name ORDER BY days_with_commits DESC;"
            },
            
            # Comparative Queries
            {
                "query": "Find repositories with more forks than stars",
                "expected_sql": "SELECT repo_id, name, forks, stars FROM Repository WHERE forks > stars;"
            },
            {
                "query": "Show files that have more deletions than additions",
                "expected_sql": "SELECT File.path, Diff.lines_added, Diff.lines_deleted FROM File JOIN Diff ON File.file_id = Diff.file_id WHERE Diff.lines_deleted > Diff.lines_added;"
            },
            
            # Status Tracking
            {
                "query": "Count bugs by their current status",
                "expected_sql": "SELECT CASE WHEN fixed_commit IS NULL THEN 'open' ELSE 'fixed' END as status, COUNT(*) as count FROM Bug GROUP BY status;"
            },
            {
                "query": "Show test failure rate by file type",
                "expected_sql": "SELECT File.type, COUNT(CASE WHEN Test.status = 'failed' THEN 1 END) * 100.0 / COUNT(*) as failure_rate FROM File JOIN Diff ON File.file_id = Diff.file_id JOIN Commit ON Diff.commit_id = Commit.commit_id JOIN Test ON Test.commit_id = Commit.commit_id GROUP BY File.type ORDER BY failure_rate DESC;"
            }
        ]

    def normalize_sql(self, sql: str) -> str:
        """Normalize SQL query for comparison by removing extra spaces and converting to lowercase"""
        return ' '.join(sql.lower().split())

    def calculate_similarity(self, sql1: str, sql2: str) -> float:
        """Calculate similarity between two SQL queries using difflib"""
        sql1 = self.normalize_sql(sql1)
        sql2 = self.normalize_sql(sql2)
        
        # Use difflib's SequenceMatcher to get similarity ratio
        similarity = difflib.SequenceMatcher(None, sql1, sql2).ratio()
        return similarity

    def test_accuracy(self) -> Dict:
        """Test the accuracy of the NLP to SQL conversion"""
        total_cases = len(self.test_cases)
        exact_matches = 0
        total_similarity = 0
        results = []

        for test_case in self.test_cases:
            query = test_case["query"]
            expected_sql = test_case["expected_sql"]
            
            # Generate SQL using our analyzer
            parsed_data = self.analyzer.parse_query(query)
            generated_sql = self.analyzer.build_sql_query(parsed_data)
            
            # Calculate similarity
            similarity = self.calculate_similarity(generated_sql, expected_sql)
            total_similarity += similarity
            
            # Check for exact match (with normalization)
            if self.normalize_sql(generated_sql) == self.normalize_sql(expected_sql):
                exact_matches += 1
            
            results.append({
                "query": query,
                "expected": expected_sql,
                "generated": generated_sql,
                "similarity": similarity
            })

        # Calculate metrics
        accuracy = exact_matches / total_cases * 100
        average_similarity = total_similarity / total_cases * 100

        return {
            "total_cases": total_cases,
            "exact_matches": exact_matches,
            "accuracy_percentage": accuracy,
            "average_similarity": average_similarity,
            "detailed_results": results
        }

def main():
    tester = AccuracyTester()
    results = tester.test_accuracy()
    
    print("NLP to SQL Accuracy Test Results")
    print("-" * 50)
    print(f"Total test cases: {results['total_cases']}")
    print(f"Exact matches: {results['exact_matches']}")
    print(f"Accuracy: {results['accuracy_percentage']:.2f}%")
    print(f"Average similarity: {results['average_similarity']:.2f}%")
    
    print("\nDetailed Results:")
    print("-" * 50)
    for idx, result in enumerate(results['detailed_results'], 1):
        print(f"\nTest Case {idx}:")
        print(f"Query: {result['query']}")
        print(f"Expected: {result['expected']}")
        print(f"Generated: {result['generated']}")
        print(f"Similarity: {result['similarity']*100:.2f}%")

if __name__ == "__main__":
    main()