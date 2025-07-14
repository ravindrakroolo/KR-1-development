import difflib
from typing import List

class SpellCorrector:
    """Intelligent spell correction for search queries using business/tech vocabulary"""
    
    def __init__(self):
        # Common business and technical terms for better suggestions
        self.common_words = [
            'project', 'document', 'meeting', 'presentation', 'report', 'analysis',
            'dashboard', 'metrics', 'data', 'api', 'documentation', 'specification',
            'requirements', 'design', 'architecture', 'development', 'testing',
            'deployment', 'infrastructure', 'security', 'compliance', 'audit',
            'budget', 'timeline', 'milestone', 'deliverable', 'stakeholder',
            'client', 'customer', 'vendor', 'partner', 'team', 'resource',
            'strategy', 'planning', 'roadmap', 'vision', 'mission', 'goals',
            'objectives', 'kpi', 'roi', 'revenue', 'cost', 'profit', 'growth',
            'market', 'competition', 'opportunity', 'risk', 'challenge',
            'solution', 'implementation', 'optimization', 'performance',
            'quality', 'efficiency', 'productivity', 'innovation', 'research',
            'proposal', 'contract', 'agreement', 'policy', 'procedure',
            'workflow', 'process', 'automation', 'integration', 'migration',
            'backup', 'recovery', 'monitoring', 'logging', 'debugging',
            'configuration', 'deployment', 'environment', 'production',
            'staging', 'testing', 'development', 'framework', 'library',
            'database', 'server', 'cloud', 'aws', 'azure', 'google',
            'kubernetes', 'docker', 'jenkins', 'git', 'github', 'gitlab',
            'slack', 'jira', 'confluence', 'notion', 'excel', 'powerpoint',
            'word', 'pdf', 'csv', 'json', 'xml', 'html', 'css', 'javascript',
            'python', 'java', 'react', 'angular', 'vue', 'node', 'express',
            'django', 'flask', 'spring', 'hibernate', 'mysql', 'postgresql',
            'mongodb', 'redis', 'elasticsearch', 'kafka', 'rabbitmq'
        ]
        
        # Add variations and common misspellings
        self.word_variations = {
            'proj': ['project', 'projects'],
            'doc': ['document', 'documents', 'documentation'],
            'meet': ['meeting', 'meetings'],
            'pres': ['presentation', 'presentations'],
            'repo': ['report', 'reports', 'repository'],
            'spec': ['specification', 'specifications'],
            'req': ['requirements', 'request'],
            'dev': ['development', 'developer'],
            'prod': ['production', 'product'],
            'env': ['environment', 'environments'],
            'config': ['configuration', 'configurations'],
            'auth': ['authentication', 'authorization'],
            'api': ['apis', 'endpoint', 'endpoints'],
            'db': ['database', 'databases'],
            'ui': ['interface', 'user interface'],
            'ux': ['user experience', 'experience']
        }
    
    def get_suggestions(self, word: str, n: int = 3) -> List[str]:
        """Get spelling suggestions for a single word"""
        if not word or len(word) < 2:
            return []
        
        word_lower = word.lower()
        
        # Check for exact matches in variations
        if word_lower in self.word_variations:
            return self.word_variations[word_lower][:n]
        
        # Find close matches using difflib
        suggestions = []
        
        # Check against common words
        close_matches = difflib.get_close_matches(
            word_lower, self.common_words, n=n, cutoff=0.6
        )
        suggestions.extend(close_matches)
        
        # Check against variation keys
        variation_matches = difflib.get_close_matches(
            word_lower, list(self.word_variations.keys()), n=2, cutoff=0.7
        )
        for match in variation_matches:
            suggestions.extend(self.word_variations[match][:2])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_suggestions = []
        for suggestion in suggestions:
            if suggestion not in seen and suggestion != word_lower:
                seen.add(suggestion)
                unique_suggestions.append(suggestion)
        
        return unique_suggestions[:n]
    
    def get_query_suggestions(self, query: str) -> List[str]:
        """Get suggestions for complete queries"""
        if not query or len(query.strip()) < 2:
            return []
        
        words = query.lower().split()
        suggestions = []
        
        # Single word suggestions
        if len(words) == 1:
            word_suggestions = self.get_suggestions(words[0], 5)
            suggestions.extend(word_suggestions)
        else:
            # Multi-word query suggestions
            for i, word in enumerate(words):
                word_suggestions = self.get_suggestions(word, 2)
                for suggestion in word_suggestions:
                    # Replace the word in the original query
                    new_words = words.copy()
                    new_words[i] = suggestion
                    new_query = ' '.join(new_words)
                    if new_query not in suggestions:
                        suggestions.append(new_query)
        
        return suggestions[:5]