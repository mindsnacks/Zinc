import fnmatch
from typing import List


class Match:
    ACCEPT = 1
    REJECT = 2
    UNKNOWN = 3


class PathFilter(object):

    class Rule(object):
        def __init__(self, pattern: str, match_action: Match):
            assert match_action in (Match.ACCEPT, Match.REJECT)
            self.pattern = pattern
            self.match_action = match_action

        def match(self, path: str):
            if fnmatch.fnmatch(path, self.pattern):
                return self.match_action
            return Match.UNKNOWN

    def __init__(self, rules: List[Rule]):
        self._rules = rules

    def match(self, path: str):
        """Tests the path against all rules in this filter"""
        for rule in self._rules:
            if rule.match(path) == Match.ACCEPT:
                return True
            elif rule.match(path) == Match.REJECT:
                return False
        return True

    @staticmethod
    def from_rule_list(rule_list):
        """Read from a dict. `version` is ignored"""
        rules = []
        for rule_string in rule_list:
            rule_string = rule_string.strip()
            rule_comps = rule_string.split()
            match_action_string = rule_comps[0]
            if match_action_string == '+':
                match_action = Match.ACCEPT
            elif match_action_string == '-':
                match_action = Match.REJECT
            else:
                raise ValueError("unknown match type: %s" %
                                 (match_action_string))
            pattern = ' '.join(rule_comps[1:])
            rules.append(PathFilter.Rule(pattern, match_action))
        return PathFilter(rules)
