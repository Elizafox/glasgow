import json
import unittest
from unittest.mock import Mock, patch

from packaging.requirements import Requirement

from glasgow.support.plugin import _unmet_requirements_in


class GitPluginRequirementsTestCase(unittest.TestCase):
    requirement = Requirement(
        "Twisted @ git+https://github.com/twisted/twisted.git@"
        "c4001f59f6787b70cb2046c0b99b4195ac2b321a")

    def unmet(self, source):
        distribution = Mock(version="26.4.0")
        distribution.read_text.return_value = json.dumps(source) if source else None
        with patch("glasgow.support.plugin.importlib.metadata.distribution",
                   return_value=distribution):
            return _unmet_requirements_in({self.requirement})

    def source(self, **vcs):
        return {
            "url": "https://github.com/twisted/twisted.git",
            "vcs_info": {"vcs": "git", "commit_id":
                "c4001f59f6787b70cb2046c0b99b4195ac2b321a", **vcs},
        }

    def test_required_commit(self):
        self.assertEqual(self.unmet(self.source()), set())

    def test_release_without_git_provenance(self):
        self.assertEqual(self.unmet(None), {self.requirement})

    def test_other_commit(self):
        self.assertEqual(self.unmet(self.source(commit_id="0" * 40)), {self.requirement})

    def test_other_repository(self):
        source = self.source()
        source["url"] = "https://example.com/twisted.git"
        self.assertEqual(self.unmet(source), {self.requirement})
