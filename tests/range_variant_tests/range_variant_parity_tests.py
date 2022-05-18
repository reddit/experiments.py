import json
import unittest

from unittest import mock

from baseplate import ServerSpan
from baseplate.lib.file_watcher import FileWatcher
from reddit_edgecontext import AuthenticationToken
from reddit_edgecontext import User

from reddit_experiments import Experiments

ORIGINAL_ZK_CONFIG_FILE = "tests/range_variant_tests/data/original_zk_config.json"
RANGE_VARIANT_ZK_CONFIG_FILE = "tests/range_variant_tests/data/range_variant_zk_config.json"
RESULTS_OUTPUT = "tests/range_variant_tests/data/output.json"
NUMBER_OF_TEST_USERS = 1000


class TestExperiments(unittest.TestCase):
    def setUp(self):
        super().setUp()
        with open(ORIGINAL_ZK_CONFIG_FILE, "r") as f:
            self.original_zk_config = json.load(f)
        with open(RANGE_VARIANT_ZK_CONFIG_FILE, "r") as f:
            self.range_variant_zk_config = json.load(f)
        self.mock_filewatcher = mock.Mock(spec=FileWatcher)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.mock_span.context = None
        self.mock_span.trace_id = "123456"
        self.mock_authentication_token = mock.Mock(spec=AuthenticationToken)
        self.mock_authentication_token.user_roles = set()

    def test_range_variant_bucketing_with_cfg_data(self):
        original_experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=self.original_zk_config,
        )

        rv_experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=self.range_variant_zk_config,
        )

        # results = {}
        for experiment_name in self.original_zk_config.keys():
            for i in range(NUMBER_OF_TEST_USERS):
                uuid = "t2_" + str(i)
                self.mock_authentication_token.subject = uuid
                user = User(
                    authentication_token=self.mock_authentication_token,
                    loid=uuid,
                    cookie_created_ms=10000,
                )
                og_variant = original_experiments.variant(
                    experiment_name, user=user, app_name="", build_number=0
                )
                rv_variant = rv_experiments.variant(
                    experiment_name, user=user, app_name="", build_number=0
                )

                if og_variant != rv_variant:
                    print(f"\n\nexperiment: {experiment_name}")
                    print(f"uuid: {uuid}")
                    print(f"original variant: {og_variant}")
                    print(f"rv variant: {rv_variant}")
                self.assertEqual(og_variant, rv_variant)

                # construct experiment/uuid to variant mapping
                # used to compare results across baseplate languages
                # results[experiment_name + ':' + uuid] = og_variant

        # with open(RESULTS_OUTPUT, "w") as f:
        #     json.dump(results, f)
