import contextlib
import json
import tempfile
import unittest

from unittest import mock

from baseplate import RequestContext
from baseplate import ServerSpan
from baseplate.lib.events import DebugLogger
from baseplate.lib.file_watcher import FileWatcher
from reddit_edgecontext import ValidatedAuthenticationToken

from reddit_decider import Decider
from reddit_decider import decider_client_from_config
from reddit_decider import DeciderContext
from reddit_decider import DeciderContextFactory
from reddit_decider import EventType
from reddit_decider import init_decider_parser

USER_ID = "t2_1234"
IS_LOGGED_IN = True
AUTH_CLIENT_ID = "token"
COUNTRY_CODE = "US"
DEVICE_ID = "abc"
COOKIE_CREATED_TIMESTAMP = 1234
LOCALE_CODE = "us_en"
ORIGIN_SERVICE = "origin"
APP_NAME = "ios"
APP_VERSION = "0.0.0.0"
BUILD_NUMBER = 1
CANONICAL_URL = "www.test.com"
EVENT_FIELDS = {
    "user_id": USER_ID,
    "logged_in": IS_LOGGED_IN,
    "cookie_created_timestamp": COOKIE_CREATED_TIMESTAMP,
}


@contextlib.contextmanager
def create_temp_config_file(contents):
    with tempfile.NamedTemporaryFile() as f:
        f.write(json.dumps(contents).encode())
        f.seek(0)
        yield f


def decider_field_extractor(_request: RequestContext):
    return {
        "app_name": APP_NAME,
        "app_version": APP_VERSION,
        "build_number": BUILD_NUMBER,
        "canonical_url": CANONICAL_URL,
    }


@mock.patch("reddit_decider.FileWatcher")
class DeciderClientFromConfigTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.event_logger = mock.Mock(spec=DebugLogger)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.mock_span.context = None

    def test_make_clients(self, file_watcher_mock):
        decider_ctx_factory = decider_client_from_config(
            {"experiments.path": "/tmp/test"}, self.event_logger
        )
        self.assertIsInstance(decider_ctx_factory, DeciderContextFactory)
        file_watcher_mock.assert_called_once_with(
            path="/tmp/test", parser=init_decider_parser, timeout=None, backoff=None
        )

    def test_timeout(self, file_watcher_mock):
        decider_ctx_factory = decider_client_from_config(
            {"experiments.path": "/tmp/test", "experiments.timeout": "60 seconds"},
            self.event_logger,
        )
        self.assertIsInstance(decider_ctx_factory, DeciderContextFactory)
        file_watcher_mock.assert_called_once_with(
            path="/tmp/test", parser=init_decider_parser, timeout=60.0, backoff=None
        )

    def test_prefix(self, file_watcher_mock):
        decider_ctx_factory = decider_client_from_config(
            {"r2_experiments.path": "/tmp/test", "r2_experiments.timeout": "60 seconds"},
            self.event_logger,
            prefix="r2_experiments.",
        )
        self.assertIsInstance(decider_ctx_factory, DeciderContextFactory)
        file_watcher_mock.assert_called_once_with(
            path="/tmp/test", parser=init_decider_parser, timeout=60.0, backoff=None
        )


@mock.patch("reddit_decider.FileWatcher")
class DeciderContextFactoryTests(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.event_logger = mock.Mock(spec=DebugLogger)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.mock_span.context = mock.Mock()
        self.mock_span.context.edge_context.user.event_fields = mock.Mock(return_value=EVENT_FIELDS)
        self.mock_span.context.edge_context.authentication_token = mock.Mock(
            spec=ValidatedAuthenticationToken
        )
        self.mock_span.context.edge_context.authentication_token.oauth_client_id = AUTH_CLIENT_ID
        self.mock_span.context.edge_context.geolocation.country_code = COUNTRY_CODE
        self.mock_span.context.edge_context.locale.locale_code = LOCALE_CODE
        self.mock_span.context.edge_context.origin_service.name = ORIGIN_SERVICE
        self.mock_span.context.edge_context.device.id = DEVICE_ID

    def test_make_object_for_context_and_decider_context(self, _filewatcher):
        decider_ctx_factory = decider_client_from_config(
            {"experiments.path": "/tmp/test", "experiments.timeout": "60 seconds"},
            self.event_logger,
            prefix="experiments.",
            request_field_extractor=decider_field_extractor,
        )
        decider = decider_ctx_factory.make_object_for_context(name="test", span=self.mock_span)
        self.assertIsInstance(decider, Decider)

        decider_context = getattr(decider, "_decider_context")
        self.assertIsInstance(decider_context, DeciderContext)

        decider_ctx_dict = decider_context.to_dict()
        self.assertEqual(decider_ctx_dict["user_id"], USER_ID)
        self.assertEqual(decider_ctx_dict["country_code"], COUNTRY_CODE)
        self.assertEqual(decider_ctx_dict["user_is_employee"], True)
        self.assertEqual(decider_ctx_dict["logged_in"], IS_LOGGED_IN)
        self.assertEqual(decider_ctx_dict["device_id"], DEVICE_ID)
        self.assertEqual(decider_ctx_dict["locale"], LOCALE_CODE)
        self.assertEqual(decider_ctx_dict["origin_service"], ORIGIN_SERVICE)
        self.assertEqual(decider_ctx_dict["auth_client_id"], AUTH_CLIENT_ID)
        self.assertEqual(
            decider_ctx_dict["cookie_created_timestamp"],
            self.mock_span.context.edge_context.user.event_fields().get("cookie_created_timestamp"),
        )
        self.assertEqual(decider_ctx_dict["app_name"], APP_NAME)
        self.assertEqual(decider_ctx_dict["other_fields"]["app_name"], APP_NAME)
        self.assertEqual(decider_ctx_dict["app_version"], APP_VERSION)
        self.assertEqual(decider_ctx_dict["other_fields"]["app_version"], APP_VERSION)
        self.assertEqual(decider_ctx_dict["build_number"], BUILD_NUMBER)
        self.assertEqual(decider_ctx_dict["other_fields"]["build_number"], BUILD_NUMBER)
        self.assertEqual(decider_ctx_dict["canonical_url"], CANONICAL_URL)
        self.assertEqual(decider_ctx_dict["other_fields"]["canonical_url"], CANONICAL_URL)

        decider_event_dict = decider_context.to_event_dict()
        self.assertEqual(decider_event_dict["user_id"], USER_ID)
        self.assertEqual(decider_event_dict["user"]["id"], USER_ID)
        self.assertEqual(decider_event_dict["country_code"], COUNTRY_CODE)
        self.assertEqual(decider_event_dict["geo"]["country_code"], COUNTRY_CODE)
        self.assertEqual(decider_event_dict["user_is_employee"], True)
        self.assertEqual(decider_event_dict["user"]["is_employee"], True)
        self.assertEqual(decider_event_dict["logged_in"], IS_LOGGED_IN)
        self.assertEqual(decider_event_dict["user"]["logged_in"], IS_LOGGED_IN)

        self.assertEqual(decider_event_dict["device_id"], DEVICE_ID)
        self.assertEqual(decider_event_dict["platform"]["device_id"], DEVICE_ID)
        self.assertEqual(decider_event_dict["locale"], LOCALE_CODE)
        self.assertEqual(decider_event_dict["app"]["relevant_locale"], LOCALE_CODE)
        self.assertEqual(decider_event_dict["origin_service"], ORIGIN_SERVICE)
        self.assertEqual(decider_event_dict.get("auth_client_id"), None)
        self.assertEqual(
            decider_event_dict["cookie_created_timestamp"],
            self.mock_span.context.edge_context.user.event_fields().get("cookie_created_timestamp"),
        )
        self.assertEqual(
            decider_event_dict["user"]["cookie_created_timestamp"],
            self.mock_span.context.edge_context.user.event_fields().get("cookie_created_timestamp"),
        )
        self.assertEqual(decider_event_dict["app_name"], APP_NAME)
        self.assertEqual(decider_event_dict["app"]["name"], APP_NAME)
        self.assertEqual(decider_event_dict["app_version"], APP_VERSION)
        self.assertEqual(decider_event_dict["app"]["version"], APP_VERSION)
        self.assertEqual(decider_event_dict["build_number"], BUILD_NUMBER)
        self.assertEqual(decider_event_dict["app"]["build_number"], BUILD_NUMBER)
        self.assertEqual(decider_event_dict["canonical_url"], CANONICAL_URL)
        self.assertEqual(decider_event_dict["request"]["canonical_url"], CANONICAL_URL)

    def test_make_object_for_context_and_decider_context_without_span(self, _filewatcher):
        decider_ctx_factory = decider_client_from_config(
            {"experiments.path": "/tmp/test", "experiments.timeout": "60 seconds"},
            self.event_logger,
            prefix="experiments.",
            request_field_extractor=decider_field_extractor,
        )
        decider = decider_ctx_factory.make_object_for_context(name="test", span=None)
        self.assertIsInstance(decider, Decider)

        decider_ctx_dict = decider._decider_context.to_dict()
        self.assertEqual(decider_ctx_dict["user_id"], None)

    def test_make_object_for_context_and_decider_context_with_broken_decider_field_extractor(
        self, _filewatcher
    ):
        def broken_decider_field_extractor(_request: RequestContext):
            return {
                "app_name": {},
                "build_number": BUILD_NUMBER,
                "canonical_url": CANONICAL_URL,
                True: "bool",
                None: "xyz",
            }

        decider_ctx_factory = decider_client_from_config(
            {"experiments.path": "/tmp/test", "experiments.timeout": "60 seconds"},
            self.event_logger,
            prefix="experiments.",
            request_field_extractor=broken_decider_field_extractor,
        )

        with self.assertLogs() as captured:
            decider_ctx_factory.make_object_for_context(name="test", span=self.mock_span)

            assert any(
                "None key in request_field_extractor() dict is not of type str and is removed."
                in x.getMessage()
                for x in captured.records
            )
            assert any(
                "True key in request_field_extractor() dict is not of type str and is removed."
                in x.getMessage()
                for x in captured.records
            )
            assert any(
                "app_name: {} value in `request_field_extractor()` dict is not one of type: [None, int, float, str, bool] and is removed."
                in x.getMessage()
                for x in captured.records
            )


# Todo: test DeciderClient()
# @mock.patch("reddit_decider.FileWatcher")
# class DeciderClientTests(unittest.TestCase):


class TestDeciderGetVariantAndExpose(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.event_logger = mock.Mock(spec=DebugLogger)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.mock_span.context = None
        self.minimal_decider_context = DeciderContext()
        self.exp_base_config = {
            "exp_1": {
                "id": 1,
                "name": "exp_1",
                "enabled": True,
                "version": "2",
                "type": "range_variant",
                "emit_event": True,
                "start_ts": 37173982,
                "stop_ts": 2147483648,
                "owner": "test_owner",
                "experiment": {
                    "variants": [
                        {"range_start": 0.0, "range_end": 0.2, "name": "control_1"},
                        {"range_start": 0.2, "range_end": 0.4, "name": "control_2"},
                        {"range_start": 0.4, "range_end": 0.6, "name": "variant_2"},
                        {"range_start": 0.6, "range_end": 0.8, "name": "variant_3"},
                        {"range_start": 0.8, "range_end": 1.0, "name": "variant_4"},
                    ],
                    "experiment_version": 2,
                    "shuffle_version": 0,
                    "bucket_val": "user_id",
                    "log_bucketing": False,
                },
            }
        }

        self.parent_hg_config = {
            "hg": {
                "enabled": True,
                "version": "5",
                "type": "range_variant",
                "emit_event": True,
                "experiment": {
                    "variants": [
                        {"name": "holdout", "size": 1.0, "range_end": 1.0, "range_start": 0.0},
                        {"name": "control_1", "size": 0.0, "range_end": 0.0, "range_start": 0.0},
                    ],
                    "experiment_version": 5,
                    "shuffle_version": 0,
                    "bucket_val": "user_id",
                    "log_bucketing": False,
                },
                "start_ts": 0,
                "stop_ts": 9668199193,
                "id": 2,
                "name": "hg",
                "owner": "test",
                "value": "range_variant",
            }
        }

        self.additional_two_exp = {
            "e1": {
                "enabled": True,
                "version": "4",
                "type": "range_variant",
                "owner": "test",
                "emit_event": True,
                "experiment": {
                    "variants": [
                        {"name": "e1treat", "size": 1.0, "range_end": 1.0, "range_start": 0.0},
                        {"name": "control_1", "size": 0.0, "range_end": 0.0, "range_start": 0.0},
                    ],
                    "experiment_version": 4,
                    "shuffle_version": 0,
                    "bucket_val": "user_id",
                },
                "start_ts": 0,
                "stop_ts": 9668199193,
                "id": 6,
                "name": "e1",
            },
            "e2": {
                "enabled": True,
                "version": "5",
                "type": "range_variant",
                "owner": "test",
                "emit_event": True,
                "experiment": {
                    "variants": [
                        {"name": "e2treat", "size": 1.0, "range_end": 1.0, "range_start": 0.0},
                        {"name": "control_1", "size": 0.0, "range_end": 0.0, "range_start": 0.0},
                    ],
                    "experiment_version": 5,
                    "shuffle_version": 0,
                    "bucket_val": "user_id",
                },
                "start_ts": 0,
                "stop_ts": 9668199193,
                "id": 7,
                "name": "e2",
            },
        }

        self.dc = DeciderContext(
            user_id=USER_ID,
            logged_in=IS_LOGGED_IN,
            country_code=COUNTRY_CODE,
            locale=LOCALE_CODE,
            origin_service=ORIGIN_SERVICE,
            user_is_employee=True,
            device_id=DEVICE_ID,
            auth_client_id=AUTH_CLIENT_ID,
            cookie_created_timestamp=COOKIE_CREATED_TIMESTAMP,
            extracted_fields=decider_field_extractor(_request=None),
        )

    def assert_exposure_event_fields(
        self,
        experiment_name: str,
        variant: str,
        event_fields: dict,
        bucket_val: str = "user_id",
        identifier: str = USER_ID,
    ):
        self.assertEqual(event_fields["variant"], variant)
        self.assertEqual(event_fields[bucket_val], identifier)
        self.assertEqual(event_fields["logged_in"], IS_LOGGED_IN)
        self.assertEqual(event_fields["app_name"], APP_NAME)
        self.assertEqual(event_fields["build_number"], BUILD_NUMBER)
        self.assertEqual(event_fields["app_version"], APP_VERSION)
        self.assertEqual(event_fields["canonical_url"], CANONICAL_URL)
        self.assertEqual(event_fields["cookie_created_timestamp"], COOKIE_CREATED_TIMESTAMP)
        self.assertEqual(event_fields["event_type"], EventType.EXPOSE)
        self.assertNotEqual(event_fields["span"], None)

        cfg = self.exp_base_config[experiment_name]
        self.assertEqual(getattr(event_fields["experiment"], "id"), cfg["id"])
        self.assertEqual(getattr(event_fields["experiment"], "name"), cfg["name"])
        self.assertEqual(getattr(event_fields["experiment"], "owner"), cfg["owner"])
        self.assertEqual(getattr(event_fields["experiment"], "version"), cfg["version"])
        self.assertEqual(getattr(event_fields["experiment"], "bucket_val"), bucket_val)

    def assert_minimal_exposure_event_fields(
        self,
        experiment_name: str,
        variant: str,
        event_fields: dict,
        bucket_val: str = "user_id",
        identifier: str = USER_ID,
    ):
        self.assertEqual(event_fields["variant"], variant)
        self.assertEqual(event_fields[bucket_val], identifier)
        self.assertEqual(event_fields["event_type"], EventType.EXPOSE)
        self.assertNotEqual(event_fields["span"], None)

        cfg = self.exp_base_config[experiment_name]
        self.assertEqual(getattr(event_fields["experiment"], "id"), cfg["id"])
        self.assertEqual(getattr(event_fields["experiment"], "name"), cfg["name"])
        self.assertEqual(getattr(event_fields["experiment"], "owner"), cfg["owner"])
        self.assertEqual(getattr(event_fields["experiment"], "version"), cfg["version"])
        self.assertEqual(getattr(event_fields["experiment"], "bucket_val"), bucket_val)

    def first_experimentName_occurrence(self, array, exp_name):
        return next((v for v in array if v["experimentName"] == exp_name), None)

    def test_get_variant(self):
        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant(experiment_name="exp_1")
            self.assertEqual(variant, "variant_4")

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]
            self.assert_exposure_event_fields(
                experiment_name="exp_1", variant=variant, event_fields=event_fields
            )

    def test_none_returned_on_variant_call_with_bad_id(self):
        config = {
            "test": {
                "id": "1",
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "start_ts": 0,
                "stop_ts": 0,
                "experiment": {
                    "id": 1,
                    "name": "test",
                    "variants": [
                        {"range_start": 0.0, "range_end": 0.2, "name": "active"},
                        {"range_start": 0.2, "range_end": 0.4, "name": "control_1"},
                        {"range_start": 0.4, "range_end": 0.6, "name": "control_2"},
                        {"range_start": 0.6, "range_end": 0.8, "name": "variant_3"},
                    ],
                },
            }
        }
        with create_temp_config_file(config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            decider = Decider(
                decider_context=self.minimal_decider_context,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            with self.assertLogs() as captured:
                variant = decider.get_variant("test")

                self.assertEqual(variant, None)
                self.assertEqual(self.event_logger.log.call_count, 0)

                assert any(
                    'Rust decider has initialization error: Decider initialization failed: Json error: "invalid type: string \\"1\\"'
                    in x.getMessage()
                    for x in captured.records
                )

    def test_none_returned_on_get_variant_call_with_no_experiment_data(self):
        config = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "start_ts": 0,
                "stop_ts": 0,
            }
        }
        with create_temp_config_file(config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            decider = Decider(
                decider_context=self.minimal_decider_context,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant("test")
            self.assertEqual(variant, None)

    def test_none_returned_on_get_variant_call_with_experiment_not_found(self):
        with create_temp_config_file({}) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            decider = Decider(
                decider_context=self.minimal_decider_context,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant("anything")
            self.assertEqual(variant, None)

    def test_get_variant_without_expose(self):
        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant_without_expose(experiment_name="exp_1")
            self.assertEqual(variant, "variant_4")

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_variant_without_expose_for_holdout_exposure(self):
        self.exp_base_config["exp_1"].update({"parent_hg_name": "hg"})
        self.exp_base_config.update(self.parent_hg_config)

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant_without_expose(experiment_name="exp_1")
            # user is part of Holdout (100% bucketing), so `None` is returned
            self.assertEqual(variant, None)

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]

            # `variant == None` for holdout but event will fire with `variant == "holdout"` for analysis
            self.assert_exposure_event_fields(
                experiment_name="hg", variant="holdout", event_fields=event_fields
            )

    def test_get_variant_for_identifier_user_id(self):
        identifier = USER_ID
        bucket_val = "user_id"
        self.exp_base_config["exp_1"]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant_for_identifier(
                experiment_name="exp_1", identifier=identifier, identifier_type=bucket_val
            )
            self.assertEqual(variant, "variant_4")

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]
            self.assert_minimal_exposure_event_fields(
                experiment_name="exp_1",
                variant=variant,
                event_fields=event_fields,
                identifier=identifier,
            )

            # `identifier` passed to correct event field of experiment's `bucket_val` config
            self.assertEqual(event_fields["user_id"], identifier)

    def test_get_variant_for_identifier_canonical_url(self):
        identifier = CANONICAL_URL
        bucket_val = "canonical_url"
        self.exp_base_config["exp_1"]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant_for_identifier(
                experiment_name="exp_1", identifier=identifier, identifier_type=bucket_val
            )
            self.assertEqual(variant, "variant_3")

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]
            self.assert_minimal_exposure_event_fields(
                experiment_name="exp_1",
                variant=variant,
                event_fields=event_fields,
                bucket_val=bucket_val,
                identifier=identifier,
            )

            # `identifier` passed to correct event field of experiment's `bucket_val` config
            self.assertEqual(event_fields["canonical_url"], identifier)

    def test_get_variant_for_identifier_device_id(self):
        identifier = DEVICE_ID
        bucket_val = "device_id"
        self.exp_base_config["exp_1"]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant_for_identifier(
                experiment_name="exp_1", identifier=identifier, identifier_type=bucket_val
            )
            self.assertEqual(variant, "variant_3")

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]
            self.assert_minimal_exposure_event_fields(
                experiment_name="exp_1",
                variant=variant,
                event_fields=event_fields,
                bucket_val=bucket_val,
                identifier=identifier,
            )

            # `identifier` passed to correct event field of experiment's `bucket_val` config
            self.assertEqual(event_fields["device_id"], identifier)

    def test_get_variant_for_identifier_wrong_bucket_val(self):
        identifier = USER_ID
        bucket_val = "device_id"
        self.exp_base_config["exp_1"]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            with self.assertLogs() as captured:
                # `identifier_type="canonical_url"`, which doesn't match `bucket_val` of `device_id`
                variant = decider.get_variant_for_identifier(
                    experiment_name="exp_1", identifier=identifier, identifier_type="canonical_url"
                )
                # `None` is returned since `identifier_type` doesn't match `bucket_val` in experiment-config json
                self.assertEqual(variant, None)
                # exposure isn't emitted either
                self.assertEqual(self.event_logger.log.call_count, 0)

                assert any(
                    'Requested identifier_type: "canonical_url" is incompatible with experiment\'s "bucket_val" = "device_id".'
                    in x.getMessage()
                    for x in captured.records
                )

    def test_get_variant_for_identifier_bogus_identifier_type(self):
        identifier = "anything"
        identifier_type = "blah"

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.minimal_decider_context,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            with self.assertLogs() as captured:
                # `identifier_type="canonical_url"`, which doesn't match `bucket_val` of `device_id`
                variant = decider.get_variant_for_identifier(
                    experiment_name="exp_1", identifier=identifier, identifier_type=identifier_type
                )
                # `None` is returned since `identifier_type` doesn't match `bucket_val` in experiment-config json
                self.assertEqual(variant, None)

                assert any(
                    "\"blah\" is not one of supported \"identifier_type\": ['user_id', 'device_id', 'canonical_url']."
                    in x.getMessage()
                    for x in captured.records
                )

        # exposure isn't emitted either
        self.assertEqual(self.event_logger.log.call_count, 0)

    def test_expose(self):
        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = "variant_4"
            decider.expose("exp_1", variant)

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]
            self.assert_exposure_event_fields(
                experiment_name="exp_1", variant=variant, event_fields=event_fields
            )

    def test_get_variant_for_identifier_without_expose_user_id(self):
        identifier = USER_ID
        bucket_val = "user_id"

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant_for_identifier_without_expose(
                experiment_name="exp_1", identifier=identifier, identifier_type=bucket_val
            )
            self.assertEqual(variant, "variant_4")

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_variant_for_identifier_without_expose_user_id_for_holdout_exposure(self):
        identifier = USER_ID
        bucket_val = "user_id"

        self.exp_base_config["exp_1"].update({"parent_hg_name": "hg"})
        self.exp_base_config.update(self.parent_hg_config)

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant_for_identifier_without_expose(
                experiment_name="exp_1", identifier=identifier, identifier_type=bucket_val
            )
            # user is part of Holdout (100% bucketing), so `None` is returned
            self.assertEqual(variant, None)

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]

            # `variant == None` for holdout but event will fire with `variant == "holdout"` for analysis
            self.assert_minimal_exposure_event_fields(
                experiment_name="hg", variant="holdout", event_fields=event_fields
            )

    def test_get_variant_for_identifier_without_expose_for_holdout_exposure_wrong_bucket_val(self):
        identifier = DEVICE_ID
        bucket_val = "device_id"
        self.exp_base_config["exp_1"]["experiment"].update({"bucket_val": bucket_val})

        self.exp_base_config["exp_1"].update({"parent_hg_name": "hg"})
        self.parent_hg_config["hg"]["experiment"].update({"bucket_val": bucket_val})
        self.exp_base_config.update(self.parent_hg_config)

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            # `identifier_type="canonical_url"`, which doesn't match `bucket_val` of `device_id`
            self.assertEqual(self.event_logger.log.call_count, 0)
            with self.assertLogs() as captured:
                variant = decider.get_variant_for_identifier_without_expose(
                    experiment_name="exp_1", identifier=identifier, identifier_type="canonical_url"
                )
                # `None` is returned since `identifier_type` doesn't match `bucket_val` in experiment-config json
                self.assertEqual(variant, None)

                assert any(
                    'Encountered error in decider.choose(): Requested identifier_type: "canonical_url" is incompatible with experiment\'s "bucket_val" = "device_id".'
                    in x.getMessage()
                    for x in captured.records
                )

            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_variant_for_identifier_without_expose_canonical_url(self):
        identifier = CANONICAL_URL
        bucket_val = "canonical_url"
        self.exp_base_config["exp_1"]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant_for_identifier_without_expose(
                experiment_name="exp_1", identifier=identifier, identifier_type="canonical_url"
            )
            self.assertEqual(variant, "variant_3")

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_variant_for_identifier_without_expose_device_id(self):
        identifier = DEVICE_ID
        bucket_val = "device_id"
        self.exp_base_config["exp_1"]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant_for_identifier_without_expose(
                experiment_name="exp_1", identifier=identifier, identifier_type="device_id"
            )
            self.assertEqual(variant, "variant_3")

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_variant_for_identifier_without_expose_bogus_identifier_type(self):
        identifier = "anything"
        identifier_type = "blah"

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            with self.assertLogs() as captured:
                variant = decider.get_variant_for_identifier_without_expose(
                    experiment_name="exp_1", identifier=identifier, identifier_type=identifier_type
                )

                self.assertEqual(variant, None)

                assert any(
                    "\"blah\" is not one of supported \"identifier_type\": ['user_id', 'device_id', 'canonical_url']."
                    in x.getMessage()
                    for x in captured.records
                )

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_all_variants_without_expose(self):
        # add 2 more experiments
        self.exp_base_config.update(self.additional_two_exp)

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant_arr = decider.get_all_variants_without_expose()

            self.assertEqual(len(variant_arr), len(self.exp_base_config))
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "exp_1"),
                {"id": 1, "name": "variant_4", "version": "2", "experimentName": "exp_1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e1"),
                {"id": 6, "name": "e1treat", "version": "4", "experimentName": "e1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e2"),
                {"id": 7, "name": "e2treat", "version": "5", "experimentName": "e2"},
            )

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_all_variants_without_expose_with_hg(self):
        # include an HG to test event still emitted for bulk call
        self.exp_base_config["exp_1"].update({"parent_hg_name": "hg"})
        self.exp_base_config.update(self.parent_hg_config)

        # add 2 more experiments
        self.exp_base_config.update(self.additional_two_exp)

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant_arr = decider.get_all_variants_without_expose()

            # "exp_1" returns variant None (due to "hg") and is excluded from the response arr
            self.assertEqual(len(variant_arr), len(self.exp_base_config) - 1)
            self.assertEqual(self.first_experimentName_occurrence(variant_arr, "exp_1"), None)
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e1"),
                {"id": 6, "name": "e1treat", "version": "4", "experimentName": "e1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e2"),
                {"id": 7, "name": "e2treat", "version": "5", "experimentName": "e2"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "hg"),
                {"id": 2, "name": "holdout", "version": "5", "experimentName": "hg"},
            )

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]

            # `variant == None` for holdout but event will fire with `variant == "holdout"` for analysis
            self.assert_exposure_event_fields(
                experiment_name="hg", variant="holdout", event_fields=event_fields
            )

    def test_get_all_variants_for_identifier_without_expose_user_id(self):
        identifier = USER_ID
        bucket_val = "user_id"

        # add 2 more experiments
        self.exp_base_config.update(self.additional_two_exp)

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant_arr = decider.get_all_variants_for_identifier_without_expose(
                identifier=identifier, identifier_type=bucket_val
            )

            self.assertEqual(len(variant_arr), len(self.exp_base_config))
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "exp_1"),
                {"id": 1, "name": "variant_4", "version": "2", "experimentName": "exp_1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e1"),
                {"id": 6, "name": "e1treat", "version": "4", "experimentName": "e1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e2"),
                {"id": 7, "name": "e2treat", "version": "5", "experimentName": "e2"},
            )

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_all_variants_for_identifier_without_expose_user_id_wrong_bucket(self):
        identifier = USER_ID
        bucket_val = "user_id"

        # add 2 more experiments
        self.exp_base_config.update(self.additional_two_exp)
        # alter `bucket_val` on exp_1 to induce err() due to `identifier_type` mismatch
        self.exp_base_config["exp_1"]["experiment"].update({"bucket_val": "device_id"})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.minimal_decider_context,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            with self.assertLogs() as captured:
                variant_arr = decider.get_all_variants_for_identifier_without_expose(
                    identifier=identifier, identifier_type=bucket_val
                )

                assert any(
                    'Encountered error for experiment: exp_1 in decider.choose_all(): Missing "device_id" in context for bucket_val = "device_id"'
                    in x.getMessage()
                    for x in captured.records
                )

                # "exp_1" returns err() (due to bucket_val/`identifier_type` mismatch) and is excluded from the response dict
                self.assertEqual(len(variant_arr), len(self.exp_base_config) - 1)
                self.assertEqual(self.first_experimentName_occurrence(variant_arr, "exp_1"), None)
                self.assertEqual(
                    self.first_experimentName_occurrence(variant_arr, "e1"),
                    {"id": 6, "name": "e1treat", "version": "4", "experimentName": "e1"},
                )
                self.assertEqual(
                    self.first_experimentName_occurrence(variant_arr, "e2"),
                    {"id": 7, "name": "e2treat", "version": "5", "experimentName": "e2"},
                )

                # no exposures should be triggered
                self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_all_variants_for_identifier_without_expose_user_id_with_hg(self):
        identifier = USER_ID
        bucket_val = "user_id"

        # include an HG to test event still emitted for bulk call
        self.exp_base_config["exp_1"].update({"parent_hg_name": "hg"})
        self.exp_base_config.update(self.parent_hg_config)

        # add 2 more experiments
        self.exp_base_config.update(self.additional_two_exp)

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant_arr = decider.get_all_variants_for_identifier_without_expose(
                identifier=identifier, identifier_type=bucket_val
            )

            # "exp_1" returns variant None (due to "hg") and is excluded from the response dict
            self.assertEqual(len(variant_arr), len(self.exp_base_config) - 1)
            self.assertEqual(self.first_experimentName_occurrence(variant_arr, "exp_1"), None)
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e1"),
                {"id": 6, "name": "e1treat", "version": "4", "experimentName": "e1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e2"),
                {"id": 7, "name": "e2treat", "version": "5", "experimentName": "e2"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "hg"),
                {"id": 2, "name": "holdout", "version": "5", "experimentName": "hg"},
            )

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]

            # `variant == None` for holdout but event will fire with `variant == "holdout"` for analysis
            self.assert_minimal_exposure_event_fields(
                experiment_name="hg", variant="holdout", event_fields=event_fields
            )

    def test_get_all_variants_for_identifier_without_expose_device_id(self):
        identifier = DEVICE_ID
        bucket_val = "device_id"

        # add 2 more experiments
        self.exp_base_config.update(self.additional_two_exp)

        for exp_name in self.exp_base_config.keys():
            self.exp_base_config[exp_name]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant_arr = decider.get_all_variants_for_identifier_without_expose(
                identifier=identifier, identifier_type=bucket_val
            )

            self.assertEqual(len(variant_arr), len(self.exp_base_config))
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "exp_1"),
                {"id": 1, "name": "variant_3", "version": "2", "experimentName": "exp_1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e1"),
                {"id": 6, "name": "e1treat", "version": "4", "experimentName": "e1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e2"),
                {"id": 7, "name": "e2treat", "version": "5", "experimentName": "e2"},
            )

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_all_variants_for_identifier_without_expose_device_id_with_hg(self):
        identifier = DEVICE_ID
        bucket_val = "device_id"

        # include an HG to test event still emitted for bulk call
        self.exp_base_config["exp_1"].update({"parent_hg_name": "hg"})
        self.exp_base_config.update(self.parent_hg_config)

        # add 2 more experiments
        self.exp_base_config.update(self.additional_two_exp)

        for exp_name in self.exp_base_config.keys():
            self.exp_base_config[exp_name]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant_arr = decider.get_all_variants_for_identifier_without_expose(
                identifier=identifier, identifier_type=bucket_val
            )

            # "exp_1" returns variant None (due to "hg") and is excluded from the response dict
            self.assertEqual(len(variant_arr), len(self.exp_base_config) - 1)
            self.assertEqual(self.first_experimentName_occurrence(variant_arr, "exp_1"), None)
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e1"),
                {"id": 6, "name": "e1treat", "version": "4", "experimentName": "e1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e2"),
                {"id": 7, "name": "e2treat", "version": "5", "experimentName": "e2"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "hg"),
                {"id": 2, "name": "holdout", "version": "5", "experimentName": "hg"},
            )

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]

            # `variant == None` for holdout but event will fire with `variant == "holdout"` for analysis
            self.assert_minimal_exposure_event_fields(
                experiment_name="hg",
                variant="holdout",
                event_fields=event_fields,
                bucket_val=bucket_val,
                identifier=identifier,
            )

    def test_get_all_variants_for_identifier_without_expose_canonical_url(self):
        identifier = CANONICAL_URL
        bucket_val = "canonical_url"

        # add 2 more experiments
        self.exp_base_config.update(self.additional_two_exp)

        for exp_name in self.exp_base_config.keys():
            self.exp_base_config[exp_name]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant_arr = decider.get_all_variants_for_identifier_without_expose(
                identifier=identifier, identifier_type=bucket_val
            )

            self.assertEqual(len(variant_arr), len(self.exp_base_config))
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "exp_1"),
                {"id": 1, "name": "variant_3", "version": "2", "experimentName": "exp_1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e1"),
                {"id": 6, "name": "e1treat", "version": "4", "experimentName": "e1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e2"),
                {"id": 7, "name": "e2treat", "version": "5", "experimentName": "e2"},
            )

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_all_variants_for_identifier_without_expose_canonical_url_with_hg(self):
        identifier = CANONICAL_URL
        bucket_val = "canonical_url"

        # include an HG to test event still emitted for bulk call
        self.exp_base_config["exp_1"].update({"parent_hg_name": "hg"})
        self.exp_base_config.update(self.parent_hg_config)

        # add 2 more experiments
        self.exp_base_config.update(self.additional_two_exp)

        for exp_name in self.exp_base_config.keys():
            self.exp_base_config[exp_name]["experiment"].update({"bucket_val": bucket_val})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant_arr = decider.get_all_variants_for_identifier_without_expose(
                identifier=identifier, identifier_type=bucket_val
            )

            # "exp_1" returns variant None (due to "hg") and is excluded from the response dict
            self.assertEqual(len(variant_arr), len(self.exp_base_config) - 1)
            self.assertEqual(self.first_experimentName_occurrence(variant_arr, "exp_1"), None)
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e1"),
                {"id": 6, "name": "e1treat", "version": "4", "experimentName": "e1"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "e2"),
                {"id": 7, "name": "e2treat", "version": "5", "experimentName": "e2"},
            )
            self.assertEqual(
                self.first_experimentName_occurrence(variant_arr, "hg"),
                {"id": 2, "name": "holdout", "version": "5", "experimentName": "hg"},
            )

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]

            # `variant == None` for holdout but event will fire with `variant == "holdout"` for analysis
            self.assert_minimal_exposure_event_fields(
                experiment_name="hg",
                variant="holdout",
                event_fields=event_fields,
                bucket_val=bucket_val,
                identifier=identifier,
            )

    def test_get_all_variants_for_identifier_without_expose_bogus_identifier_type(self):
        identifier = "anything"
        # use non-supported `identifier_type`
        identifier_type = "blah"

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)

            with self.assertLogs() as captured:
                variant_arr = decider.get_all_variants_for_identifier_without_expose(
                    identifier=identifier, identifier_type="blah"
                )

                self.assertEqual(len(variant_arr), 0)

                assert any(
                    "\"blah\" is not one of supported \"identifier_type\": ['user_id', 'device_id', 'canonical_url']."
                    in x.getMessage()
                    for x in captured.records
                )

            # no exposures should be triggered
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_get_variant_with_exposure_kwargs(self):
        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            exp_kwargs = {"foo": "test_1", "bar": "test_2"}
            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant(experiment_name="exp_1", **exp_kwargs)
            self.assertEqual(variant, "variant_4")

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]
            self.assert_exposure_event_fields(
                experiment_name="exp_1", variant=variant, event_fields=event_fields
            )

            # additional kwargs logged
            self.assertEqual(event_fields["foo"], exp_kwargs["foo"])
            self.assertEqual(event_fields["bar"], exp_kwargs["bar"])

            self.assertEqual(event_fields["inputs"]["foo"], exp_kwargs["foo"])
            self.assertEqual(event_fields["inputs"]["bar"], exp_kwargs["bar"])

    def test_get_variant_with_disabled_exp(self):
        self.exp_base_config["exp_1"].update({"enabled": False})

        with create_temp_config_file(self.exp_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)

            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )
            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant(experiment_name="exp_1")
            self.assertEqual(variant, None)

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 0)


class TestDeciderGetDynamicConfig(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.event_logger = mock.Mock(spec=DebugLogger)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.mock_span.context = None
        self.minimal_decider_context = DeciderContext()
        self.dc_base_config = {
            "dc_1": {
                "id": 1,
                "name": "dc_1",
                "enabled": True,
                "version": "2",
                "type": "dynamic_config",
                "start_ts": 37173982,
                "stop_ts": 2147483648,
                "owner": "test_owner",
                "experiment": {
                    "experiment_version": 1,
                },
            }
        }
        self.dc = DeciderContext(
            user_id=USER_ID,
            logged_in=IS_LOGGED_IN,
            country_code=COUNTRY_CODE,
            locale=LOCALE_CODE,
            origin_service=ORIGIN_SERVICE,
            user_is_employee=True,
            device_id=DEVICE_ID,
            auth_client_id=AUTH_CLIENT_ID,
            cookie_created_timestamp=COOKIE_CREATED_TIMESTAMP,
        )

    def test_get_bool(self):
        self.dc_base_config["dc_1"].update({"value_type": "Boolean", "value": True})

        with create_temp_config_file(self.dc_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            res = decider.get_bool("dc_1")
            self.assertEqual(res, True)
            res = decider.get_float("dc_1")
            self.assertEqual(res, 0.0)

    def test_get_int(self):
        self.dc_base_config["dc_1"].update({"value_type": "Integer", "value": 7})

        with create_temp_config_file(self.dc_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            res = decider.get_int("dc_1")
            self.assertEqual(res, 7)
            res = decider.get_float("dc_1")
            self.assertEqual(res, 7.0)

    def test_get_float(self):
        self.dc_base_config["dc_1"].update({"value_type": "Float", "value": 4.20})

        with create_temp_config_file(self.dc_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            res = decider.get_float("dc_1")
            self.assertEqual(res, 4.20)
            res = decider.get_int("dc_1")
            self.assertEqual(res, 0)

    def test_get_string(self):
        self.dc_base_config["dc_1"].update({"value_type": "Text", "value": "helloworld!"})

        with create_temp_config_file(self.dc_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            res = decider.get_string("dc_1")
            self.assertEqual(res, "helloworld!")
            res = decider.get_int("dc_1")
            self.assertEqual(res, 0)

    def test_get_map(self):
        self.dc_base_config["dc_1"].update(
            {"value_type": "Map", "value": {"key": "value", "another_key": "another_value"}}
        )

        with create_temp_config_file(self.dc_base_config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            decider = Decider(
                decider_context=self.dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            res = decider.get_map("dc_1")
            self.assertEqual(res, dict({"key": "value", "another_key": "another_value"}))
            res = decider.get_string("dc_1")
            self.assertEqual(res, "")
