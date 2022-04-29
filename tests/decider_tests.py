import contextlib
import json
import tempfile
import unittest

from unittest import mock

from baseplate import ServerSpan
from baseplate.lib.events import DebugLogger
from baseplate.lib.file_watcher import FileWatcher
from reddit_edgecontext import User
from reddit_edgecontext import ValidatedAuthenticationToken

from reddit_decider import Decider
from reddit_decider import decider_client_from_config
from reddit_decider import DeciderContext
from reddit_decider import DeciderContextFactory
from reddit_decider import EventType
from reddit_decider import init_decider_parser
from reddit_decider import decider_client_from_config

user_id = "t2_1234"
is_logged_in = True
auth_client_id = "token"
country_code = "US"
device_id = "abc"
cookie_created_timestamp = 1234
locale_code = "us_en"
origin_service = "origin"
app_name = "ios"
build_number = 1
event_fields = {
    "user_id": user_id,
    "logged_in": is_logged_in,
    "cookie_created_timestamp": cookie_created_timestamp,
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
        self.mock_span.context.edgecontext.user.event_fields = mock.Mock(
            return_value=event_fields
        )
        self.mock_span.context.edgecontext.authentication_token = mock.Mock(spec=ValidatedAuthenticationToken)
        self.mock_span.context.edgecontext.authentication_token.oauth_client_id = auth_client_id
        self.mock_span.context.edgecontext.geolocation.country_code = country_code
        self.mock_span.context.edgecontext.locale.locale_code = locale_code
        self.mock_span.context.edgecontext.origin_service.name = origin_service
        self.mock_span.context.edgecontext.device.id = device_id

    def test_make_object_for_context_and_decider_context(self, file_watcher_mock):
        decider_ctx_factory = decider_client_from_config(
            {"experiments.path": "/tmp/test", "experiments.timeout": "60 seconds"},
            self.event_logger,
            prefix="experiments.",
        )
        decider = decider_ctx_factory.make_object_for_context(name="test", span=self.mock_span)
        self.assertIsInstance(decider, Decider)

        decider_context = getattr(decider, "_decider_context")
        self.assertIsInstance(decider_context, DeciderContext)

        decider_ctx_dict = decider_context.to_dict()
        self.assertEqual(decider_ctx_dict["user_id"], user_id)
        self.assertEqual(decider_ctx_dict["country_code"], country_code)
        self.assertEqual(decider_ctx_dict["user_is_employee"], True)
        self.assertEqual(decider_ctx_dict["logged_in"], is_logged_in)
        self.assertEqual(decider_ctx_dict["device_id"], device_id)
        self.assertEqual(decider_ctx_dict["locale"], locale_code)
        self.assertEqual(decider_ctx_dict["origin_service"], origin_service)
        self.assertEqual(decider_ctx_dict["auth_client_id"], auth_client_id)
        self.assertEqual(
            decider_ctx_dict["app_name"], None
        )  # requires request_field_extractor param
        self.assertEqual(
            decider_ctx_dict["build_number"], None
        )  # requires request_field_extractor param
        self.assertEqual(
            decider_ctx_dict["cookie_created_timestamp"],
            self.mock_span.context.edgecontext.user.event_fields().get("cookie_created_timestamp"),
        )

    # Todo: DeciderContext request_field_extractor tests


# Todo: test DeciderClient()
# @mock.patch("reddit_decider.FileWatcher")
# class DeciderClientTests(unittest.TestCase):


class TestDeciderGetVariant(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.event_logger = mock.Mock(spec=DebugLogger)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.mock_span.context = None
        self.minimal_decider_context = DeciderContext(user_id=user_id)

    @contextlib.contextmanager
    def create_temp_config_file(self, contents):
        with tempfile.NamedTemporaryFile() as f:
            f.write(json.dumps(contents).encode())
            f.seek(0)
            yield f

    def test_get_variant_expose_event_fields(self):
        config = {
            "exp_1": {
                "id": 1,
                "name": "exp_1",
                "enabled": True,
                "version": "2",
                "type": "range_variant",
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

        with self.create_temp_config_file(config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            dc = DeciderContext(
                user_id=user_id,
                logged_in=is_logged_in,
                country_code=country_code,
                locale=locale_code,
                origin_service=origin_service,
                user_is_employee=True,
                device_id=device_id,
                auth_client_id=auth_client_id,
                app_name=app_name,
                build_number=build_number,
                cookie_created_timestamp=cookie_created_timestamp,
            )

            decider = Decider(
                decider_context=dc,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            self.assertEqual(self.event_logger.log.call_count, 0)
            variant = decider.get_variant("exp_1")
            self.assertEqual(variant, "variant_4")

            # exposure assertions
            self.assertEqual(self.event_logger.log.call_count, 1)
            event_fields = self.event_logger.log.call_args[1]
            self.assertEqual(event_fields["variant"], variant)
            self.assertEqual(event_fields["user_id"], user_id)
            self.assertEqual(event_fields["logged_in"], is_logged_in)
            self.assertEqual(event_fields["app_name"], app_name)
            self.assertEqual(event_fields["cookie_created_timestamp"], cookie_created_timestamp)
            self.assertEqual(event_fields["event_type"], EventType.EXPOSE)
            self.assertNotEqual(event_fields["span"], None)

            cfg = config["exp_1"]
            self.assertEqual(getattr(event_fields["experiment"], "id"), cfg["id"])
            self.assertEqual(getattr(event_fields["experiment"], "name"), cfg["name"])
            self.assertEqual(getattr(event_fields["experiment"], "owner"), cfg["owner"])
            self.assertEqual(getattr(event_fields["experiment"], "version"), cfg["version"])

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
        with self.create_temp_config_file(config) as f:
            filewatcher = FileWatcher(path=f.name, parser=init_decider_parser, timeout=2, backoff=2)
            decider = Decider(
                decider_context=self.minimal_decider_context,
                config_watcher=filewatcher,
                server_span=self.mock_span,
                context_name="test",
                event_logger=self.event_logger,
            )

            variant = decider.get_variant("test")
            self.assertEqual(self.event_logger.log.call_count, 0)
            self.assertEqual(variant, None)

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
        with self.create_temp_config_file(config) as f:
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
        with self.create_temp_config_file({}) as f:
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

    # Todo: test exposure_kwargs

    # Todo: test un-enabled experiment


# Todo: test get_variant_without_expose()
# class TestDeciderGetVariantWithoutExpose(unittest.TestCase):

# Todo: test expose()
# class TestDeciderExpose(unittest.TestCase):


class TestDeciderGetDynamicConfig(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.event_logger = mock.Mock(spec=DebugLogger)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.mock_span.context = None
        self.minimal_decider_context = DeciderContext(user_id=user_id)
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
            user_id=user_id,
            logged_in=is_logged_in,
            country_code=country_code,
            locale=locale_code,
            origin_service=origin_service,
            user_is_employee=True,
            device_id=device_id,
            auth_client_id=auth_client_id,
            app_name=app_name,
            build_number=build_number,
            cookie_created_timestamp=cookie_created_timestamp,
        )

    @contextlib.contextmanager
    def create_temp_config_file(self, contents):
        with tempfile.NamedTemporaryFile() as f:
            f.write(json.dumps(contents).encode())
            f.seek(0)
            yield f

    def test_get_bool(self):
        self.dc_base_config["dc_1"].update({"value_type": "Boolean","value": True,})

        with self.create_temp_config_file(self.dc_base_config) as f:
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
            self.assertEqual(res, None)

    def test_get_int(self):
        self.dc_base_config["dc_1"].update({"value_type": "Integer","value": 7,})

        with self.create_temp_config_file(self.dc_base_config) as f:
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
        self.dc_base_config["dc_1"].update({"value_type": "Float","value": 4.20,})

        with self.create_temp_config_file(self.dc_base_config) as f:
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
            self.assertEqual(res, None)

    def test_get_string(self):
        self.dc_base_config["dc_1"].update({"value_type": "Text","value": "helloworld!",})

        with self.create_temp_config_file(self.dc_base_config) as f:
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
            self.assertEqual(res, None)

    def test_get_map(self):
        self.dc_base_config["dc_1"].update(
            {"value_type": "Map","value": {"key": "value", "another_key": "another_value"},}
        )

        with self.create_temp_config_file(self.dc_base_config) as f:
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
            self.assertEqual(res, None)
    