import logging
import warnings

from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Callable
from typing import Dict
from typing import IO
from typing import List
from typing import Optional
from typing import Union

import rust_decider  # type: ignore
from rust_decider import Decider as RustDecider

from baseplate import RequestContext
from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.events import DebugLogger
from baseplate.lib.events import EventLogger
from baseplate.lib.file_watcher import FileWatcher
from baseplate.lib.file_watcher import T
from baseplate.lib.file_watcher import WatchedFileNotAvailableError
from reddit_edgecontext import ValidatedAuthenticationToken
from rust_decider import Decider as RustDecider
from rust_decider import DeciderException
from rust_decider import FeatureNotFoundException
from rust_decider import make_ctx
from typing_extensions import Literal


logger = logging.getLogger(__name__)

EMPLOYEE_ROLES = ["employee", "contractor"]
IDENTIFIERS = ["user_id", "device_id", "canonical_url"]


class EventType(Enum):
    EXPOSE = "expose"


@dataclass
class ExperimentConfig:
    id: int
    version: str
    name: str
    bucket_val: str
    start_ts: int
    stop_ts: int
    owner: str


class DeciderContext:
    """DeciderContext() is used to contain all fields necessary for
    bucketing, targeting, and overrides.
    :code:`DeciderContext()` is populated in :code:`make_object_for_context()`.
    """

    def __init__(
        self,
        user_id: Optional[str] = None,
        country_code: Optional[str] = None,
        locale: Optional[str] = None,
        user_is_employee: Optional[bool] = None,
        logged_in: Optional[bool] = None,
        device_id: Optional[str] = None,
        oauth_client_id: Optional[str] = None,
        origin_service: Optional[str] = None,
        cookie_created_timestamp: Optional[float] = None,
        loid_created_timestamp: Optional[float] = None,
        extracted_fields: Optional[dict] = None,
    ):
        self._user_id = user_id
        self._country_code = country_code
        self._locale = locale
        self._user_is_employee = user_is_employee
        self._logged_in = logged_in
        self._device_id = device_id
        self._oauth_client_id = oauth_client_id
        self._origin_service = origin_service
        self._cookie_created_timestamp = cookie_created_timestamp
        self._loid_created_timestamp = loid_created_timestamp
        self._extracted_fields = extracted_fields

    def to_dict(self) -> Dict:
        ef = deepcopy(self._extracted_fields or {})

        return {
            "user_id": self._user_id,
            "country_code": self._country_code,
            "locale": self._locale,
            "user_is_employee": self._user_is_employee,
            "logged_in": self._logged_in,
            "device_id": self._device_id,
            "oauth_client_id": self._oauth_client_id,
            "origin_service": self._origin_service,
            "cookie_created_timestamp": self._cookie_created_timestamp,
            "loid_created_timestamp": self._loid_created_timestamp,
            "other_fields": ef,
            **ef,
        }

    def to_event_dict(self) -> Dict:
        user_fields = {
            "id": self._user_id,
            "logged_in": self._logged_in,
            "cookie_created_timestamp": self._cookie_created_timestamp,
            "is_employee": self._user_is_employee,
        }

        ef = deepcopy(self._extracted_fields or {})

        app_fields = {}
        if ef.get("app_name"):
            app_fields["name"] = ef["app_name"]
        if ef.get("app_version"):
            app_fields["version"] = ef["app_version"]
        if ef.get("build_number"):
            app_fields["build_number"] = ef["build_number"]
        if self._locale:
            app_fields["relevant_locale"] = self._locale

        geo_fields = {}
        if self._country_code:
            geo_fields["country_code"] = self._country_code

        request_fields = {}
        if ef.get("canonical_url"):
            request_fields["canonical_url"] = ef["canonical_url"]

        platform_fields = {}
        if self._device_id:
            platform_fields["device_id"] = self._device_id

        return {
            "user_id": self._user_id,
            "country_code": self._country_code,
            "locale": self._locale,
            "user_is_employee": self._user_is_employee,
            "logged_in": self._logged_in,
            "device_id": self._device_id,
            "origin_service": self._origin_service,
            "cookie_created_timestamp": self._cookie_created_timestamp,
            "user": user_fields,
            "app": app_fields,
            "geo": geo_fields,
            "request": request_fields,
            "platform": platform_fields,
            **ef,
        }


def init_decider_parser(file: IO) -> Any:
    return RustDecider(file.name)


class Decider:
    """Access to experiments with automatic refresh when changed.

    This decider client allows access to the experiments cached on disk by
    the experiment configuration fetcher daemon.
    It will automatically reload the cache when changed.
    """

    def __init__(
        self,
        decider_context: DeciderContext,
        internal: Optional[RustDecider],
        server_span: Span,
        context_name: str,
        event_logger: Optional[EventLogger] = None,
    ):
        self._decider_context = decider_context
        self._internal = internal
        self._span = server_span
        self._context_name = context_name
        if event_logger:
            self._event_logger = event_logger
        else:
            self._event_logger = DebugLogger()

    def _get_decider(self) -> Optional[T]:
        if self._internal is not None:
            return self._internal.get_decider()

        return None

    def _get_ctx(self) -> Any:
        context_fields = self._decider_context.to_dict()
        return make_ctx(context_fields)

    def _get_ctx_with_set_identifier(
        self, identifier: str, identifier_type: Literal["user_id", "device_id", "canonical_url"]
    ) -> Dict[str, Any]:
        context_fields = self._decider_context.to_dict()
        context_fields[identifier_type] = identifier

        return make_ctx(context_fields)

    def _format_decision(self, decision_dict: Dict[str, str]) -> Dict[str, Any]:
        out = {}
        # cast id to int
        for k, v in decision_dict.items():
            if k == "id":
                try:
                    out[k] = int(v)
                except ValueError:
                    out[k] = v  # type: ignore
            else:
                out[k] = v  # type: ignore

        return out

    def _send_expose(
        self, event: str, exposure_fields: dict, overwrite_identifier: bool = False
    ) -> None:
        event_fields = deepcopy(exposure_fields)
        try:
            (
                _event_type,
                exp_id,
                name,
                version,
                event_variant,
                bucketing_value,
                bucket_val,
                start_ts,
                stop_ts,
                owner,
            ) = event.split("::::")
        except ValueError:
            logger.warning(
                f'Encountered error in event.split("::::") for event: {event}. Exposure not emitted.'
            )
            return

        experiment = ExperimentConfig(
            id=self._cast_to_int(exp_id),
            name=name,
            version=version,
            bucket_val=bucket_val,
            start_ts=self._cast_to_int(start_ts),
            stop_ts=self._cast_to_int(stop_ts),
            owner=owner,
        )

        if overwrite_identifier:
            event_fields = {**event_fields, **{bucket_val: bucketing_value}}

        self._event_logger.log(
            experiment=experiment,
            variant=event_variant,
            span=self._span,
            event_type=EventType.EXPOSE,
            inputs=event_fields,
            **event_fields,
        )
        return

    def _send_expose_if_holdout(
        self, event: str, exposure_fields: dict, overwrite_identifier: bool = False
    ) -> None:
        event_fields = deepcopy(exposure_fields)
        try:
            (
                event_type,
                exp_id,
                name,
                version,
                event_variant,
                bucketing_value,
                bucket_val,
                start_ts,
                stop_ts,
                owner,
            ) = event.split("::::")
        except ValueError:
            logger.warning(
                f'Encountered error in event.split("::::") for event: {event}. Exposure not emitted.'
            )
            return

        # event_type enum:
        #   0: regular bucketing
        #   1: override
        #   2: holdout
        if event_type == "2":
            experiment = ExperimentConfig(
                id=self._cast_to_int(exp_id),
                name=name,
                version=version,
                bucket_val=bucket_val,
                start_ts=self._cast_to_int(start_ts),
                stop_ts=self._cast_to_int(stop_ts),
                owner=owner,
            )

            if overwrite_identifier:
                event_fields = {**event_fields, **{bucket_val: bucketing_value}}

            self._event_logger.log(
                experiment=experiment,
                variant=event_variant,
                span=self._span,
                event_type=EventType.EXPOSE,
                inputs=event_fields,
                **event_fields,
            )
        return

    @staticmethod
    def _cast_to_int(input: str) -> int:
        out = 1
        try:
            out = int(input)
        except ValueError as e:
            logger.info(f"Encountered error casting to integer: {e}")
        return out

    def get_variant(
        self, experiment_name: str, **exposure_kwargs: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        """Return a bucketing variant, if any, with auto-exposure.

        Since calling :code:`get_variant()` will fire an exposure event, it
        is best to call it when you are sure the user will be exposed to the experiment.

        If you absolutely must check the status of an experiment
        before the user will be exposed to the experiment,
        use :code:`get_variant_without_expose()` to disable exposure events
        and call :code:`expose()` manually later.

        :param experiment_name: Name of the experiment you want a variant for.

        :param exposure_kwargs:  Additional arguments that will be passed
            to :code:`events_logger` (keys must be part of v2 event schema,
            use dicts for nested fields) under :code:`inputs` and as :code:`kwargs`

        :return: Variant name if a variant is assigned, :code:`None` otherwise.
        """
        if self._internal is None:
            logger.error("RustDecider is None--did not initialize.")
            return None

        ctx = self._decider_context.to_dict()

        try:
            decision = self._internal.choose(experiment_name, ctx)
        except FeatureNotFoundException as exc:
            warnings.warn(str(exc))
            return None
        except DeciderException as exc:
            logger.info(str(exc))
            return None

        event_context_fields = self._decider_context.to_event_dict()
        event_context_fields.update(exposure_kwargs or {})

        for event in decision.events:
            self._send_expose(event=event, exposure_fields=event_context_fields)

        return decision.variant

    def get_variant_without_expose(self, experiment_name: str) -> Optional[str]:
        """Return a bucketing variant, if any, without emitting exposure event.

        The :code:`expose()` function is available to be manually called afterward.

        However, experiments in Holdout Groups will still send an exposure for
        the holdout parent experiment, since it is not possible to
        manually expose the holdout later (because it's impossible to know if a
        returned :code:`None` or :code:`"control_1"` string
        came from the holdout group or its child experiment once this function exits).

        :param experiment_name: Name of the experiment you want a variant for.

        :return: Variant name if a variant is assigned, None otherwise.
        """
        if self._internal is None:
            logger.error("RustDecider is None--did not initialize.")
            return None

        ctx = self._decider_context.to_dict()

        try:
            decision = self._internal.choose(experiment_name, ctx)
        except FeatureNotFoundException as exc:
            warnings.warn(str(exc))
            return None
        except DeciderException as exc:
            logger.info(str(exc))
            return None

        event_context_fields = self._decider_context.to_event_dict()

        for event in decision.events:
            self._send_expose_if_holdout(event=event, exposure_fields=event_context_fields)

        return decision.variant

    def expose(
        self, experiment_name: str, variant_name: str, **exposure_kwargs: Optional[Dict[str, Any]]
    ) -> None:
        """Log an event to indicate that a user has been exposed to an experimental treatment.

        Meant to be used after calling :code:`get_variant_without_expose()`
        since :code:`get_variant()` emits exposure event automatically.

        :param experiment_name: Name of the experiment that was exposed.

        :param variant_name: Name of the variant that was exposed.

        :param exposure_kwargs: Additional arguments that will be passed
            to :code:`events_logger` (keys must be part of v2 event schema,
            use dicts for nested fields) under :code:`inputs` and as :code:`kwargs`
        """
        decider = self._get_decider()
        if decider is None:
            return

        experiment = decider.get_experiment(experiment_name)
        error = experiment.err()
        if error:
            logger.warning(f"Encountered error in decider.get_experiment(): {error}")
            return

        event_context_fields = self._decider_context.to_event_dict()
        event_context_fields.update(exposure_kwargs or {})
        event_fields = deepcopy(event_context_fields)

        exp_dict = experiment.val()

        experiment = ExperimentConfig(
            id=int(exp_dict.get("id", 0)),
            name=exp_dict.get("name"),
            version=str(exp_dict.get("version")),
            bucket_val=exp_dict.get("variant_set", {}).get("bucket_val"),
            start_ts=exp_dict.get("variant_set", {}).get("start_ts"),
            stop_ts=exp_dict.get("variant_set", {}).get("stop_ts"),
            owner=exp_dict.get("owner"),
        )

        self._event_logger.log(
            experiment=experiment,
            variant=variant_name,
            span=self._span,
            event_type=EventType.EXPOSE,
            inputs=event_fields,
            **event_fields,
        )

    def get_variant_for_identifier(
        self,
        experiment_name: str,
        identifier: str,
        identifier_type: Literal["user_id", "device_id", "canonical_url"],
        **exposure_kwargs: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        """Return a bucketing variant, if any, with auto-exposure for a given :code:`identifier`.

        Since calling :code:`get_variant_for_identifier()` will fire an exposure event, it
        is best to call it when you are sure the user will be exposed to the experiment.

        :param experiment_name: Name of the experiment you want a variant for.

        :param identifier: an arbitary string used to bucket the experiment by
            being set on :code:`DeciderContext`'s :code:`identifier_type` field.

        :param identifier_type: Sets :code:`{identifier_type: identifier}` on DeciderContext and
            should match an experiment's :code:`bucket_val` to get a variant.

        :param exposure_kwargs: Additional arguments that will be passed
            to :code:`events_logger` (keys must be part of v2 event schema,
            use dicts for nested fields) under :code:`inputs` and as :code:`kwargs`


        :return: Variant name if a variant is assigned, None otherwise.
        """
        if identifier_type not in IDENTIFIERS:
            logger.warning(
                f'"{identifier_type}" is not one of supported "identifier_type": {IDENTIFIERS}.'
            )
            return None

        decider = self._get_decider()
        if decider is None:
            return None

        ctx = self._get_ctx_with_set_identifier(
            identifier=identifier, identifier_type=identifier_type
        )
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.info(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")
            return None

        choice = decider.choose(
            feature_name=experiment_name, ctx=ctx, identifier_type=identifier_type
        )
        error = choice.err()

        if error:
            logger.info(f"Encountered error in decider.choose(): {error}")
            return None

        variant = choice.decision()

        event_context_fields = self._decider_context.to_event_dict()
        event_context_fields.update(exposure_kwargs or {})

        for event in choice.events():
            self._send_expose(
                event=event, exposure_fields=event_context_fields, overwrite_identifier=True
            )

        return variant

    def get_variant_for_identifier_without_expose(
        self,
        experiment_name: str,
        identifier: str,
        identifier_type: Literal["user_id", "device_id", "canonical_url"],
    ) -> Optional[str]:
        """Return a bucketing variant, if any, without emitting exposure event for a given :code:`identifier`.

        The :code:`expose()` function is available to be manually called afterward to emit
        exposure event.

        However, experiments in Holdout Groups will still send an exposure for
        the holdout parent experiment, since it is not possible to
        manually expose the holdout later (because it's impossible to know if a
        returned :code:`None` or :code:`"control_1"` string
        came from the holdout group or its child experiment once this function exits).

        :param experiment_name: Name of the experiment you want a variant for.

        :param identifier: an arbitary string used to bucket the experiment by
            being set on :code:`DeciderContext`'s :code:`identifier_type` field.

        :param identifier_type: Sets :code:`{identifier_type: identifier}` on DeciderContext and
            should match an experiment's :code:`bucket_val` to get a variant.

        :return: Variant name if a variant is assigned, None otherwise.
        """
        if identifier_type not in IDENTIFIERS:
            logger.warning(
                f'"{identifier_type}" is not one of supported "identifier_type": {IDENTIFIERS}.'
            )
            return None

        decider = self._get_decider()
        if decider is None:
            return None

        ctx = self._get_ctx_with_set_identifier(
            identifier=identifier, identifier_type=identifier_type
        )
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.info(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")
            return None

        choice = decider.choose(
            feature_name=experiment_name, ctx=ctx, identifier_type=identifier_type
        )
        error = choice.err()
        if error:
            logger.info(f"Encountered error in decider.choose(): {error}")
            return None

        variant = choice.decision()

        event_context_fields = self._decider_context.to_event_dict()

        # expose Holdout if the experiment is part of one
        for event in choice.events():
            self._send_expose_if_holdout(
                event=event, exposure_fields=event_context_fields, overwrite_identifier=True
            )

        return variant

    def get_all_variants_without_expose(self) -> List[Dict[str, Union[str, int]]]:
        """Return a list of experiment dicts in this format:

            .. code-block:: json

                [
                    {
                        "id": 1,
                        "name": "variant_1",
                        "version": "1",
                        "experimentName": "exp_1"

                    }
                ]

            If an experiment has a variant of :code:`None`, it is not included
            in the returned list. All available experiments get bucketed.
            Exposure events are not emitted.

        The :code:`expose()` function is available to be manually called afterward to emit
        exposure event.

        However, experiments in Holdout Groups will still send an exposure for
        the holdout parent experiment, since it is not possible to
        manually expose the holdout later (because it's impossible to know if a
        returned :code:`None` or :code:`"control_1"` string
        came from the holdout group or its child experiment once this function exits).

        :return: list of experiment dicts with non-:code:`None` variants.
        """
        if self._rs_decider is None:
            logger.error("rs_decider is None--did not initialize.")
            return []

        ctx = self._decider_context.to_dict()

        try:
            all_decisions = self._rs_decider.choose_all(ctx)
        except ValueError as exc:
            logger.info(exc)
            return []

        parsed_choices = []

        event_context_fields = self._decider_context.to_event_dict()

        for decision_dict in all_decisions:
            if decision_dict:
                variant = decision_dict.get("variant")
                if variant:
                    parsed_choices.append(self._transform_decision(decision_dict))

                # expose Holdout if the experiment is part of one
                for event in decision_dict.get("events", []):
                    self._send_expose_if_holdout(event=event, exposure_fields=event_context_fields)

        return parsed_choices


    def _transform_decision(self, decision_dict: Dict[str, str]) -> Dict[str, Any]:
        return {
            "name": decision_dict.get("variant"),
            "id": decision_dict.get("experiment_id"),
            "version": str(decision_dict.get("experiment_version")),
            "experimentName": decision_dict.get("experiment_name"),
        }

    def get_all_variants_for_identifier_without_expose(
        self, identifier: str, identifier_type: Literal["user_id", "device_id", "canonical_url"]
    ) -> List[Dict[str, Union[str, int]]]:
        """Return a list of experiment dicts for a given :code:`identifier` in this format:

                .. code-block:: json

                    [
                        {
                            "id": 1,
                            "name": "variant_1",
                            "version": "1",
                            "experimentName": "exp_1"

                        }
                    ]
            If an experiment has a variant of :code:`None`, it is not included
            in the returned list. All available experiments get bucketed.
            Exposure events are not emitted.

        However, experiments in Holdout Groups will still send an exposure for
        the holdout parent experiment, since it is not possible to
        manually expose the holdout later (because it's impossible to know if a
        returned :code:`None` or :code:`"control_1"` string
        came from the holdout group or its child experiment once this function exits).

        :param identifier: an arbitary string used to bucket the experiment by
            being set on :code:`DeciderContext`'s :code:`identifier_type` field.

        :param identifier_type: Sets :code:`{identifier_type: identifier}` on DeciderContext and
            should match an experiment's :code:`bucket_val` to get a variant.

        :return: list of experiment dicts with non-:code:`None` variants.
        """
        if identifier_type not in IDENTIFIERS:
            logger.warning(
                f'"{identifier_type}" is not one of supported "identifier_type": {IDENTIFIERS}.'
            )
            return []

        decider = self._get_decider()
        if decider is None:
            return []

        ctx = self._get_ctx_with_set_identifier(
            identifier=identifier, identifier_type=identifier_type
        )
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.info(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")
            return []

        all_decisions_result = decider.choose_all(ctx=ctx, identifier_type=identifier_type)

        error = all_decisions_result.err()
        if error:
            logger.info(f"Encountered error in decider.choose_all(): {error}")
            return []

        all_decisions = all_decisions_result.decisions()
        parsed_choices = []

        event_context_fields = self._decider_context.to_event_dict()

        for exp_name, decision in all_decisions.items():
            decision_error = decision.err()
            if decision_error:
                logger.info(
                    f"Encountered error for experiment: {exp_name} in decider.choose_all(): {decision_error}"
                )
                continue

            decision_dict = decision.decision_dict()

            if decision_dict:
                parsed_choices.append(self._format_decision(decision_dict))

            # expose Holdout if the experiment is part of one
            for event in decision.events():
                self._send_expose_if_holdout(
                    event=event, exposure_fields=event_context_fields, overwrite_identifier=True
                )

        return parsed_choices

    def _get_dynamic_config_value(
        self,
        feature_name: str,
        decider_func: Callable[[str, DeciderContext], Any],
        default: Any,
    ) -> Optional[Any]:
        ctx = self._get_ctx()
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.info(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")
            return None

        res = decider_func(feature_name, ctx)
        if res is None:
            return default
        error = res.err()
        if error:
            logger.warning(f"Encountered error {decider_func.__name__}: {error}")
            return default

        return res.val()

    def get_bool(self, feature_name: str, default: bool = False) -> bool:
        """Fetch a Dynamic Configuration of boolean type.

        :param feature_name: Name of the dynamic config you want a value for.

        :param default: what is returned if dynamic config is not active
            (:code:`False` unless overriden).

        :return: the boolean value of the dyanimc config if it is active/exists, :code:`default` parameter otherwise.
        """
        decider = self._get_decider()
        if not decider:
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_bool, default)

    def get_int(self, feature_name: str, default: int = 0) -> int:
        """Fetch a Dynamic Configuration of int type.

        :param feature_name: Name of the dynamic config you want a value for.

        :param default: what is returned if dynamic config is not active
            (:code:`0` unless overriden).

        :return: the int value of the dyanimc config if it is active/exists, :code:`default` parameter otherwise.
        """
        decider = self._get_decider()
        if not decider:
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_int, default)

    def get_float(self, feature_name: str, default: float = 0.0) -> float:
        """Fetch a Dynamic Configuration of float type.

        :param feature_name: Name of the dynamic config you want a value for.

        :param default: what is returned if dynamic config is not active
            (:code:`0.0` unless overriden).

        :return: the float value of the dyanimc config if it is active/exists, :code:`default` parameter otherwise.
        """
        decider = self._get_decider()
        if not decider:
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_float, default)

    def get_string(self, feature_name: str, default: str = "") -> str:
        """Fetch a Dynamic Configuration of string type.

        :param feature_name: Name of the dynamic config you want a value for.

        :param default: what is returned if dynamic config is not active
            (:code:`""` unless overriden).

        :return: the string value of the dyanimc config if it is active/exists, :code:`default` parameter otherwise.
        """
        decider = self._get_decider()
        if not decider:
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_string, default)

    def get_map(self, feature_name: str, default: Optional[dict] = None) -> Optional[dict]:
        """Fetch a Dynamic Configuration of map type.

        :param feature_name: Name of the dynamic config you want a value for.

        :param default: what is returned if dynamic config is not active
            (:code:`None` unless overriden).

        :return: the map value of the dyanimc config if it is active/exists, :code:`default` parameter otherwise.
        """
        decider = self._get_decider()
        if not decider:
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_map, default)

    def get_all_dynamic_configs(self) -> List[Dict[str, Any]]:
        """Return a list of dynamic configuration dicts in this format:

            .. code-block:: json

                [
                    {
                        "name": "example_dc",
                        "type": "float",
                        "value": 1.0
                    }
                ]

        where "type" field can be one of:

            .. code-block:: python

                "boolean", "integer", "float", "string", "map"

        Dynamic Configurations that are malformed, fail parsing, or otherwise
        error for any reason are included in the response and have their respective default
        values set:

        .. code-block:: python

            "boolean" -> False
            "integer" -> 0
            "float"   -> 0.0
            "string"  -> ""
            "map"     -> {}

        :return: list of all active dynamic config dicts.
        """
        decider = self._get_decider()
        if not decider:
            return []

        ctx = self._get_ctx()
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.info(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")
            return []

        all_decisions_result = decider.get_all_values(ctx)

        error = all_decisions_result.err()
        if error:
            logger.info(f"Encountered error in decider.choose_all(): {error}")
            return []

        all_decisions = all_decisions_result.decisions()
        parsed_configs = []

        for dc_name, decision in all_decisions.items():
            decision_error = decision.err()
            if decision_error:
                logger.info(
                    f"Encountered error for dynamic config: {dc_name} in decider.get_all_values(): {decision_error}"
                )
                continue

            value_dict = decision.value_dict()

            if value_dict:
                parsed_configs.append(value_dict)

        return parsed_configs

    def get_experiment(self, experiment_name: str) -> Optional[ExperimentConfig]:
        """Get an :py:class:`~reddit_decider.ExperimentConfig` `dataclass <https://github.com/reddit/experiments.py/blob/728a9501faceab7072f9d62f4e391fa4c34a68b1/reddit_decider/__init__.py#L39-L47>`_
        representation of an experiment or :code:`None` if not found.

        :param experiment_name: Name of the experiment to be fetched.

        :return: an :py:class:`~reddit_decider.ExperimentConfig` `dataclass <https://github.com/reddit/experiments.py/blob/728a9501faceab7072f9d62f4e391fa4c34a68b1/reddit_decider/__init__.py#L39-L47>`_
            representation of an experiment if found, else :code:`None`.
        """
        decider = self._get_decider()
        if decider is None:
            return None

        experiment = decider.get_experiment(experiment_name)
        error = experiment.err()
        if error:
            # sending to debug logger to avoid printing "Feature x not found." logs
            logger.debug(f"Encountered error in decider.get_experiment(): {error}")
            return None

        exp_dict = experiment.val()

        if exp_dict is None:
            return None

        return ExperimentConfig(
            id=int(exp_dict.get("id", 0)),
            name=exp_dict.get("name"),
            version=str(exp_dict.get("version")),
            bucket_val=exp_dict.get("variant_set", {}).get("bucket_val"),
            start_ts=exp_dict.get("variant_set", {}).get("start_ts"),
            stop_ts=exp_dict.get("variant_set", {}).get("stop_ts"),
            owner=exp_dict.get("owner"),
        )


class DeciderContextFactory(ContextFactory):
    """Decider client context factory.

    This factory will attach a new
    :py:class:`reddit_decider.Decider` to an attribute on the
    :py:class:`~baseplate.RequestContext`.

    :param path: Path to the experiment configuration file.
    :param event_logger: The logger to use to log experiment eligibility
        events. If not provided, a :py:class:`~baseplate.lib.events.DebugLogger`
        will be created and used.
    :param timeout: How long, in seconds, to block instantiation waiting
        for the watched experiments file to become available (defaults to not
        blocking).
    :param backoff: retry backoff time for experiments file watcher. Defaults to
        None, which is mapped to DEFAULT_FILEWATCHER_BACKOFF.
    :param request_field_extractor: an optional function used to populate fields such as
        "app_name" & "build_number" in DeciderContext() that may be used for targeting

    """

    def __init__(
        self,
        path: str,
        event_logger: Optional[EventLogger] = None,
        timeout: Optional[float] = None,
        backoff: Optional[float] = None,
        request_field_extractor: Optional[
            Callable[[RequestContext], Dict[str, Union[str, int, float, bool]]]
        ] = None,
    ):
        self._filewatcher = FileWatcher(
            path=path, parser=init_decider_parser, timeout=timeout, backoff=backoff
        )
        self._event_logger = event_logger
        self._request_field_extractor = request_field_extractor

    @staticmethod
    def _is_employee(edge_context: Any) -> bool:
        return (
            any([edge_context.user.has_role(role) for role in EMPLOYEE_ROLES])
            if edge_context.user.is_logged_in
            else False
        )

    @staticmethod
    def _prune_extracted_dict(extracted_dict: dict) -> dict:
        parsed_extracted_fields = deepcopy(extracted_dict)

        for k, v in extracted_dict.items():
            # remove invalid keys
            if k is None or not isinstance(k, str):
                logger.info(
                    f"{k} key in request_field_extractor() dict is not of type str and is removed."
                )
                del parsed_extracted_fields[k]
                continue
            # remove invalid values
            if not isinstance(v, (int, float, str, bool)) and v is not None:
                logger.info(
                    f"{k}: {v} value in `request_field_extractor()` dict is not one of type: [None, int, float, str, bool] and is removed."
                )
                del parsed_extracted_fields[k]
        return parsed_extracted_fields

    def _minimal_decider(
        self,
        internal: Optional[RustDecider],
        name: str,
        span: Span,
        parsed_extracted_fields: Optional[Dict] = None,
    ) -> Decider:
        return Decider(
            decider_context=DeciderContext(extracted_fields=parsed_extracted_fields),
            internal=internal,
            server_span=span,
            context_name=name,
            event_logger=self._event_logger,
        )

    def make_object_for_context(self, name: str, span: Span) -> Decider:
        rs_decider = None
        try:
            rs_decider = self._filewatcher.get_data()
        except WatchedFileNotAvailableError as exc:
            logger.error(f"Experiment config file unavailable: {exc}")

        if span is None:
            logger.debug("`span` is `None` in reddit_decider `make_object_for_context()`.")
            return self._minimal_decider(internal=rs_decider, name=name, span=span)

        request = None
        parsed_extracted_fields = None
        try:
            request = span.context

            if self._request_field_extractor:
                extracted_fields = self._request_field_extractor(request)
                # prune any invalid keys/values
                parsed_extracted_fields = self._prune_extracted_dict(
                    extracted_dict=extracted_fields
                )
        except Exception as exc:
            logger.info(
                f"Unable to extract fields from `request_field_extractor()` in `make_object_for_context()`. details: {exc}"
            )

        ec = None
        try:
            # if `edge_context` is inaccessible, bail early
            if request is None:
                return self._minimal_decider(
                    internal=rs_decider,
                    name=name,
                    span=span,
                    parsed_extracted_fields=parsed_extracted_fields,
                )

            ec = request.edge_context

            if ec is None:
                return self._minimal_decider(
                    internal=rs_decider,
                    name=name,
                    span=span,
                    parsed_extracted_fields=parsed_extracted_fields,
                )
        except Exception as exc:
            logger.info(
                f"Unable to access `request.edge_context` in `make_object_for_context()`. details: {exc}"
            )
            return self._minimal_decider(
                internal=rs_decider,
                name=name,
                span=span,
                parsed_extracted_fields=parsed_extracted_fields,
            )

        # All fields below are derived from `edge_context`

        user_id = None
        logged_in = None
        cookie_created_timestamp = None
        try:
            user_event_fields = ec.user.event_fields()
            user_id = user_event_fields.get("user_id")
            logged_in = user_event_fields.get("logged_in")
            cookie_created_timestamp = user_event_fields.get("cookie_created_timestamp")
        except Exception as exc:
            logger.info(
                f"Error while accessing `user.event_fields()` in `make_object_for_context()`. details: {exc}"
            )

        loid_created_timestamp = None
        try:
            if isinstance(ec.authentication_token, ValidatedAuthenticationToken):
                loid_cms = ec.authentication_token.loid_created_ms
                if loid_cms:
                    loid_created_timestamp = loid_cms
        except Exception as exc:
            logger.info(
                f"Unable to access `ec.authentication_token.loid_created_ms` in `make_object_for_context()`. details: {exc}"
            )

        oauth_client_id = None
        try:
            if isinstance(ec.authentication_token, ValidatedAuthenticationToken):
                oc_id = ec.authentication_token.oauth_client_id
                if oc_id:
                    oauth_client_id = oc_id
        except Exception as exc:
            logger.info(
                f"Unable to access `ec.authentication_token.oauth_client_id` in `make_object_for_context()`. details: {exc}"
            )

        country_code = None
        try:
            country_code = ec.geolocation.country_code
        except Exception as exc:
            logger.info(
                f"Unable to access `ec.geolocation.country_code` in `make_object_for_context()`. details: {exc}"
            )

        locale = None
        try:
            locale = ec.locale.locale_code
        except Exception as exc:
            logger.info(
                f"Unable to access `ec.locale.locale_code` in `make_object_for_context()`. details: {exc}"
            )

        origin_service = None
        try:
            origin_service = ec.origin_service.name
        except Exception as exc:
            logger.info(
                f"Unable to access `ec.origin_service.name` in `make_object_for_context()`. details: {exc}"
            )

        is_employee = None
        try:
            is_employee = self._is_employee(ec)
        except Exception as exc:
            logger.info(
                f"Error in `DeciderContextFactory.is_employee(ec)` in `make_object_for_context()`. details: {exc}"
            )

        device_id = None
        try:
            device_id = ec.device.id
        except Exception as exc:
            logger.info(
                f"Unable to access `ec.device.id` in `make_object_for_context()`. details: {exc}"
            )

        try:
            decider_context = DeciderContext(
                user_id=user_id,
                logged_in=logged_in,
                country_code=country_code,
                locale=locale,
                origin_service=origin_service,
                user_is_employee=is_employee,
                device_id=device_id,
                oauth_client_id=oauth_client_id,
                cookie_created_timestamp=cookie_created_timestamp,
                loid_created_timestamp=loid_created_timestamp,
                extracted_fields=parsed_extracted_fields,
            )
        except Exception as exc:
            logger.warning(
                "Could not create full DeciderContext() (defaulting to empty DeciderContext()): %s",
                str(exc),
            )
            decider_context = DeciderContext()

        return Decider(
            decider_context=decider_context,
            internal=rs_decider,
            server_span=span,
            context_name=name,
            event_logger=self._event_logger,
        )


class DeciderClient(config.Parser):
    """Configure a decider client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`decider_client_from_config` for available configuration settings.

    :param event_logger: The EventLogger instance to be used to log bucketing events.

    :param prefix: the prefix used to filter config keys (defaults to "experiments.").

    :param request_field_extractor: (optional) function used to populate fields such as
        :code:`"app_name"` & :code:`"build_number"` in :code:`DeciderContext()` that may be used for targeting
    """

    def __init__(
        self,
        event_logger: EventLogger,
        prefix: str = "experiments.",
        request_field_extractor: Optional[
            Callable[[RequestContext], Dict[str, Union[str, int, float, bool]]]
        ] = None,
    ):
        self._prefix = prefix
        self._event_logger = event_logger
        self._request_field_extractor = request_field_extractor

    def parse(self, _key_path: str, raw_config: config.RawConfig) -> DeciderContextFactory:
        # `_key_path` is ignored for prefix because most services will not change `app_config`
        # to use "decider" key, so using `prefix` from `__init__`
        return decider_client_from_config(
            app_config=raw_config,
            event_logger=self._event_logger,
            prefix=self._prefix,
            request_field_extractor=self._request_field_extractor,
        )


def decider_client_from_config(
    app_config: config.RawConfig,
    event_logger: EventLogger,
    prefix: str = "experiments.",
    request_field_extractor: Optional[
        Callable[[RequestContext], Dict[str, Union[str, int, float, bool]]]
    ] = None,
) -> DeciderContextFactory:
    """Configure and return an :py:class:`DeciderContextFactory` object.

    The keys used in your app's :code:`some_config.ini` file should be prefixed, e.g.
    ``experiments.path``, etc.

    Supported config keys:

        ``path`` (optional)
            The path to the experiment configuration file generated by the
            experiment configuration fetcher daemon.
        ``timeout`` (optional)
            The time that we should wait for the file specified by ``path`` to
            exist.  Defaults to `None` which is `infinite`.
        ``backoff`` (optional)
            The base amount of time for exponential backoff when trying to find the
            experiments config file. Defaults to no backoff between tries.

    :param app_config: The application configuration which should have
        settings for the decider client.
    :param event_logger: The EventLogger to be used to log bucketing events.
    :param prefix: the prefix used to filter keys (defaults to "experiments.").
    :param request_field_extractor: (optional) function used to populate fields such as
        "app_name" & "build_number" in DeciderContext() that may be used for targeting
    """
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]

    cfg = config.parse_config(
        app_config,
        {
            config_prefix: {
                "path": config.Optional(config.String, default="/var/local/experiments.json"),
                "timeout": config.Optional(config.Timespan),
                "backoff": config.Optional(config.Timespan),
            }
        },
    )
    options = getattr(cfg, config_prefix)

    # pylint: disable=maybe-no-member
    if options.timeout:
        timeout = options.timeout.total_seconds()
    else:
        timeout = None

    if options.backoff:
        backoff = options.backoff.total_seconds()
    else:
        backoff = None

    return DeciderContextFactory(
        path=options.path,
        event_logger=event_logger,
        timeout=timeout,
        backoff=backoff,
        request_field_extractor=request_field_extractor,
    )
