"""Tests for command and config validation."""

import pytest

from kedro_viz.api.rest.runner.validator import validate_raw_command


class TestValidateRawCommand:
    """Tests for validate_raw_command function."""

    # --- Valid commands ---

    def test_valid_kedro_run(self):
        result = validate_raw_command(["kedro", "run"])
        assert result.valid is True
        assert result.errors == []

    def test_valid_kedro_run_with_pipeline(self):
        result = validate_raw_command(["kedro", "run", "--pipeline=data_processing"])
        assert result.valid is True
        assert result.errors == []

    def test_valid_kedro_run_with_multiple_args(self):
        result = validate_raw_command(
            ["kedro", "run", "--pipeline=x", "--env=base", "--to-nodes=my_node"]
        )
        assert result.valid is True
        assert result.errors == []

    def test_valid_kedro_run_with_params(self):
        result = validate_raw_command(
            ["kedro", "run", "--params=learning_rate:0.01,epochs:100"]
        )
        assert result.valid is True
        assert result.errors == []

    def test_valid_run_verb_without_kedro_prefix(self):
        """When cmd_parts doesn't start with 'kedro', the verb is the first element."""
        result = validate_raw_command(["run", "--pipeline=x"])
        assert result.valid is True
        assert result.errors == []

    # --- Invalid verb ---

    def test_invalid_verb_info(self):
        result = validate_raw_command(["kedro", "info"])
        assert result.valid is False
        assert any("Only 'kedro run' is permitted" in e for e in result.errors)

    def test_invalid_verb_catalog_list(self):
        result = validate_raw_command(["kedro", "catalog"])
        assert result.valid is False
        assert any("Only 'kedro run' is permitted" in e for e in result.errors)

    def test_invalid_verb_new(self):
        result = validate_raw_command(["kedro", "new"])
        assert result.valid is False
        assert any("Only 'kedro run' is permitted" in e for e in result.errors)

    def test_invalid_verb_test(self):
        result = validate_raw_command(["kedro", "test"])
        assert result.valid is False
        assert any("Only 'kedro run' is permitted" in e for e in result.errors)

    def test_kedro_only_no_subcommand(self):
        result = validate_raw_command(["kedro"])
        assert result.valid is False
        assert any("Only 'kedro run' is permitted" in e for e in result.errors)

    # --- Dangerous characters ---

    def test_dangerous_semicolon(self):
        """Semicolons in arguments are rejected (caught by verb or char check)."""
        result = validate_raw_command(["kedro", "run", "--pipeline=x;y"])
        assert result.valid is False
        assert any("Disallowed shell characters" in e for e in result.errors)

    def test_dangerous_semicolon_in_verb(self):
        """'run;' is not a valid verb, so it's rejected."""
        result = validate_raw_command(["kedro", "run;", "rm", "-rf", "/"])
        assert result.valid is False
        assert any("Only 'kedro run' is permitted" in e for e in result.errors)

    def test_dangerous_and_operator(self):
        result = validate_raw_command(["kedro", "run", "&&", "echo", "hacked"])
        assert result.valid is False
        assert any("Disallowed shell characters" in e for e in result.errors)

    def test_dangerous_or_operator(self):
        result = validate_raw_command(["kedro", "run", "||", "echo", "hacked"])
        assert result.valid is False
        assert any("Disallowed shell characters" in e for e in result.errors)

    def test_dangerous_pipe(self):
        result = validate_raw_command(["kedro", "run", "|", "grep", "x"])
        assert result.valid is False
        assert any("Disallowed shell characters" in e for e in result.errors)

    def test_dangerous_backtick(self):
        result = validate_raw_command(["kedro", "run", "`whoami`"])
        assert result.valid is False
        assert any("Disallowed shell characters" in e for e in result.errors)

    def test_dangerous_dollar_paren(self):
        result = validate_raw_command(["kedro", "run", "$(id)"])
        assert result.valid is False
        assert any("Disallowed shell characters" in e for e in result.errors)

    def test_dangerous_chars_embedded_in_arg(self):
        """Dangerous chars embedded within a longer argument should be caught."""
        result = validate_raw_command(["kedro", "run", "--pipeline=x;y"])
        assert result.valid is False
        assert any("Disallowed shell characters" in e for e in result.errors)

    # --- Empty command ---

    def test_empty_command(self):
        result = validate_raw_command([])
        assert result.valid is False
        assert any("Empty command" in e for e in result.errors)


from unittest.mock import MagicMock, PropertyMock

from kedro_viz.api.rest.runner.models import RunConfig
from kedro_viz.api.rest.runner.validator import RunValidator


def _make_task_node(name, tags=None):
    """Create a mock task node with name and tags."""
    node = MagicMock()
    node.name = name
    node.type = "task"
    node.tags = set(tags) if tags else set()
    return node


def _make_data_node(name):
    """Create a mock data node (should be excluded from node-name validation)."""
    node = MagicMock()
    node.name = name
    node.type = "data"
    node.tags = set()
    return node


def _make_dam(pipeline_ids, nodes_by_pipeline=None):
    """Create a mock DataAccessManager.

    Args:
        pipeline_ids: List of available pipeline IDs.
        nodes_by_pipeline: Dict mapping pipeline_id -> list of mock nodes.
    """
    dam = MagicMock()
    dam.registered_pipelines.get_pipeline_ids.return_value = pipeline_ids

    if nodes_by_pipeline is None:
        nodes_by_pipeline = {}

    def get_nodes(pipeline_id="__default__"):
        return nodes_by_pipeline.get(pipeline_id, [])

    dam.get_nodes_for_registered_pipeline.side_effect = get_nodes
    return dam


class TestRunValidator:
    """Tests for RunValidator class."""

    def test_valid_config_all_fields_exist(self):
        """Valid config where pipeline, nodes, and tags all exist."""
        nodes = [
            _make_task_node("clean_data", tags=["preprocessing"]),
            _make_task_node("train_model", tags=["training"]),
            _make_task_node("evaluate", tags=["training", "evaluation"]),
        ]
        dam = _make_dam(
            pipeline_ids=["__default__", "data_processing", "data_science"],
            nodes_by_pipeline={"data_science": nodes},
        )

        validator = RunValidator(dam)
        config = RunConfig(
            pipeline="data_science",
            from_nodes=["clean_data"],
            to_nodes=["evaluate"],
            tags=["training"],
        )
        result = validator.validate(config)

        assert result.valid is True
        assert result.errors == []
        assert result.warnings == []

    def test_invalid_pipeline_returns_error(self):
        """Specifying a non-existent pipeline returns an error."""
        dam = _make_dam(pipeline_ids=["__default__", "data_processing"])

        validator = RunValidator(dam)
        config = RunConfig(pipeline="nonexistent_pipeline")
        result = validator.validate(config)

        assert result.valid is False
        assert len(result.errors) == 1
        assert "nonexistent_pipeline" in result.errors[0]
        assert "not found" in result.errors[0].lower()

    def test_invalid_from_nodes_returns_error(self):
        """Specifying from_nodes that don't exist returns an error."""
        nodes = [
            _make_task_node("node_a"),
            _make_task_node("node_b"),
        ]
        dam = _make_dam(
            pipeline_ids=["__default__"],
            nodes_by_pipeline={"__default__": nodes},
        )

        validator = RunValidator(dam)
        config = RunConfig(from_nodes=["node_a", "nonexistent_node"])
        result = validator.validate(config)

        assert result.valid is False
        assert len(result.errors) == 1
        assert "nonexistent_node" in result.errors[0]

    def test_invalid_to_nodes_returns_error(self):
        """Specifying to_nodes that don't exist returns an error."""
        nodes = [
            _make_task_node("node_a"),
            _make_task_node("node_b"),
        ]
        dam = _make_dam(
            pipeline_ids=["__default__"],
            nodes_by_pipeline={"__default__": nodes},
        )

        validator = RunValidator(dam)
        config = RunConfig(to_nodes=["node_b", "missing_node"])
        result = validator.validate(config)

        assert result.valid is False
        assert len(result.errors) == 1
        assert "missing_node" in result.errors[0]

    def test_invalid_tags_returns_warning_not_error(self):
        """Tags that don't match any nodes produce a warning, not an error."""
        nodes = [
            _make_task_node("node_a", tags=["real_tag"]),
        ]
        dam = _make_dam(
            pipeline_ids=["__default__"],
            nodes_by_pipeline={"__default__": nodes},
        )

        validator = RunValidator(dam)
        config = RunConfig(tags=["real_tag", "fake_tag"])
        result = validator.validate(config)

        assert result.valid is True
        assert result.errors == []
        assert len(result.warnings) == 1
        assert "fake_tag" in result.warnings[0]

    def test_no_pipeline_uses_default(self):
        """When no pipeline is specified, validation uses __default__."""
        nodes = [
            _make_task_node("default_node"),
        ]
        dam = _make_dam(
            pipeline_ids=["__default__", "other"],
            nodes_by_pipeline={"__default__": nodes},
        )

        validator = RunValidator(dam)
        config = RunConfig(from_nodes=["default_node"])
        result = validator.validate(config)

        assert result.valid is True
        assert result.errors == []
        # Verify it queried the __default__ pipeline
        dam.get_nodes_for_registered_pipeline.assert_called_with("__default__")

    def test_empty_config_is_valid(self):
        """A config with all None fields is valid (runs entire default pipeline)."""
        dam = _make_dam(
            pipeline_ids=["__default__"],
            nodes_by_pipeline={"__default__": [_make_task_node("some_node")]},
        )

        validator = RunValidator(dam)
        config = RunConfig()
        result = validator.validate(config)

        assert result.valid is True
        assert result.errors == []
        assert result.warnings == []

    def test_data_nodes_excluded_from_node_name_validation(self):
        """Data nodes should not be considered when validating from_nodes/to_nodes."""
        nodes = [
            _make_task_node("task_node"),
            _make_data_node("data_node"),
        ]
        dam = _make_dam(
            pipeline_ids=["__default__"],
            nodes_by_pipeline={"__default__": nodes},
        )

        validator = RunValidator(dam)
        # Referencing a data node name as a from_node should fail
        config = RunConfig(from_nodes=["data_node"])
        result = validator.validate(config)

        assert result.valid is False
        assert "data_node" in result.errors[0]

    def test_constructor_stores_dam(self):
        """RunValidator stores the data_access_manager as constructor arg."""
        dam = MagicMock()
        validator = RunValidator(dam)
        assert validator.dam is dam
