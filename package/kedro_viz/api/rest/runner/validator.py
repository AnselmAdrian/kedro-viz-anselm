"""Command and config validation."""

from __future__ import annotations

from kedro_viz.api.rest.runner.models import RunConfig, ValidationResult


def validate_raw_command(cmd_parts: list[str]) -> ValidationResult:
    """Validates a shlex-split command for basic safety.

    Rules:
    - Only 'kedro run' verb is allowed
    - Reject shell metacharacters: ;, &&, ||, |, backticks, $()
    - Return ValidationResult with errors list

    Args:
        cmd_parts: The shlex-split command parts (e.g. ["kedro", "run", "--pipeline=x"]).

    Returns:
        A ValidationResult indicating whether the command is safe to execute.
    """
    ALLOWED_VERBS = {"run"}
    DANGEROUS_CHARS = {";", "&&", "||", "|", "`", "$("}

    if not cmd_parts:
        return ValidationResult(valid=False, errors=["Empty command"])

    # Determine the verb: if the first part is "kedro", the verb is the second part
    if cmd_parts[0] == "kedro" and len(cmd_parts) > 1:
        verb = cmd_parts[1]
    elif cmd_parts[0] == "kedro" and len(cmd_parts) == 1:
        # Just "kedro" with no subcommand
        return ValidationResult(
            valid=False, errors=["Only 'kedro run' is permitted, got 'kedro'"]
        )
    else:
        verb = cmd_parts[0]

    if verb not in ALLOWED_VERBS:
        return ValidationResult(
            valid=False, errors=[f"Only 'kedro run' is permitted, got '{verb}'"]
        )

    # Check for dangerous shell characters in each part
    for part in cmd_parts:
        for d in DANGEROUS_CHARS:
            if d in part:
                return ValidationResult(
                    valid=False,
                    errors=[f"Disallowed shell characters in command: '{d}'"],
                )

    return ValidationResult(valid=True, errors=[])


class RunValidator:
    """Validates a RunConfig against the loaded Kedro project.

    Uses the DataAccessManager to check that referenced pipelines,
    nodes, and tags actually exist in the project.
    """

    def __init__(self, data_access_manager):
        """
        Args:
            data_access_manager: The DAM instance providing pipeline/node metadata.
        """
        self.dam = data_access_manager

    def validate(self, config: RunConfig) -> ValidationResult:
        """Validate a RunConfig against the loaded project.

        Checks:
        - Pipeline exists (if specified)
        - from_nodes exist in target pipeline
        - to_nodes exist in target pipeline
        - Tags match at least one node (warning, not error)

        Returns:
            ValidationResult with errors and warnings.
        """
        errors = []
        warnings = []

        # Check pipeline exists
        if config.pipeline:
            pipeline_ids = self.dam.registered_pipelines.get_pipeline_ids()
            if config.pipeline not in pipeline_ids:
                errors.append(
                    f"Pipeline '{config.pipeline}' not found. "
                    f"Available: {sorted(pipeline_ids)}"
                )
                # Can't validate nodes if pipeline doesn't exist
                return ValidationResult(valid=False, errors=errors, warnings=warnings)

        # Get nodes for target pipeline
        target_pipeline = config.pipeline or "__default__"
        pipeline_nodes = self._get_node_names_for_pipeline(target_pipeline)

        # Check from_nodes exist
        if config.from_nodes:
            invalid = [n for n in config.from_nodes if n not in pipeline_nodes]
            if invalid:
                errors.append(
                    f"Nodes not found in pipeline '{target_pipeline}': {invalid}"
                )

        # Check to_nodes exist
        if config.to_nodes:
            invalid = [n for n in config.to_nodes if n not in pipeline_nodes]
            if invalid:
                errors.append(
                    f"Nodes not found in pipeline '{target_pipeline}': {invalid}"
                )

        # Check tags (warning only)
        if config.tags:
            all_tags = self._get_tags_for_pipeline(target_pipeline)
            invalid_tags = [t for t in config.tags if t not in all_tags]
            if invalid_tags:
                warnings.append(f"Tags matched 0 nodes: {invalid_tags}")

        return ValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings)

    def _get_node_names_for_pipeline(self, pipeline_id: str) -> set[str]:
        """Get node names for a pipeline from the DAM.

        Only returns names of task nodes (not data nodes).
        """
        try:
            nodes = self.dam.get_nodes_for_registered_pipeline(pipeline_id)
            return {n.name for n in nodes if n.type == "task"} if nodes else set()
        except (AttributeError, KeyError):
            return set()

    def _get_tags_for_pipeline(self, pipeline_id: str) -> set[str]:
        """Get all tags used by task nodes in a pipeline from the DAM."""
        try:
            nodes = self.dam.get_nodes_for_registered_pipeline(pipeline_id)
            tags = set()
            for node in nodes:
                if node.type == "task" and node.tags:
                    tags.update(node.tags)
            return tags
        except (AttributeError, KeyError):
            return set()
