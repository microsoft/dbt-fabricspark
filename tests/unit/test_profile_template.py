from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE_TEMPLATE = REPO_ROOT / "src" / "dbt" / "include" / "fabricspark" / "profile_template.yml"


def test_profile_template_is_valid_yaml() -> None:
    template = yaml.safe_load(PROFILE_TEMPLATE.read_text(encoding="utf-8"))

    workspace_prompt = template["prompts"]["_choose_authentication_method"]["livy"][
        "workspace_name"
    ]
    assert workspace_prompt["hint"].endswith(
        "Precedence: model config(workspace_name=...) > this value."
    )

    session = template["prompts"]["_choose_authentication_method"]["session"]
    assert session == {
        "_fixed_method": "session",
        "_fixed_spark_config": {"name": "dbt-fabricspark-session"},
    }
