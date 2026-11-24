"""The preflight (scripts/preflight.py): table-driven, with a fake command runner. Nothing touches the machine, AWS,
Databricks or GitHub; the runner answers from a table of (command prefix -> exit code, output)."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/preflight.py"
spec = importlib.util.spec_from_file_location("preflight", SCRIPT)
pf = importlib.util.module_from_spec(spec)
sys.modules["preflight"] = pf
spec.loader.exec_module(pf)


def runner(answers: dict[str, tuple[int, str]]):
    """Answer a command by the longest key that its text starts with; anything else is 'not installed'."""
    def run(cmd: list[str], cwd: str | None = None) -> tuple[int, str]:
        text = " ".join(cmd)
        hits = [k for k in answers if text.startswith(k)]
        return answers[max(hits, key=len)] if hits else (127, f"{cmd[0]}: not installed")
    return run


TOOLS = {"aws --version": (0, "aws-cli/2.15.0"), "databricks --version": (0, "Databricks CLI v0.250.0")}


def ctx(answers, tmp_path=None, **kw):
    """The AWS and Databricks CLIs are installed unless a test says otherwise (checks that need them depend on that)."""
    root = tmp_path or Path("/nonexistent")
    return pf.Ctx(run=runner({**TOOLS, **answers}), root=root, env=kw.pop("env", {}), **kw)


@pytest.mark.parametrize(
    ("case", "answers", "expect_status", "expect_in"),
    [
        ("terraform new enough", {"terraform version": (0, "Terraform v1.10.3")}, "pass", "1.10.3"),
        ("terraform too old", {"terraform version": (0, "Terraform v1.5.7")}, "fail", "need 1.10"),
        ("terraform missing", {}, "fail", "not installed"),
        ("unreadable version", {"terraform version": (0, "no numbers here")}, "fail", "unreadable"),
    ],
)
def test_tool_version(case, answers, expect_status, expect_in):
    status, detail = pf.tool(["terraform", "version"], (1, 10))(ctx(answers))
    assert status == expect_status and expect_in in detail


@pytest.mark.parametrize(
    ("case", "answers", "status_by_id"),
    [
        ("all good", {"aws sts": (0, "arn:aws:iam::123456789012:user/me"), "aws s3api": (0, "x"), "aws iam": (0, "x")},
         {"A1": "pass", "A2": "pass", "A3": "pass"}),
        ("not signed in: everything that needs it is skipped",
         {"aws sts": (255, "Unable to locate credentials")},
         {"A1": "fail", "A2": "skip", "A3": "skip", "A9": "skip"}),
        ("a policy denies S3 only", {"aws sts": (0, "arn"), "aws s3api list-buckets": (254, "AccessDenied: explicit deny"),
                                     "aws iam": (0, "x")},
         {"A1": "pass", "A2": "fail", "A3": "pass", "A9": "skip"}),
    ],
)
def test_aws_checks_follow_their_dependencies(case, answers, status_by_id):
    rows = {r["id"]: r for r in pf.run_checks(ctx(answers), {"aws"})}
    for cid, status in status_by_id.items():
        assert rows[cid]["status"] == status, (cid, rows[cid])


def test_the_account_number_is_never_printed():
    status, detail = pf.aws_identity(ctx({"aws sts": (0, "arn:aws:iam::123456789012:user/me")}))
    assert status == "pass" and "123456789012" not in detail and "<account>" in detail


def test_optional_failures_are_warnings_and_do_not_fail_the_run():
    rows = pf.run_checks(ctx({"databricks current-user": (0, "me"), "databricks postgres": (1, "no"),
                              "databricks database": (1, "no")}), {"databricks"})
    d5 = next(r for r in rows if r["id"] == "D5")
    assert d5["status"] == "warn" and d5["fix"]


def test_a_check_that_crashes_is_a_failed_check_not_a_crashed_preflight():
    boom = pf.Check("X1", "tools", "boom", "none", lambda c: 1 / 0)
    rows = pf.run_checks(ctx({}), None, (boom,))
    assert rows[0]["status"] == "fail" and "the check itself failed" in rows[0]["detail"]


def test_variants_hide_checks_that_do_not_apply():
    aws = {r["id"] for r in pf.run_checks(ctx({}, variant="aws"))}
    free = {r["id"] for r in pf.run_checks(ctx({}, variant="free-edition"))}
    assert "T3" in aws and "T3" not in free and "R2" not in free


@pytest.mark.parametrize(
    ("case", "files", "expect"),
    [
        ("only the tracked file exists", {"dev.tfvars": 'name_prefix = "acme-dp"\n'}, "fail"),
        ("local file with the placeholder prefix", {"dev.local.tfvars": 'name_prefix = "changeme-dp"\n'}, "fail"),
        ("local file with zero ids", {"dev.local.tfvars": 'metastore_id = "00000000-0000-0000-0000-000000000000"\n'}, "fail"),
        ("local file filled in", {"dev.local.tfvars": 'name_prefix = "acme-dp"\n'}, "pass"),
    ],
)
def test_real_values_come_from_the_git_ignored_local_file(tmp_path, case, files, expect):
    d = tmp_path / "realms/aws-databricks/infra/envs"
    d.mkdir(parents=True)
    for name, text in files.items():
        (d / name).write_text(text)
    assert pf.placeholders_filled(ctx({}, tmp_path))[0] == expect


@pytest.mark.parametrize(
    ("case", "text", "expect"),
    [
        ("everything ignored", "*.local.tfvars\n.env\n*.pem\n*.tfstate\n.databricks/\n", "pass"),
        ("a pattern missing", "*.local.tfvars\n.env\n", "fail"),
        ("no gitignore", None, "fail"),
    ],
)
def test_gitignore_covers_secret_files(tmp_path, case, text, expect):
    if text is not None:
        (tmp_path / ".gitignore").write_text(text)
    assert pf.git_ignores_secrets(ctx({}, tmp_path))[0] == expect


@pytest.mark.parametrize(
    ("case", "content", "expect"),
    [
        ("clean file", "name_prefix = 'acme'\n", "pass"),
        ("an AWS key id", "key = AKIAABCDEFGHIJKLMNOP\n", "fail"),
        ("a private key block", "-----BEGIN RSA PRIVATE KEY-----\n", "fail"),
        ("a databricks token", "t = dapi" + "0123456789abcdef0123456789abcdef", "fail"),
        ("a password assigned", 'password = "hunter2hunter2"\n', "fail"),
    ],
)
def test_secret_scan_of_tracked_files_never_prints_the_value(tmp_path, case, content, expect):
    (tmp_path / "f.txt").write_text(content)
    status, detail = pf.no_secrets_tracked(ctx({"git -C": (0, "f.txt")}, tmp_path))
    assert status == expect
    assert "AKIAABCDEFGHIJKLMNOP" not in detail and "hunter2hunter2" not in detail


def _tfvars(tmp_path, prefix="acme-dp"):
    d = tmp_path / "realms/aws-databricks/infra/envs"
    d.mkdir(parents=True, exist_ok=True)
    (d / "dev.tfvars").write_text(f'name_prefix = "{prefix}"\n')


@pytest.mark.parametrize(
    ("case", "out", "expect"),
    [
        ("a bucket with the prefix", "acme-dp-dev-landing other-bucket", "pass"),
        ("only other buckets", "other-bucket another", "fail"),
    ],
)
def test_buckets_found_by_prefix(tmp_path, case, out, expect):
    _tfvars(tmp_path)
    check = pf.named_in(["s3api", "list-buckets"], r"^{prefix}")
    assert check(ctx({"aws s3api list-buckets": (0, out)}, tmp_path))[0] == expect


def test_deployed_checks_need_a_prefix(tmp_path):
    assert pf.named_in(["s3api", "list-buckets"], r"^{prefix}")(ctx({"aws": (0, "x")}, tmp_path))[0] == "fail"


@pytest.mark.parametrize(
    ("case", "missing", "expect"),
    [("all five", set(), "pass"), ("silver missing", {"dev_silver"}, "fail"), ("two missing", {"dev_ops", "dev_gold"}, "fail")],
)
def test_catalogs(case, missing, expect):
    answers = {f"databricks catalogs get dev_{layer}": (0 if f"dev_{layer}" not in missing else 1, "")
               for layer in ("landing", "bronze", "silver", "gold", "ops")}
    status, detail = pf.catalogs_exist(ctx(answers))
    assert status == expect and all(m in detail for m in missing)


@pytest.mark.parametrize(
    ("case", "answers", "expect"),
    [
        ("project listed", {"databricks postgres list-projects": (0, json.dumps([{"name": "dp-meta-dev"}]))}, "pass"),
        ("older instance listed", {"databricks postgres": (1, "x"),
                                   "databricks database list-database-instances": (0, '[{"name": "acme-dev-meta"}]')}, "pass"),
        ("listed but not ours", {"databricks postgres list-projects": (0, '[{"name": "other"}]'),
                                 "databricks database": (0, "[]")}, "fail"),
        ("cannot list at all", {}, "fail"),
    ],
)
def test_lakebase_named(case, answers, expect):
    assert pf.lakebase_named(ctx(answers))[0] == expect


def test_render_names_the_fix_and_counts():
    rows = [{"id": "A1", "group": "aws", "title": "signed in", "status": "fail", "detail": "no credentials", "fix": "log in"},
            {"id": "A2", "group": "aws", "title": "s3", "status": "skip", "detail": "needs A1 first", "fix": ""}]
    text = pf.render(rows)
    assert "fix A1: log in" in text and "0 passed, 1 failed, 0 warnings, 1 skipped" in text


def test_exit_code_follows_failures(monkeypatch, capsys):
    monkeypatch.setattr(pf, "_run", lambda cmd: (127, "not installed"))
    assert pf.main(["--only", "tools"]) == 1
    monkeypatch.setattr(pf, "_run", lambda cmd: (0, "version 99.0.0"))
    assert pf.main(["--only", "tools", "--variant", "free-edition"]) == 0
    capsys.readouterr()


def test_env_name_is_checked():
    with pytest.raises(SystemExit):
        pf.main(["--env", "../etc"])


# ------------------------------------------------------------------------------------------------------------ gates
def test_every_check_has_a_stage_and_every_stage_has_a_check():
    assert {c.id for c in pf.CHECKS} <= set(pf.STAGE_OF)
    assert {s for s in pf.STAGE_OF.values()} == {g for g, _ in pf.GATES}
    assert set(pf.STAGE_OF) <= {c.id for c in pf.CHECKS}


@pytest.mark.parametrize(
    ("case", "gate", "stages_seen"),
    [
        ("G0 stops at tools and the checkout", "G0", {"G0"}),
        ("G3 includes everything before it", "G3", {"G0", "G1", "G2", "G3"}),
        ("no gate runs all stages", "", {g for g, _ in pf.GATES}),
    ],
)
def test_a_gate_runs_its_stage_and_all_earlier_ones(case, gate, stages_seen):
    rows = pf.run_checks(ctx({}), None, gate=gate)
    assert {r["stage"] for r in rows} == stages_seen or (not gate and {r["stage"] for r in rows} <= stages_seen)


def test_an_unknown_gate_is_refused():
    with pytest.raises(ValueError, match="unknown gate"):
        pf.run_checks(ctx({}), None, gate="G9")


def test_a_failed_early_check_blocks_the_later_stage_checks_that_need_it():
    rows = {r["id"]: r for r in pf.run_checks(ctx({"aws sts": (255, "no credentials")}), None, gate="G3")}
    assert rows["A1"]["status"] == "fail"
    assert rows["P1"]["status"] == "skip" and "A1" in rows["P1"]["detail"]


@pytest.mark.parametrize(
    ("case", "answers", "check", "expect"),
    [
        ("job deployed", {"databricks jobs list": (0, '[{"name": "dp-dev-engine-days"}]')}, "jobs_exist", "pass"),
        ("job missing", {"databricks jobs list": (0, "[]")}, "jobs_exist", "fail"),
        ("first run left a folder", {"databricks fs ls": (0, '[{"path": "r1"}]')}, "first_run_left_evidence", "pass"),
        ("no run folder", {"databricks fs ls": (0, "[]")}, "first_run_left_evidence", "fail"),
    ],
)
def test_stage_five_and_six_checks(case, answers, check, expect):
    assert getattr(pf, check)(ctx(answers))[0] == expect


@pytest.mark.parametrize(
    ("case", "name", "content", "expect"),
    [
        ("the documented example key", "cfg.toml", "AKIAIOSFODNN7EXAMPLE", "pass"),
        ("a made-up password in a test", "tests/t.py", 'password = "fixture-password"', "pass"),
        ("the same password outside tests", "src/x.py", 'password = "fixture-password"', "fail"),
        ("a real-looking key in a test is still caught", "tests/t.py", "AKIAABCDEFGHIJKLMNOP", "fail"),
    ],
)
def test_secret_scan_knows_examples_and_fixtures(tmp_path, case, name, content, expect):
    p = tmp_path / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)
    assert pf.no_secrets_tracked(ctx({"git -C": (0, name)}, tmp_path))[0] == expect


@pytest.mark.parametrize(
    ("case", "text", "must_not_contain"),
    [
        ("an account number in an error", "User: arn:aws:iam::477165397616:user/x is not authorized", "477165397616"),
        ("an e-mail address", "signed in as someone.name@example.com now", "someone.name@example.com"),
        ("both", "477165397616 and a.b@c.org", "c.org"),
    ],
)
def test_details_never_show_account_numbers_or_emails(case, text, must_not_contain):
    assert must_not_contain not in pf.short(text)


def test_a_passing_command_shows_none_of_its_output():
    status, detail = pf.ok_if_command_works(lambda c: ["databricks", "current-user", "me"])(
        ctx({"databricks current-user": (0, '{"emails": [{"value": "a.b@c.org"}]}')}))
    assert (status, detail) == ("pass", "ok")
