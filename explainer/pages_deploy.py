"""Deployment: one page that is both the guide and the runbook, from a fresh checkout to a first run."""
from lib import *  # noqa: F401,F403

AUTO, MANUAL, BOTH = "automated", "manual", "manual, then automated"

GATES = [
    ["G0", "tools, the catalog checkout, no keys in git", "<code>python3 scripts/preflight.py --gate G0</code>"],
    ["G1", "access: AWS, Databricks, GitHub (and which services a policy denies)", "<code>--gate G1 --databricks-profile NAME --aws-profile NAME</code>"],
    ["G2", "after the account setup: state bucket and CI roles exist", "<code>--gate G2</code>"],
    ["G3", "after the infrastructure deploy: buckets, keys, secrets, catalogs, volume, external locations", "<code>--gate G3 --env dev</code>"],
    ["G4", "after the metadata store setup: the Lakebase database exists", "<code>--gate G4 --env dev</code>"],
    ["G5", "after the bundle deploy: the jobs exist and validate", "<code>--gate G5 --env dev</code>"],
    ["G6", "after the first run: a report is in the volume", "<code>--gate G6 --env dev</code>"],
]

STEPS = [
    ["0", "Check out the code and the catalog; install Python, uv, Terraform 1.10+, the AWS CLI v2, the Databricks CLI, rsync",
     MANUAL, "Installing tools is a person's job.", "G0"],
    ["1", "Run the offline checks: tests, bundle check, Terraform validate",
     AUTO, "<code>make -C realms/aws-databricks check</code>. Needs no account.", "G0"],
    ["2", "Sign in to AWS: <code>aws configure sso</code>, then <code>aws sso login</code>. If a policy denies a service, ask the account owner",
     MANUAL, "The preflight lists exactly which services answer and which are denied.", "G1"],
    ["3", "Create the Databricks account on AWS. Do not create a workspace in the quickstart. Note the account id",
     MANUAL, "Terraform creates the workspaces.", "G1"],
    ["4", "In the account console: group <code>platform-admins</code>, service principal <code>dp-ci</code> (account admin, in the group), a federation policy for GitHub Actions",
     MANUAL, "Not created by Terraform. Terraform needs an admin identity to exist first.", "G1"],
    ["5", "Make your local values file: copy <code>infra/envs/dev.tfvars</code> to <code>dev.local.tfvars</code> and fill in the prefix and ids",
     MANUAL, "The <code>.local.tfvars</code> file is git-ignored. Real values never go in a tracked file.", "G0"],
    ["6", "Sign in to Databricks: <code>databricks auth login --host https://accounts.cloud.databricks.com --account-id ID</code>, and a profile for the workspace later",
     MANUAL, "Opens a browser.", "G1"],
    ["7", "<code>make bootstrap</code> (state bucket), then <code>make account</code> (GitHub OIDC roles, the regional metastore, tags). Copy <code>metastore_id</code> into your local file",
     AUTO, "Terraform and the AWS CLI do it. A person starts it once and copies two outputs.", "G2"],
    ["8", "Lakebase: check it is offered in the region. If not, set <code>lakebase_enabled = false</code>. Otherwise make the project, then run <code>meta_setup.py</code>",
     BOTH, "The probe is a decision. The database, schemas, functions and manifest load are one idempotent script.", "G4"],
    ["9", "<code>make deploy ENV=dev</code>: infrastructure, then the asset bundle",
     AUTO, "Buckets, keys, secrets, roles, workspace, catalogs, volume, jobs.", "G3"],
    ["10", "Put the secrets' values: <code>aws secretsmanager put-secret-value ...</code>. Give <code>dp-ci</code> the admin role in the new workspace",
     MANUAL, "Terraform makes the secret, never its value.", "G3"],
    ["11", "GitHub: environments <code>dev</code> and <code>prod</code> (reviewers on prod), the repository and environment variables, then <code>DEPLOY_ENABLED = true</code> last",
     MANUAL, "Settings in the GitHub web page. Not code.", "G5"],
    ["12", "Run the job once: <code>databricks bundle run engine_days</code> (3 days). Look at the operations page it leaves in the volume",
     BOTH, "A person starts it. The run, its checks and the report are automatic.", "G6"],
    ["13", "From here on: a pull request runs the checks and a real plan. A merge deploys dev and runs the integration test. Prod waits for a reviewer, then deploys the same commit",
     AUTO, "The one manual act is the approval click on prod.", "G6"],
    ["14", "A day after the first deploy: switch on <code>activate_cost_tags</code> and run <code>make account</code> again",
     BOTH, "So the cost tags appear in billing.", "G6"],
]

CODE_VS_HAND = [
    ["S3 buckets per layer, KMS key, IAM roles", "Terraform: <code>modules/storage</code>, <code>network</code>, <code>github_ci</code>", "code"],
    ["Secret containers (Secrets Manager)", "Terraform: <code>modules/secrets</code>", "code"],
    ["Secret values", "<code>aws secretsmanager put-secret-value</code>", "by hand, never in git"],
    ["Kinesis, SNS, log group, budget", "Terraform: <code>modules/stream</code>, <code>alerts</code>, <code>budget</code>", "code"],
    ["Workspace, metastore assignment, storage credential, external locations", "Terraform: <code>modules/workspace</code>, <code>unity_catalog</code>", "code"],
    ["Catalogs by layer, schemas, landing volume", "Terraform: <code>modules/unity_catalog</code>", "code"],
    ["Lakebase", "Terraform <code>modules/lakebase</code> creates an instance. Lakebase has moved from instances to projects: verify the module against your workspace", "code, to verify"],
    ["Metadata store schemas and the manifest", "<code>meta_setup.py</code>", "automated, run by a person"],
    ["Jobs and pipelines", "Asset bundle: <code>realms/databricks/engine</code>, <code>benchmarking/databricks</code>", "code"],
    ["Group, service principal <code>dp-ci</code>, federation policy", "Account console", "by hand"],
    ["Admin role for <code>dp-ci</code> in each workspace", "Workspace settings", "by hand"],
    ["GitHub environments, variables, reviewers", "Repository settings", "by hand"],
    ["Your own account values", "<code>infra/envs/NAME.local.tfvars</code>", "by hand, git-ignored"],
]

RUNBOOK = [
    ["G0 fails: a tool is old or missing", "Install or upgrade the tool named. Terraform must be 1.10 or newer; the Databricks CLI must have bundles."],
    ["G0 fails: key-like text in a tracked file", "Remove it, rotate the key, and keep the value in the secret store. The check names the file, never the value."],
    ["G1 fails: a service answers AccessDenied", "An organisation policy or a missing permission denies it. Ask the account owner. For Kinesis alone, set <code>streams = {}</code>."],
    ["G1 fails: Databricks login", "Run <code>databricks auth login</code> again. The token may have expired."],
    ["G2 fails: the state bucket is not reachable", "Run <code>make bootstrap</code>. The bucket name is in <code>account.backend.hcl</code>."],
    ["G3 fails: a catalog is missing", "<code>make deploy</code> did not finish. Read its Terraform error, fix, run it again. It is idempotent."],
    ["G3 fails: the secret does not exist", "Terraform did not create it, or the prefix in your local file differs from the one used at deploy."],
    ["G4 fails: no Lakebase", "Create the project, wake a disabled endpoint, then run <code>meta_setup.py</code>. Or set <code>lakebase_enabled = false</code> and bind plain PostgreSQL."],
    ["G5 fails: the bundle does not validate", "Read the message it prints. Usually a wrong profile or a missing variable."],
    ["G6 fails: no report in the volume", "The run failed. Open the job run, then the operations page of the last run."],
    ["The run says a table or schema already exists", "Safe: every step is idempotent. Run it again."],
]


@page("deploy", "Deployment: from a fresh checkout to a first run, with a gate at every stage", parent="index", short="Deploy",
      blurb="One page that is the guide and the runbook. Each step says if it is manual or automated, and which gate proves it.")
def deploy():
    out = ("<p class='lead'>After you check out the code, deployment is fifteen steps. Some are automated by Terraform, the asset "
           "bundle and CI. Some are manual and will stay manual: signing in, accepting an account, putting a secret's value in. "
           "Between the steps there is a <b>gate</b>: a script that checks, without changing anything, that the step really "
           "happened. You do not start a step until the gate before it passes.</p>")
    out += "<h2>The gates</h2>"
    out += table(["Gate", "What it proves", "Run it with"], GATES)
    out += (f"<p>All gates are one script, {src('P/scripts/preflight.py')}. It only lists and describes, so it is safe to run "
            "anywhere, any number of times. A failed check prints the step that fixes it. A check that depends on a failed one "
            "is skipped, so you see the first real problem and not a pile of follow-on errors. Optional things (Lakebase, a "
            "SQL warehouse) are warnings, not failures. Secrets are never printed: the account number is masked, and a key "
            "found in a file is reported by file name only.</p>")
    out += raw("python3 scripts/preflight.py --gate G1 --databricks-profile dev --aws-profile dev\n"
               "[PASS] T1  Python 3.11 or newer: 3.12.4\n"
               "[PASS] A1  signed in to AWS: arn:aws:iam::<account>:user/you\n"
               "[FAIL] A2  S3 is allowed: AccessDenied: explicit deny in a service control policy\n"
               "       fix A2: ask the account owner to lift the policy that denies S3 (step 2)\n"
               "[skip] A9  Terraform state bucket: needs A2 first", "What a run looks like (an example, not a recording)", "Example")
    out += "<h2>The steps</h2>"
    out += table(["Step", "What to do", "Who", "Note", "Gate after it"], STEPS)
    out += "<h2>What is code and what is by hand</h2>"
    out += table(["Thing", "Where it comes from", "Code or by hand"], CODE_VS_HAND)
    out += ("<h2>Keys never go in git</h2><p>Three rules, all checked by the gate G0. Real account values live in "
            "<code>NAME.local.tfvars</code>, which is git-ignored, and Make passes it after the tracked file. Secret values "
            "live only in the secret store. Tokens (a Databricks login, a Power BI token) come from the environment or a "
            "sign-in, and are held in the running process only. The gate checks that the ignore patterns are in place, "
            "scans every tracked text file for key-shaped text, and runs gitleaks over the history if it is installed.</p>")
    out += "<h2>Setting up Lakebase, the metadata database</h2>"
    out += ("<p>The metadata store is a PostgreSQL database on Lakebase, one per environment. Setting it up is four steps, the "
            "last three in one idempotent script.</p>")
    out += ladder([
        ("Check", "Lakebase must be offered in your region and tier. The gate G1 warns if it is not. If not, set <code>lakebase_enabled = false</code> and the same store binds to plain PostgreSQL."),
        ("Create", "A Lakebase project or instance named for the environment. Terraform has a module for it; verify it against your workspace, because Lakebase changed from instances to projects."),
        ("Wake and connect", "A disabled endpoint is enabled, and a short-lived token is minted from your login. It lives in the process only."),
        ("Install and load", f"{src('P/realms/aws-databricks/meta_setup.py')} creates the database, installs the schemas and functions, loads the manifest, and reads it back as a check."),
    ])
    out += raw("python3 realms/aws-databricks/meta_setup.py --instance dp-meta-dev \\\n"
               "    --manifest ~/data-platform-catalog/slices/02-tyre/manifest \\\n"
               "    --table-format delta-uc-uniform --silver-engine job",
               "Load the tyre manifest and record the table format and silver engine on every dataset", "Example")
    out += ("<p>The table format and silver engine are stored beside each dataset and are part of its fingerprint, so a run "
            "record shows which format it ran on. Be aware that today the run itself takes the table format from the job's "
            "parameter, not from the store, so the stored value documents the run and does not change it.</p>")
    out += "<h2>Fixing a failed gate</h2>"
    out += table(["What you see", "What to do"], RUNBOOK)
    out += "<h2>Day two: changing things</h2>"
    out += table(["Change", "How"], [
        ["A new dataset", "Add its manifest and contract in the catalog, load with <code>meta_setup.py</code> or the loader, deploy the bundle. No pipeline code."],
        ["A new environment", "A new <code>NAME.local.tfvars</code> and a <code>make deploy ENV=NAME</code>. The gates take <code>--env NAME</code>."],
        ["A different table format", "Change the job's <code>table_format</code> parameter and the stored setting. See " + link("map-orchestration") + "."],
        ["A rollback", "Re-run the deploy at the previous commit. Terraform and the bundle are both declarative."],
    ])
    out += not_built([
        "Nothing has been deployed on Databricks on AWS yet. The infrastructure is written and checked offline (validate and a plan on mocked providers). Only the Free Edition Databricks half was ever deployed, and its AWS half was blocked by the account's policy.",
        "The service principal, group and federation policy are made by hand. They could be Terraform at the account level once a first admin exists. That is not written.",
        "The Lakebase module creates the older instance shape. Lakebase has moved from instances to projects, so the module needs checking before the first real run.",
        "There is no gate that proves the manifest is loaded: that needs a database login, and the gate script only lists. <code>meta_setup.py</code> reads it back and says so.",
        "Gate G6 checks that a report exists, not that its numbers equal the world's truth. That judgement is in the report itself.",
        "The steps were written from the realm's README, Makefile and workflow files. They have not been run end to end on a fresh account.",
    ])
    return out
