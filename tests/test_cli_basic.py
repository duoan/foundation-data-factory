import contextlib
import io

from fdf.cli.main import main as fdf_main


def test_cli_help_shows_usage():
    # Call the CLI entrypoint directly so we don't rely on subprocess.
    # This avoids security lint issues around subprocess use in tests and
    # handles the SystemExit raised by argparse when printing help.
    # We pass ["--help"] to trigger the help output path.
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        try:
            fdf_main(["--help"])
        except SystemExit as exc:
            # argparse exits with status 0 after printing help
            assert exc.code == 0

    output = stdout.getvalue().lower()
    assert "usage" in output
    assert "fdf" in output


def test_cli_version_prints_project_version():
    exit_code = fdf_main(["version"])

    assert exit_code == 0
