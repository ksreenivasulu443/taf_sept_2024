import pytest
import argparse
from datetime import datetime
import os

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run Pytest with custom options.")
    parser.add_argument(
        "--env",
        default="dev",
        help="Specify the environment to run tests (default: dev)."
    )
    parser.add_argument(
        "--marker",
        default='retest',
        help="Run tests with a specific marker (e.g., 'count_check')."
    )
    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Specify the directory to save the test report (default: reports)."
    )
    parser.add_argument(
        "--html-report",
        action="store_true",
        help="Generate an HTML report."
    )
    parser.add_argument(
        "--test-dir",
        default="tests",
        help="Specify the directory containing test files (default: tests)."
    )
    parser.add_argument(
        "--maxfail",
        type=int,
        default=None,
        help="Stop after the specified number of failures."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output."
    )
    args = parser.parse_args()

    # Prepare Pytest arguments
    pytest_args = [args.test_dir, "--env", args.env]

    # Add marker if specified
    if args.marker:
        pytest_args.extend(["-m", args.marker])

    # Add maxfail if specified
    if args.maxfail:
        pytest_args.extend(["--maxfail", str(args.maxfail)])

    # Enable verbose output if specified
    if args.verbose:
        pytest_args.append("-v")

    # Generate a dynamic report name if HTML report is requested
    if args.html_report:
        report_dir = args.output_dir
        os.makedirs(report_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        report_path = os.path.join(report_dir, f"report_{timestamp}.html")
        pytest_args.extend(["--html", report_path, "--self-contained-html"])

    # Run Pytest
    print(f"Running Pytest with arguments: {pytest_args}")
    exit_code = pytest.main(pytest_args)

    # Exit with the appropriate status code
    exit(exit_code)


if __name__ == "__main__":
    main()
