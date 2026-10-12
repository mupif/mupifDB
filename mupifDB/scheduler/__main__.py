"""
Command line of the scheduler package: ``python -m mupifDB.scheduler``.

Without options the scheduler is started (same as running ``mupifDB/workflowscheduler.py``).
``--print-config`` prints all settings with their current values, defaults and environment
variables, and exits.
"""

import argparse
import sys

from .config import SchedulerConfig


def printConfig(config: SchedulerConfig) -> None:
    """
    Print the settings as a table.

    :param config: configuration to print
    """
    for name, env, value, default, description in config.describe():
        print(f'{name:<16} = {value!s:<44} {env}\n{"":<19}default: {default}; {description}')


def main(argv: list[str] | None = None) -> int:
    """
    Run the command line.

    :param argv: command-line arguments (default: :data:`sys.argv`)
    :return: process exit status
    """
    ap = argparse.ArgumentParser(prog='python -m mupifDB.scheduler', description='MupifDB workflow scheduler')
    ap.add_argument('--print-config', action='store_true', help='print the settings and exit')
    args = ap.parse_args(argv)
    if args.print_config:
        printConfig(SchedulerConfig.fromEnv())
        return 0
    from mupifDB import workflowscheduler
    workflowscheduler.main()
    return 0


if __name__ == '__main__':
    sys.exit(main())
