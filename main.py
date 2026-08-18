#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Entry point for token_checker — a thin wrapper over `src/`.

Importing src.checker pulls in src.settings, so a missing or invalid environment
variable exits here with a message naming it (see src/config_errors.py) before
any work starts. The container is expected to DIE in that case: production runs
it with `restart: unless-stopped`, which is what does the restarting.

Deliberately thin, and deliberately the only thing the image's CMD runs: the old
`CMD while true; do python token_checker.py; sleep 10; done` wrapper made that
death invisible, because the shell outlived the program and kept the container
looking alive.
"""

from src.checker import run

if __name__ == "__main__":
    run()
