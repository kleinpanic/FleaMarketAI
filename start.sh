#!/usr/bin/env bash
# Activate the virtual environment if present
VENV_DIR="$(dirname "${BASH_SOURCE[0]}")/.venv"
if [ -d "$VENV_DIR" ]; then
  source "$VENV_DIR/bin/activate"
fi
# Host‑mode runner – loops forever with 4‑hour sleep
cd "$(dirname "${BASH_SOURCE[0]}")"
python3 -m src.main