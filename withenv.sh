#!/bin/zsh
# Load only AMPERSAND_* exports from ~/.zshrc (never prints values)
eval "$(grep -E "^export AMPERSAND_" ~/.zshrc)"
exec "$@"
