#!/usr/bin/env python3
"""PreToolUse guardrail: deny Write/Edit calls that add comments to Python files.

Scope is the Python code of this repository only. The Kubernetes manifests,
compose files, Dockerfiles, shell scripts and SQL migrations document
themselves with comments and are left alone. Docstrings are always allowed,
and so are tool directives such as ``# noqa`` or ``# type: ignore``.
Comments that already exist in the file may be kept or moved.
"""
import io
import json
import os
import sys
import textwrap
import tokenize

GUARDED_EXTENSIONS = (".py",)

ALLOWED_PREFIXES = (
    "#!", "# -*-", "# type:", "# noqa", "# pylint", "# pyright", "# mypy",
    "# fmt:", "# isort", "# ruff:", "# pragma:", "# shellcheck",
)


def is_allowed(comment):
    return comment.startswith(ALLOWED_PREFIXES)


def tokenized_comments(text):
    comments = []
    reader = io.StringIO(textwrap.dedent(text)).readline
    for token in tokenize.generate_tokens(reader):
        if token.type == tokenize.COMMENT:
            comments.append(token.string.strip())
    return comments


def line_comments(text):
    return [
        line.strip() for line in text.splitlines() if line.strip().startswith("#")
    ]


def comments_in(text):
    try:
        found = tokenized_comments(text)
    except (tokenize.TokenError, SyntaxError):
        found = line_comments(text)
    return [comment for comment in found if not is_allowed(comment)]


def inside_project(path):
    root = os.environ.get("CLAUDE_PROJECT_DIR")
    if not root:
        return True
    root = os.path.realpath(root)
    target = os.path.realpath(path)
    return target == root or target.startswith(root + os.sep)


def read_text(path):
    try:
        with open(path, encoding="utf-8", errors="ignore") as handle:
            return handle.read()
    except OSError:
        return ""


def main():
    payload = json.load(sys.stdin)
    tool = payload.get("tool_name", "")
    tool_input = payload.get("tool_input") or {}
    path = tool_input.get("file_path", "")

    if os.path.splitext(path)[1].lower() not in GUARDED_EXTENSIONS:
        return
    if not inside_project(path):
        return

    if tool == "Write":
        new_text = tool_input.get("content", "")
        old_text = read_text(path)
    elif tool == "Edit":
        new_text = tool_input.get("new_string", "")
        old_text = tool_input.get("old_string", "")
    else:
        return

    existing = set(comments_in(old_text))
    added = [comment for comment in comments_in(new_text) if comment not in existing]
    if not added:
        return

    sample = "\n".join(f"  {comment}" for comment in added[:5])
    reason = (
        "Project policy: no new comments in Python code. "
        f"Found {len(added)} new comment(s) in {os.path.basename(path)}:\n"
        f"{sample}\n"
        "Redo the change without them. If the code needs explaining, use "
        "clearer names or a docstring. Comments that were already in the file "
        "can stay, and tool directives such as '# noqa' or '# type: ignore' "
        "are allowed."
    )
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }))


if __name__ == "__main__":
    main()
