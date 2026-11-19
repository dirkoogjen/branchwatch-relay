# Claude Runner 2 and Monitor 2 — user guide

This guide accompanies the local release candidate. See [Installation](INSTALLATION.md) and [Verification](VERIFICATION.md) for the tested profile.

## Purpose and components

Runner 2 manages software tasks performed by Claude Code. A task receives a separate Git worktree for its changes. A controller manages tasks, a watch process monitors progress and recovery, and a central queue can distribute jobs among machines. Monitor 2 provides a browser interface for available status and controls. The monitor depends on the runner and additional modules; copying the monitor alone is not a complete installation.

## Before use

Once explicitly enabled, the runner starts Claude Code with `bypassPermissions` and no interactive permission prompts. A task can therefore act with the privileges of its operating-system account. A Git worktree is **not a security sandbox**: other files, network access and available credentials may be accessible. Use trusted instructions and repositories, a separate account or suitable isolation, and minimum privileges. Review permissions, external services, costs and security settings before starting a task. Do not expose the monitor or controller to a network or the internet without appropriate access protection.

## Jobs and tasks

A job selects a machine, repository and instruction and may specify a model and dependencies. Depending on configuration, the queue can wait for preceding jobs or human approval. The watch can start the next job when that runner is free. Available controls depend on configuration and version.

A task may be running, waiting for an answer, paused, completed, failed or stopped. Available controls include status and result, answering, pausing, resuming and stopping. Review the final report and worktree changes: a completed status alone does not establish correctness or integration.

An optional chain can execute sequential tasks. An optional supervisor can review chains and plan follow-up work within configured limits. External supervision is available for explicitly registered projects. Time, count and progress limits constrain execution but do not guarantee freedom from errors or costs.

## Monitor and operations

The inspected monitor provides machine, job and attention views, reports and controls, subject to configuration and permissions. Its inspected server defaults to local binding and includes checks for browser actions; separate routes exist for specific network access. The running release and all routes still require review. Treat local access as access to sensitive data and potentially powerful actions.

Keep configuration, task state and keys separate from public source. Logs and transcripts may contain confidential instructions and results. Back up state and test restoration separately. Do not move or delete active worktrees or change services during a task without checking the consequences for that particular installation.

## First use

Follow [Installation](INSTALLATION.md) for your platform. Open Monitor 2 and check the local runner. Remote runner cards only become active after configuration. The monitor defaults to read-only. Use `python -m runner2 status` to list tasks and `status TASK-ID` for one task. Use `pause TASK-ID` to pause, `send TASK-ID` to resume a paused task, or append a reply if a task is waiting for your answer. Use `stop TASK-ID` to end it. Check `--help` for options. Substitute `python3` if that is your interpreter command.

If startup fails, check Python, Git, the state directory, the watch and the chosen port. If a task cannot start, check the pinned Claude version and the dedicated account's repository access. Share only redacted diagnostics.
