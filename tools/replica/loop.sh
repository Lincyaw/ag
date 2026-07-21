#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
artifacts_root=${REPLICA_ARTIFACTS:-"$repo_root/.replica-artifacts"}
worktrees_root=${REPLICA_WORKTREES:-"$repo_root/.replica-worktrees"}
scenario_source=${REPLICA_SCENARIOS_DIR:-"$repo_root/tools/replica/scenarios"}
reference_command=${REPLICA_REFERENCE_COMMAND:-claude}
agent_command=${REPLICA_AGENT_COMMAND:-}
test_command=${REPLICA_TEST_COMMAND:-"go test ./gateway ./gateway/client ./internal/bootstrap ./internal/cli"}
progress_document=${REPLICA_PROGRESS_DOCUMENT:-docs/replica-progress.md}
max_iterations=${REPLICA_MAX_ITERATIONS:-0}
width=${REPLICA_WIDTH:-120}
height=${REPLICA_HEIGHT:-40}
settle=${REPLICA_SETTLE:-2s}
agent_timeout=${REPLICA_AGENT_TIMEOUT:-45m}
agent_system=${REPLICA_AGENT_SYSTEM:-"You are an autonomous coding agent in a disposable git worktree. You may inspect and modify workspace files and run bounded shell commands. Implement one focused change from the iteration prompt, add or update tests, and maintain the required progress handoff. Do not merely report recommendations."}
compact_trigger_tokens=${REPLICA_COMPACT_TRIGGER_TOKENS:-1}
compact_target_tokens=${REPLICA_COMPACT_TARGET_TOKENS:-12000}
compact_keep_recent=${REPLICA_COMPACT_KEEP_RECENT_MESSAGES:-8}
quiesce_timeout_seconds=${REPLICA_QUIESCE_TIMEOUT_SECONDS:-30}

fail() {
  echo "replica loop: $*" >&2
  exit 2
}

for dependency in git go jq rsync tmux; do
  command -v "$dependency" >/dev/null || fail "$dependency is required"
done

case "$progress_document" in
  ""|/*|..|../*|*/../*|*/..)
    fail "REPLICA_PROGRESS_DOCUMENT must be a repository-relative path"
    ;;
esac
if [[ ! -d "$scenario_source" ]]; then
  fail "scenario directory does not exist: $scenario_source"
fi
if ! [[ "$max_iterations" =~ ^[0-9]+$ ]]; then
  fail "REPLICA_MAX_ITERATIONS must be a non-negative integer"
fi
for value in "$compact_trigger_tokens" "$compact_target_tokens" \
  "$compact_keep_recent"; do
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    fail "compact thresholds must be positive integers"
  fi
done
if ! [[ "$quiesce_timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  fail "REPLICA_QUIESCE_TIMEOUT_SECONDS must be a positive integer"
fi
run_id=${REPLICA_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}
if ! [[ "$run_id" =~ ^[A-Za-z0-9._-]+$ ]]; then
  fail "REPLICA_RUN_ID contains unsafe characters"
fi
run_root="$artifacts_root/$run_id"
worktree="$worktrees_root/$run_id"
branch="replica-lab/$run_id"
self_root="$run_root/self"
self_binary="$self_root/ag"
self_next="$self_root/ag.next"
trajectory_file="$self_root/trajectory-id"
state_file="$run_root/state.json"
results_file="$run_root/results.tsv"
scenario_root="$run_root/scenarios"
progress_path="$worktree/$progress_document"
launch_directory=$(pwd -P)
managed_gateway_directory=""
managed_gateway_ready=""

mkdir -p "$run_root" "$worktrees_root" "$self_root" "$scenario_root"
case "$worktree" in
  "$worktrees_root"/*) ;;
  *) fail "refusing unsafe worktree path: $worktree" ;;
esac

trap 'exit 130' INT
trap 'exit 143' TERM

git -C "$repo_root" worktree add -b "$branch" "$worktree" HEAD
git -C "$worktree" config user.name "ag replica loop"
git -C "$worktree" config user.email "replica-loop@localhost"

# Seed the disposable branch with the caller's complete non-ignored workspace
# without staging, committing, resetting, or otherwise mutating the caller.
if ! git -C "$repo_root" diff --quiet HEAD --; then
  git -C "$repo_root" diff --binary HEAD -- |
    git -C "$worktree" apply --whitespace=nowarn
fi
git -C "$repo_root" ls-files --others --exclude-standard -z |
  rsync -a --from0 --files-from=- "$repo_root/" "$worktree/"
git -C "$worktree" add -A
if ! git -C "$worktree" diff --cached --quiet; then
  git -C "$worktree" commit -m "replica: seed caller workspace"
fi

mkdir -p "$(dirname "$progress_path")"
if [[ ! -f "$progress_path" ]]; then
  cat >"$progress_path" <<EOF
# Claude Code Replica Progress

North star: reproduce Claude Code's terminal interaction logic and visible
behavior completely while keeping the TUI independent from the background
agent manager.

## Current run

- Run: $run_id
- Branch: $branch
- Worktree: $worktree
- Status: initializing

## Iterations

EOF
fi
git -C "$worktree" add "$progress_document"
if ! git -C "$worktree" diff --cached --quiet; then
  git -C "$worktree" commit -m "replica: initialize progress handoff"
fi

shopt -s nullglob
scenario_files=("$scenario_source"/*.json)
if [[ "${#scenario_files[@]}" -eq 0 ]]; then
  fail "no JSON scenarios found in $scenario_source"
fi
for scenario_file in "${scenario_files[@]}"; do
  jq -e '
    (type == "object") and
    ((.actions // []) | type == "array") and
    ((.reference_wait_for // "Claude") | type == "string") and
    ((.candidate_wait_for // "ag") | type == "string")
  ' "$scenario_file" >/dev/null || fail "invalid scenario: $scenario_file"
  cp "$scenario_file" "$scenario_root/"
done
shopt -u nullglob

scenarios=()
shopt -s nullglob
for scenario_file in "$scenario_root"/*.json; do
  scenario=$(basename "$scenario_file" .json)
  if ! [[ "$scenario" =~ ^[A-Za-z0-9._-]+$ ]]; then
    fail "invalid scenario name: $scenario"
  fi
  scenarios+=("$scenario")
done
shopt -u nullglob

echo "Replica branch: $branch"
echo "Artifacts: $run_root"
echo "Progress: $progress_document"
if [[ -n "$agent_command" ]]; then
  echo "Agent: external REPLICA_AGENT_COMMAND override"
else
  echo "Agent: self-hosted ag trajectory"
fi

lab_binary="$run_root/replica-lab"
candidate_binary="$run_root/ag-candidate"
(cd "$worktree" && go build -o "$lab_binary" ./cmd/replica-lab)

set_scenario_arguments() {
  local scenario_name=$1 scenario_file settle_ms
  scenario_file="$scenario_root/$scenario_name.json"
  extra=()
  scenario_reference_wait=$(jq -r '.reference_wait_for // "Claude"' "$scenario_file")
  scenario_candidate_wait=$(jq -r '.candidate_wait_for // "ag"' "$scenario_file")
  settle_ms=$(jq -r '.settle_ms // 0' "$scenario_file")
  scenario_settle="$settle"
  if [[ "$settle_ms" != "0" ]]; then
    scenario_settle="${settle_ms}ms"
  fi
  while IFS= read -r action; do
    extra+=(--action "$action")
  done < <(jq -c '.actions[]?' "$scenario_file")
}

capture_reference() {
  local scenario
  local -a extra capture_arguments
  for scenario in "${scenarios[@]}"; do
    set_scenario_arguments "$scenario"
    capture_arguments=(capture \
      --name "claude-$scenario" \
      --command "$reference_command" \
      --cwd "$worktree" \
      --out "$run_root/reference/$scenario" \
      --width "$width" --height "$height" \
      --wait-for "$scenario_reference_wait" --settle "$scenario_settle")
    if [[ "${#extra[@]}" -gt 0 ]]; then
      capture_arguments+=("${extra[@]}")
    fi
    "$lab_binary" "${capture_arguments[@]}"
  done
}

capture_candidate() {
  local destination=$1 scenario
  local -a extra capture_arguments
  git -C "$worktree" diff --check || return
  (cd "$worktree" && go build -o "$candidate_binary" ./cmd/ag) || return
  for scenario in "${scenarios[@]}"; do
    set_scenario_arguments "$scenario"
    capture_arguments=(capture \
      --name "ag-$scenario" \
      --command "'$worktree/tools/replica/run-ag.sh' '$candidate_binary'" \
      --cwd "$worktree" \
      --out "$destination/$scenario" \
      --width "$width" --height "$height" \
      --wait-for "$scenario_candidate_wait" --settle "$scenario_settle")
    if [[ "${#extra[@]}" -gt 0 ]]; then
      capture_arguments+=("${extra[@]}")
    fi
    "$lab_binary" "${capture_arguments[@]}" || return
  done
}

score_suite() {
  local candidate=$1 report=$2 scenario score
  local total=0
  mkdir -p "$report"
  : >"$report/suite.md"
  printf '# Replica suite\n\n' >>"$report/suite.md"
  for scenario in "${scenarios[@]}"; do
    score=$("$lab_binary" compare \
      --reference "$run_root/reference/$scenario" \
      --candidate "$candidate/$scenario" \
      --out "$report/$scenario" |
      sed -n 's/.*"score":\([0-9.eE+-]*\).*/\1/p') || return
    if [[ -z "$score" ]]; then
      echo "comparison did not return a score for $scenario" >&2
      return 1
    fi
    total=$(awk -v total="$total" -v score="$score" \
      'BEGIN { printf "%.12f", total + score }')
    printf -- '- %s: %s\n' "$scenario" "$score" >>"$report/suite.md"
  done
  awk -v total="$total" -v count="${#scenarios[@]}" \
    'BEGIN { printf "%.12f\n", total / count }'
}

suite_has_no_regressions() {
  local next_report=$1 previous_report=$2 scenario next_score previous_score
  for scenario in "${scenarios[@]}"; do
    next_score=$(sed -n \
      's/^[[:space:]]*"score":[[:space:]]*\([0-9.eE+-]*\).*/\1/p' \
      "$next_report/$scenario/report.json")
    previous_score=$(sed -n \
      's/^[[:space:]]*"score":[[:space:]]*\([0-9.eE+-]*\).*/\1/p' \
      "$previous_report/$scenario/report.json")
    if [[ -z "$next_score" || -z "$previous_score" ]] ||
      ! awk -v candidate_score="$next_score" \
        -v previous_score="$previous_score" \
        'BEGIN { exit !(candidate_score >= previous_score) }'; then
      return 1
    fi
  done
}

resolve_managed_gateway() {
  local raw target configured
  [[ -n "$managed_gateway_directory" ]] && return 0
  if ! raw=$("$self_binary" --otel=false -o json config show); then
    fail "cannot resolve the self-hosted ag gateway configuration"
  fi
  target=$(jq -r '.config.gateway.target // empty' <<<"$raw")
  if [[ -n "$target" ]]; then
    fail "the self-hosted loop cannot restart remote gateway target: $target"
  fi
  configured=$(jq -r '.config.gateway.directory // empty' <<<"$raw")
  [[ -n "$configured" ]] || fail "effective gateway directory is empty"
  if [[ "$configured" != /* ]]; then
    configured="$launch_directory/$configured"
  fi
  mkdir -p "$configured"
  managed_gateway_directory=$(cd "$configured" && pwd -P)
  managed_gateway_ready="$managed_gateway_directory/managed/ready.json"
}

restart_managed_gateway() {
  local manager_pid ready_directory attempt
  resolve_managed_gateway
  [[ -f "$managed_gateway_ready" ]] || return 0
  manager_pid=$(jq -r '.pid // empty' "$managed_gateway_ready" 2>/dev/null || true)
  ready_directory=$(jq -r '.directory // empty' "$managed_gateway_ready" 2>/dev/null || true)
  if [[ "$ready_directory" != "$managed_gateway_directory" ]]; then
    fail "refusing to restart gateway with unexpected directory: $ready_directory"
  fi
  case "$manager_pid" in
    ""|*[!0-9]*) return 0 ;;
  esac
  if ! kill -0 "$manager_pid" 2>/dev/null; then
    return 0
  fi
  kill -TERM "$manager_pid"
  attempt=0
  while kill -0 "$manager_pid" 2>/dev/null && [[ "$attempt" -lt 300 ]]; do
    sleep 0.1
    attempt=$((attempt + 1))
  done
  if kill -0 "$manager_pid" 2>/dev/null; then
    fail "gateway $manager_pid did not stop within 30 seconds"
  fi
}

install_self_binary() {
  local source_binary=$1
  cp "$source_binary" "$self_next"
  chmod 0700 "$self_next"
  mv -f "$self_next" "$self_binary"
  if [[ -z "$agent_command" ]]; then
    restart_managed_gateway
  fi
}

sync_progress() {
  cp "$progress_path" "$run_root/progress.md"
}

trajectory_id() {
  if [[ -f "$trajectory_file" ]]; then
    tr -d '\r\n' <"$trajectory_file"
  fi
}

write_state() {
  local iteration=$1 trajectory temporary
  trajectory=$(trajectory_id)
  temporary="$state_file.tmp"
  jq -n \
    --arg run_id "$run_id" \
    --arg branch "$branch" \
    --arg worktree "$worktree" \
    --arg trajectory_id "$trajectory" \
    --arg best_score "$best_score" \
    --arg best_report "$best_report" \
    --arg self_binary "$self_binary" \
    --arg progress "$progress_path" \
    --argjson next_iteration "$iteration" \
    '{
      run_id: $run_id,
      branch: $branch,
      worktree: $worktree,
      trajectory_id: $trajectory_id,
      best_score: $best_score,
      best_report: $best_report,
      self_binary: $self_binary,
      progress: $progress,
      next_iteration: $next_iteration
    }' >"$temporary"
  mv -f "$temporary" "$state_file"
}

append_progress_result() {
  local serial=$1 status=$2 score=$3 detail=$4 iteration_root=$5 trajectory
  trajectory=$(trajectory_id)
  cat >>"$progress_path" <<EOF

### Loop result $serial — $status

- Time: $(date -u +%Y-%m-%dT%H:%M:%SZ)
- Score: $score
- Best score: $best_score
- Trajectory: ${trajectory:-not-created}
- Detail: $detail
- Evidence: $iteration_root
EOF
}

run_quiesce_command() {
  local log_path=$1
  shift
  local command_pid attempt limit status
  limit=$((quiesce_timeout_seconds * 10))
  "$@" >>"$log_path" 2>&1 &
  command_pid=$!
  attempt=0
  while kill -0 "$command_pid" 2>/dev/null && [[ "$attempt" -lt "$limit" ]]; do
    sleep 0.1
    attempt=$((attempt + 1))
  done
  if kill -0 "$command_pid" 2>/dev/null; then
    kill -TERM "$command_pid" 2>/dev/null || true
    wait "$command_pid" 2>/dev/null || true
    echo "cleanup command exceeded ${quiesce_timeout_seconds}s: $*" \
      >>"$log_path"
    return 124
  fi
  if wait "$command_pid"; then
    status=0
  else
    status=$?
  fi
  return "$status"
}

quiesce_self_trajectory() {
  local iteration_root=$1 existing_id raw pending cleanup_log
  existing_id=$(trajectory_id)
  [[ -n "$existing_id" ]] || return 0
  cleanup_log="$iteration_root/cleanup.log"
  : >"$cleanup_log"

  # A failed foreground invocation may leave its durable input running in the
  # gateway. Never reset the worktree while that background writer is alive.
  run_quiesce_command "$cleanup_log" \
    "$self_binary" --otel=false -o json trajectory cancel "$existing_id" || true
  if ! run_quiesce_command "$cleanup_log" \
    "$self_binary" --otel=false -o json trajectory wait "$existing_id"; then
    return 1
  fi
  if ! raw=$("$self_binary" --otel=false -o json trajectory list \
    2>>"$cleanup_log"); then
    return 1
  fi
  pending=$(jq -r --arg id "$existing_id" \
    '.[] | select(.id == $id) | .pending_inputs' <<<"$raw")
  if [[ "$pending" != "0" ]]; then
    echo "trajectory $existing_id still has ${pending:-unknown} pending input(s)" \
      >>"$cleanup_log"
    return 1
  fi
}

run_self_agent() {
	local prompt_path=$1 iteration_root=$2 prompt output_id existing_id temporary
	local cause_code cause_detail
  local -a arguments
  prompt=$(<"$prompt_path")
  existing_id=$(trajectory_id)
  arguments=(--otel=false --progress never -o json run)
  if [[ -n "$existing_id" ]]; then
    arguments+=("$existing_id")
  fi
	arguments+=(
    --interactive=false
    --cwd "$worktree"
    --write
    --bash
		--compact
		--timeout "$agent_timeout"
    --prompt "$prompt"
  )
  if ! AGENTM_AGENT_SYSTEM="$agent_system" \
    AGENTM_COMPACT_TRIGGER_TOKENS="$compact_trigger_tokens" \
    AGENTM_COMPACT_TARGET_TOKENS="$compact_target_tokens" \
    AGENTM_COMPACT_KEEP_RECENT_MESSAGES="$compact_keep_recent" \
    "$self_binary" "${arguments[@]}" \
      >"$iteration_root/agent.json" 2>"$iteration_root/agent.log"; then
    return 1
  fi
  output_id=$(jq -r '.trajectory_id // empty' "$iteration_root/agent.json")
  if [[ -z "$output_id" ]]; then
    echo "self-hosted ag did not return a trajectory_id" \
      >>"$iteration_root/agent.log"
    return 1
  fi
  if [[ -n "$existing_id" && "$output_id" != "$existing_id" ]]; then
    echo "trajectory changed from $existing_id to $output_id" \
      >>"$iteration_root/agent.log"
    return 1
  fi
	if [[ -z "$existing_id" ]]; then
    temporary="$trajectory_file.tmp"
    printf '%s\n' "$output_id" >"$temporary"
		mv -f "$temporary" "$trajectory_file"
	fi
	cause_code=$(jq -r '.result.cause.code // empty' "$iteration_root/agent.json")
	if [[ "$cause_code" != "model_end" ]]; then
		cause_detail=$(jq -r '.result.cause.detail // empty' \
			"$iteration_root/agent.json")
		agent_failure_detail="agent ended with ${cause_code:-unknown cause}"
		if [[ -n "$cause_detail" ]]; then
			agent_failure_detail="$agent_failure_detail: $cause_detail"
		fi
		printf '%s\n' "$agent_failure_detail" >>"$iteration_root/agent.log"
		return 1
	fi
}

run_external_agent() {
  local prompt_path=$1 iteration_root=$2
  REPLICA_PROMPT="$prompt_path" \
  REPLICA_REPORT="$best_report" \
  REPLICA_WORKTREE="$worktree" \
  REPLICA_PROGRESS="$progress_path" \
    bash -lc "cd '$worktree' && $agent_command" \
      >"$iteration_root/agent.log" 2>&1
}

invoke_agent() {
  local prompt_path=$1 iteration_root=$2
  if [[ -n "$agent_command" ]]; then
    run_external_agent "$prompt_path" "$iteration_root"
  else
    run_self_agent "$prompt_path" "$iteration_root"
  fi
}

capture_reference
capture_candidate "$run_root/baseline"
best_score=$(score_suite "$run_root/baseline" "$run_root/report-0000")
best_report="$run_root/report-0000"
install_self_binary "$candidate_binary"
printf 'timestamp\titeration\tcommit\tstatus\tscore\tbest_score\tdescription\tartifacts\n' \
  >"$results_file"
printf '%s\t0\t%s\tbaseline\t%s\t%s\tinitial fixed scenario suite\t%s\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "$(git -C "$worktree" rev-parse --short HEAD)" \
  "$best_score" "$best_score" "$run_root/report-0000" >>"$results_file"
append_progress_result "0000" "baseline" "$best_score" \
  "Fixed Claude reference captured for: ${scenarios[*]}" "$run_root/report-0000"
git -C "$worktree" add "$progress_document"
git -C "$worktree" commit -m "replica: record baseline"
sync_progress
write_state 1
echo "Baseline score: $best_score"

iteration=1
while [[ "$max_iterations" -eq 0 || "$iteration" -le "$max_iterations" ]]; do
  printf -v serial '%04d' "$iteration"
  iteration_root="$run_root/iteration-$serial"
  mkdir -p "$iteration_root"
  prompt_path="$iteration_root/prompt.md"
  iteration_base=$(git -C "$worktree" rev-parse HEAD)
  progress_before=$(git -C "$worktree" hash-object "$progress_document")

  cat >"$prompt_path" <<EOF
You are iteration $serial of ag's self-hosted Claude Code replica loop.

North star
Reproduce Claude Code's TUI interaction logic and visible behavior completely:
editing, completion, commands, submission and streaming, tools, cancellation,
queued input, background/detach/reattach, interactions, permissions and diffs,
scrolling, resizing, session lifecycle, errors, and terminal rendering.

Boundaries
- Work only in this disposable worktree: $worktree
- Preserve the split: gateway owns background agent/trajectory behavior; TUI
  owns terminal interaction and presentation.
- The fixed Claude captures are in: $run_root/reference
- The current accepted comparison is: $best_report/suite.md
- Current accepted score: $best_score
- Durable handoff document: $progress_path
- The loop resumes the trajectory recorded at: $trajectory_file

Iteration contract
1. Read the durable handoff. Then, before broad exploration, append a
   "Candidate iteration $serial — in progress" section to
   $progress_document. This early checkpoint is mandatory.
2. Inspect the current report and only the files needed for one highest-value
   mismatch. Spend at most 10 tool calls on reconnaissance before editing.
3. Make one focused change and add or update its state-transition tests.
4. Run focused checks. The loop owns the authoritative visual suite.
5. Replace or complete your candidate section with: observed gap, files
   changed, tests run, evidence, risks, and exactly one next target.
6. Return a concise final message. Do not commit and do not reset; the loop
   owns keep/discard and commits.

The next invocation uses the rebuilt ag binary, restarts the auto-managed
gateway, resumes this same durable trajectory, and runs auto-compact before the
provider call. The trajectory must remain visible through the normal
ag trajectory list and attach flow. Treat the progress document as authoritative
memory across compaction.
EOF

  agent_ok=false
  detail="agent invocation failed"
  agent_failure_detail=""
  if invoke_agent "$prompt_path" "$iteration_root"; then
    if [[ "$(git -C "$worktree" rev-parse HEAD)" != "$iteration_base" ]]; then
      detail="agent committed even though the loop owns commits"
    elif [[ ! -f "$progress_path" ]]; then
      detail="agent removed the progress handoff document"
    elif [[ "$(git -C "$worktree" hash-object "$progress_document")" == "$progress_before" ]]; then
      detail="agent did not update the progress handoff document"
    else
      agent_ok=true
      detail="candidate did not improve the fixed suite"
    fi
  elif [[ -z "$agent_command" ]] &&
    ! quiesce_self_trajectory "$iteration_root"; then
    fail "self-hosted trajectory did not quiesce after iteration $serial; refusing to reset $worktree"
  elif [[ -n "$agent_failure_detail" ]]; then
    detail="$agent_failure_detail"
  fi

  accepted=false
  score="$best_score"
  next_report="$run_root/report-$serial"
  if [[ "$agent_ok" == true ]]; then
    if bash -lc "cd '$worktree' && $test_command" \
      >"$iteration_root/tests.log" 2>&1; then
      if capture_candidate "$iteration_root/candidate"; then
        if next_score=$(score_suite "$iteration_root/candidate" "$next_report"); then
          score="$next_score"
          if suite_has_no_regressions "$next_report" "$best_report" &&
            awk -v candidate_score="$next_score" \
              -v accepted_score="$best_score" \
              'BEGIN { exit !(candidate_score > accepted_score) }'; then
            accepted=true
            best_score="$next_score"
            best_report="$next_report"
            detail="all tests passed; every scenario held or improved"
          fi
        else
          detail="comparison suite failed"
        fi
      else
        detail="candidate build or capture failed"
      fi
    else
      detail="test command failed; see tests.log"
    fi
  fi

  if [[ "$accepted" == true ]]; then
    append_progress_result "$serial" "accepted" "$score" "$detail" "$iteration_root"
    git -C "$worktree" add -A
    git -C "$worktree" commit -m "replica: improve TUI fidelity ($serial)"
    install_self_binary "$candidate_binary"
    status="keep"
    echo "Accepted iteration $serial: $best_score"
  else
    git -C "$worktree" add -N -- . >/dev/null 2>&1 || true
    git -C "$worktree" diff --binary "$iteration_base" \
      >"$iteration_root/rejected.patch"
    if [[ -f "$progress_path" ]]; then
      cp "$progress_path" "$iteration_root/candidate-progress.md"
    fi
    # Destructive cleanup is deliberately fenced to the validated disposable
    # worktree created by this run.
    git -C "$worktree" reset --hard "$iteration_base"
    git -C "$worktree" clean -fd
    append_progress_result "$serial" "rejected" "$score" "$detail" "$iteration_root"
    git -C "$worktree" add "$progress_document"
    git -C "$worktree" commit -m "replica: log rejected iteration ($serial)"
    status="discard"
    echo "Rejected iteration $serial: $detail"
  fi

  commit=$(git -C "$worktree" rev-parse --short HEAD)
  clean_detail=$(printf '%s' "$detail" | tr '\t\r\n' '   ')
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$iteration" "$commit" "$status" \
    "$score" "$best_score" "$clean_detail" "$iteration_root" >>"$results_file"
  sync_progress
  iteration=$((iteration + 1))
  write_state "$iteration"
done
