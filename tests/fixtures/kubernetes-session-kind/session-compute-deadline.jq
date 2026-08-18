def deadline_error($message):
  error($message);

def add_seconds_preserving_fraction($seconds):
  . as $timestamp
  | if ($timestamp | type) != "string" then
      deadline_error("deadline timestamp must be canonical UTC RFC3339")
    else
      .
    end
  | (
      try capture(
        "^(?<whole>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?<fraction>[.][0-9]{1,9})?Z$"
      )
      catch deadline_error("deadline timestamp must be canonical UTC RFC3339")
    ) as $parts
  | ($parts.whole + "Z") as $whole
  | (
      try ($whole | fromdateiso8601 | todateiso8601)
      catch deadline_error("deadline timestamp must be canonical UTC RFC3339")
    ) as $roundtrip
  | if $roundtrip != $whole then
      deadline_error("deadline timestamp must be canonical UTC RFC3339")
    else
      (
        ($whole | fromdateiso8601) + $seconds
        | todateiso8601
      ) as $next
      | $next[0:-1] + ($parts.fraction // "") + "Z"
    end;

if (
  ($compute_idle_seconds | type) != "number"
  or ($compute_idle_seconds | floor) != $compute_idle_seconds
  or $compute_idle_seconds <= 1
  or ($storage_retention_seconds | type) != "number"
  or ($storage_retention_seconds | floor) != $storage_retention_seconds
  or $storage_retention_seconds < $compute_idle_seconds
) then
  deadline_error("deadline planner policy seconds are invalid")
else
  .
end
| . as $snapshot
| if (
    ($snapshot | type) != "object"
    or ($snapshot.name | type) != "string"
    or ($snapshot.name | length) == 0
    or ($snapshot.uid | type) != "string"
    or ($snapshot.uid | length) == 0
    or ($snapshot.resourceVersion | type) != "string"
    or ($snapshot.resourceVersion | length) == 0
    or ($snapshot.anchorRaw | type) != "string"
    or ($snapshot.anchorRaw | length) == 0
    or ($snapshot.state | type) != "object"
  ) then
    deadline_error("deadline snapshot is malformed")
  else
    .
  end
| (
    try ($snapshot.anchorRaw | fromjson)
    catch deadline_error("deadline snapshot raw anchor disagrees with parsed state")
  ) as $before
| if $before != $snapshot.state then
    deadline_error("deadline snapshot raw anchor disagrees with parsed state")
  else
    .
  end
| if (
    $before.phase != "ready"
    or ($before.podUid | type) != "string"
    or ($before.podUid | length) == 0
  ) then
    deadline_error("deadline planner requires a ready anchor with a live Pod")
  else
    .
  end
| ($before.lastActivityAt | add_seconds_preserving_fraction($compute_idle_seconds))
    as $configured_compute_deadline
| ($before.lastActivityAt | add_seconds_preserving_fraction($storage_retention_seconds))
    as $configured_storage_deadline
| if $before.computeDeadlineAt != $configured_compute_deadline then
    deadline_error("live anchor compute deadline does not match configured TTL")
  elif $before.storageDeadlineAt != $configured_storage_deadline then
    deadline_error("live anchor storage deadline does not match configured retention")
  else
    .
  end
| ($before.lastActivityAt | add_seconds_preserving_fraction(1))
    as $expected_deadline
| ($before | .computeDeadlineAt = $expected_deadline) as $expected_state
| {
    expectedDeadline: $expected_deadline,
    expectedState: $expected_state,
    patch: [
      {
        op: "test",
        path: "/metadata/uid",
        value: $snapshot.uid
      },
      {
        op: "test",
        path: "/metadata/resourceVersion",
        value: $snapshot.resourceVersion
      },
      {
        op: "test",
        path: "/data/anchor.json",
        value: $snapshot.anchorRaw
      },
      {
        op: "replace",
        path: "/data/anchor.json",
        value: ($expected_state | tojson)
      }
    ]
  }
