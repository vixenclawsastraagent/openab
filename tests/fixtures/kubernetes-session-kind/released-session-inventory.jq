def release_error($message):
  error($message);

if (
  ($session_id | type) != "string"
  or ($session_id | length) == 0
  or (type != "object")
  or (.items | type) != "array"
) then
  release_error("released-session inventory is malformed")
else
  .
end
| . as $inventory
| (if (
    ($expected | length) != 1
    or ($expected[0] | type) != "array"
    or ($expected[0] | length) != 5
    or any($expected[0][];
      type != "object"
      or keys != ["kind", "name", "uid"]
      or (.kind | type) != "string"
      or (.kind | length) == 0
      or (.name | type) != "string"
      or (.name | length) == 0
      or (.uid | type) != "string"
      or (.uid | length) == 0
    )
    or ([$expected[0][].kind] | sort) != [
      "ConfigMap",
      "NetworkPolicy",
      "PersistentVolumeClaim",
      "Pod",
      "ServiceAccount"
    ]
  ) then
    release_error("released-session expected resources are malformed")
  else
    $expected[0]
  end) as $targets
| (if any($inventory.items[];
    type != "object"
    or (.kind | type) != "string"
    or (.kind | length) == 0
    or (.metadata | type) != "object"
    or (.metadata.name | type) != "string"
    or (.metadata.name | length) == 0
    or (.metadata.uid | type) != "string"
    or (.metadata.uid | length) == 0
    or (
      .metadata.annotations != null
      and (.metadata.annotations | type) != "object"
    )
    or (
      (.metadata.annotations | type) == "object"
      and (.metadata.annotations | has("openab.dev/session-id"))
      and (
        .metadata.annotations["openab.dev/session-id"] | type
      ) != "string"
    )
  ) then
    release_error("released-session inventory item is malformed")
  else
    [
      $inventory.items[]
      | {
          kind,
          name: .metadata.name,
          uid: .metadata.uid,
          sessionId: (
            .metadata.annotations["openab.dev/session-id"] // ""
          )
        }
    ]
  end) as $resources
| ($targets | map(select(.kind == "ConfigMap")) | .[0]) as $anchor
| {
    exactMatches: ([
      $targets[] as $target
      | $resources[]
      | select(
          .kind == $target.kind
          and (
            .name == $target.name
            or .uid == $target.uid
          )
        )
      | {kind, name, uid}
    ] | sort_by([.kind, .name, .uid])),
    annotatedChildren: ([
      $resources[]
      | select(.sessionId == $session_id)
      | select((
          .kind == $anchor.kind
          and (
            .name == $anchor.name
            or .uid == $anchor.uid
          )
        ) | not)
      | {kind, name, uid}
    ] | sort_by([.kind, .name, .uid]))
  }
