def nonempty_string:
    type == "string" and length > 0;

def resource_identity:
    type == "object"
    and keys == ["podUid", "pvUid", "pvcUid", "serviceAccountUid"]
    and all(.[]; nonempty_string);

. as $root
| type == "object"
and keys == [
    "claimBoundary",
    "isolation",
    "lifecycle",
    "mode",
    "network",
    "result",
    "schemaVersion",
    "source"
]
and .schemaVersion == 1
and .result == "passed"
and .mode == "isolation"
and (
    .source
    | type == "object"
    and keys == ["testedSha", "treeState"]
    and (.testedSha | test("^([0-9a-f]{40}|[0-9a-f]{64})$"))
    and (.treeState == "clean" or .treeState == "dirty")
)
and (
    .isolation
    | type == "object"
    and keys == [
        "peerPvcMountAbsent",
        "sessionA",
        "sessionB",
        "sharedSkillsReadOnly",
        "uidsDistinct",
        "workspaceProbe"
    ]
    and (.sessionA | resource_identity)
    and (.sessionB | resource_identity)
    and .sessionA.podUid != .sessionB.podUid
    and .sessionA.pvcUid != .sessionB.pvcUid
    and .sessionA.pvUid != .sessionB.pvUid
    and .sessionA.serviceAccountUid != .sessionB.serviceAccountUid
    and .uidsDistinct == {
        pod: true,
        pv: true,
        pvc: true,
        serviceAccount: true
    }
    and .workspaceProbe == "passed"
    and .peerPvcMountAbsent == true
    and .sharedSkillsReadOnly == true
)
and (
    .network
    | type == "object"
    and keys == [
        "cniDefaultDenyProbe",
        "controllerServiceExposure",
        "perSessionPolicyContract",
        "workerServicesAbsent"
    ]
    and .cniDefaultDenyProbe == "passed"
    and .perSessionPolicyContract == "passed"
    and .workerServicesAbsent == true
    and .controllerServiceExposure == "cluster-internal"
)
and (
    .lifecycle
    | type == "object"
    and keys == ["release", "replacement", "resume", "suspension"]
    and (
        .replacement
        | type == "object"
        and keys == [
            "generation",
            "peerUnaffected",
            "podUidChanged",
            "pvUid",
            "pvUidPreserved",
            "pvcUidPreserved",
            "workspaceStatePreserved"
        ]
        and (.generation | type == "number" and floor == . and . >= 2)
        and (.pvUid | nonempty_string)
        and .pvUid == $root.isolation.sessionA.pvUid
        and .podUidChanged == true
        and .pvcUidPreserved == true
        and .pvUidPreserved == true
        and .workspaceStatePreserved == true
        and .peerUnaffected == true
    )
    and (
        .suspension
        | type == "object"
        and keys == [
            "computeResourcesAbsent",
            "peerUnaffected",
            "phase",
            "pvUid",
            "pvUidPreserved",
            "pvcApiObjectRetained"
        ]
        and .phase == "suspended"
        and (.pvUid | nonempty_string)
        and .pvUid == $root.isolation.sessionA.pvUid
        and .computeResourcesAbsent == true
        and .pvcApiObjectRetained == true
        and .pvUidPreserved == true
        and .peerUnaffected == true
    )
    and (
        .resume
        | type == "object"
        and keys == [
            "generation",
            "podUidChanged",
            "pvUid",
            "pvUidPreserved",
            "pvcUidPreserved",
            "workspaceStatePreserved"
        ]
        and (.generation | type == "number" and floor == . and . >= 3)
        and (.pvUid | nonempty_string)
        and .pvUid == $root.isolation.sessionA.pvUid
        and .podUidChanged == true
        and .pvcUidPreserved == true
        and .pvUidPreserved == true
        and .workspaceStatePreserved == true
    )
    and (
        .release
        | type == "object"
        and keys == [
            "acknowledged",
            "backingVolumeReclaimProof",
            "observedPreReleasePvReclaimPolicy",
            "peerUnaffected",
            "pvcApiObjectAbsent"
        ]
        and .acknowledged == true
        and .pvcApiObjectAbsent == true
        and .backingVolumeReclaimProof == "not-asserted"
        and (.observedPreReleasePvReclaimPolicy | nonempty_string)
        and .peerUnaffected == true
    )
)
and .claimBoundary == {
    backingVolumeDeletion: "not-asserted",
    fullDiscordIngressE2e: "not-tested",
    gitWorktreeIsolation: "not-tested",
    productionAgentCli: "not-tested"
}
