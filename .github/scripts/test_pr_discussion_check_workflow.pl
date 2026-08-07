#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw($Bin);
use Test::More;

my $workflow_path = "$Bin/../workflows/pr-discussion-check.yml";
open my $workflow_handle, '<', $workflow_path
    or die "Cannot read $workflow_path: $!";
my $workflow = do { local $/; <$workflow_handle> };
close $workflow_handle;

like(
    $workflow,
    qr/^\s+contents: read$/m,
    'grants the trusted policy checkout read-only repository access',
);

like(
    $workflow,
    qr{uses: actions/checkout\@[0-9a-f]{40}.*?ref: \$\{\{ github\.event\.repository\.default_branch \}\}.*?persist-credentials: false}s,
    'checks out only the trusted default-branch policy without credentials',
);

like(
    $workflow,
    qr{run: perl \.github/scripts/test_pr_discussion_check_workflow\.pl},
    'runs this workflow contract before the policy',
);

like(
    $workflow,
    qr{name: Create GitHub App token.*?if: github\.repository == 'openabdev/openab'.*?uses: actions/create-github-app-token\@v3}s,
    'creates the organization App token only in the upstream repository',
);

like(
    $workflow,
    qr{github-token: \$\{\{ steps\.app-token\.outputs\.token \|\| github\.token \}\}},
    'falls back to the repository-scoped token when the App step is skipped',
);

like(
    $workflow,
    qr{const isUpstream =\s*context\.repo\.owner === 'openabdev' &&\s*context\.repo\.repo === 'openab';},
    'defines the upstream repository trust boundary explicitly',
);

like(
    $workflow,
    qr{if \(isUpstream\) \{.*?github\.rest\.teams\.listMembersInOrg\(}s,
    'queries the OpenAB maintainer team only inside the upstream boundary',
);

my $team_lookup_count = () =
    $workflow =~ /github\.rest\.teams\.listMembersInOrg\(/g;
is(
    $team_lookup_count,
    1,
    'contains exactly one guarded maintainer-team lookup',
);

unlike(
    $workflow,
    qr{ref:\s*\$\{\{\s*github\.event\.pull_request\.(?:head|merge)},
    'never checks out contributor-controlled pull-request code',
);

done_testing();
