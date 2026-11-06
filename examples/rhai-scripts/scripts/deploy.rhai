// A release runbook. Each host call is a saved step, so a crash or a
// `kill -9` resumes after the last step that finished.

let version = if ARGS.len() > 0 { ARGS[0] } else { "dev" };
print(`deploying ${version}`);

progress("build");
let sha = sh("git rev-parse --short HEAD 2>/dev/null || echo no-git");
write_file(`out/${version}/BUILD`, `version=${version}\nsha=${sha}\n`);
print(`built ${version} at ${sha}`);

progress("smoke test");
let build = read_file(`out/${version}/BUILD`);
if !build.contains(sha) {
    throw `the build file does not name ${sha}`;
}

// A durable timer. The run survives a reboot while it waits.
progress("cool down");
sleep(1);

// The run stops here until someone runs `harvest-rhai approve <run>`.
progress("waiting for approval");
let approver = wait_for("approve");
print(`approved by ${approver}`);

progress("release");
sh(`echo ${version} > out/CURRENT`);
#{ version: version, sha: sha, approver: approver }
