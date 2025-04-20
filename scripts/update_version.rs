use std::fs;
use std::process::{Command, exit};

fn main() {
    // Read Cargo.toml
    let cargo_toml = match fs::read_to_string("Cargo.toml") {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading Cargo.toml: {}", e);
            exit(1);
        }
    };

    // Extract current version
    let version_line = cargo_toml.lines()
        .find(|line| line.trim().starts_with("version = "))
        .unwrap_or_else(|| {
            eprintln!("Could not find version in Cargo.toml");
            exit(1);
        });

    let current_version = version_line
        .split('=')
        .nth(1)
        .unwrap_or_else(|| {
            eprintln!("Invalid version format in Cargo.toml");
            exit(1);
        })
        .trim()
        .trim_matches('"');

    println!("Current version: {}", current_version);

    // Parse version components
    let version_parts: Vec<&str> = current_version.split('.').collect();
    if version_parts.len() != 3 {
        eprintln!("Invalid version format: {}", current_version);
        exit(1);
    }

    let major: u32 = version_parts[0].parse().unwrap_or_else(|_| {
        eprintln!("Invalid major version: {}", version_parts[0]);
        exit(1);
    });
    let minor: u32 = version_parts[1].parse().unwrap_or_else(|_| {
        eprintln!("Invalid minor version: {}", version_parts[1]);
        exit(1);
    });
    let patch: u32 = version_parts[2].parse().unwrap_or_else(|_| {
        eprintln!("Invalid patch version: {}", version_parts[2]);
        exit(1);
    });

    // Get commit messages since last tag
    let output = Command::new("git")
        .args(&["log", "--pretty=format:%s", &format!("v{}..HEAD", current_version)])
        .output()
        .unwrap_or_else(|e| {
            // If the tag doesn't exist, get all commit messages
            if e.to_string().contains("unknown revision") {
                Command::new("git")
                    .args(&["log", "--pretty=format:%s"])
                    .output()
                    .unwrap_or_else(|e| {
                        eprintln!("Error getting commit messages: {}", e);
                        exit(1);
                    })
            } else {
                eprintln!("Error getting commit messages: {}", e);
                exit(1);
            }
        });

    let commit_messages = String::from_utf8_lossy(&output.stdout);

    // Determine version bump
    let mut new_major = major;
    let mut new_minor = minor;
    let mut new_patch = patch;

    let mut has_breaking_change = false;
    let mut has_feature = false;
    let mut has_fix = false;

    for message in commit_messages.lines() {
        println!("Analyzing commit: {}", message);

        if message.contains("BREAKING CHANGE") {
            println!("  - Contains BREAKING CHANGE");
            has_breaking_change = true;
            break; // Major change takes precedence
        }

        if message.starts_with("feat:") || message.starts_with("feature:") {
            println!("  - Contains feature");
            has_feature = true;
        }

        if message.starts_with("fix:") || message.starts_with("bugfix:") {
            println!("  - Contains fix");
            has_fix = true;
        }
    }

    // Update version based on commit messages and current version
    if major == 0 {
        // Pre-1.0 versioning: breaking changes bump minor version
        if has_breaking_change || has_feature {
            new_minor += 1;
            new_patch = 0;
            if has_breaking_change {
                println!("BREAKING CHANGE detected. Bumping minor version (pre-1.0).");
            } else {
                println!("New feature detected. Bumping minor version.");
            }
        } else if has_fix {
            new_patch += 1;
            println!("Bug fix detected. Bumping patch version.");
        } else {
            println!("No version bump needed.");
            exit(0);
        }
    } else {
        // Post-1.0 versioning: standard semver rules
        if has_breaking_change {
            new_major += 1;
            new_minor = 0;
            new_patch = 0;
            println!("BREAKING CHANGE detected. Bumping major version.");
        } else if has_feature {
            new_minor += 1;
            new_patch = 0;
            println!("New feature detected. Bumping minor version.");
        } else if has_fix {
            new_patch += 1;
            println!("Bug fix detected. Bumping patch version.");
        } else {
            println!("No version bump needed.");
            exit(0);
        }
    }

    let new_version = format!("{}.{}.{}", new_major, new_minor, new_patch);
    println!("New version: {}", new_version);

    // Update Cargo.toml
    let new_cargo_toml = cargo_toml.replace(
        &format!("version = \"{}\"", current_version),
        &format!("version = \"{}\"", new_version)
    );

    match fs::write("Cargo.toml", new_cargo_toml) {
        Ok(_) => println!("Updated Cargo.toml with new version: {}", new_version),
        Err(e) => {
            eprintln!("Error writing to Cargo.toml: {}", e);
            exit(1);
        }
    }

    println!("Note: Git tagging will be handled by the CI during the publish phase.");
    println!("Version update complete!");
}
