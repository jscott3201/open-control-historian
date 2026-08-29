#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Command-line entry point for the private workspace dependency-policy check.

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn usage() -> &'static str {
    "usage: och-policy check [--manifest-path <path>]"
}

fn main() -> ExitCode {
    let mut arguments = env::args_os().skip(1);
    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("check")) {
        eprintln!("{}", usage());
        return ExitCode::from(2);
    }

    let mut manifest_path = PathBuf::from("Cargo.toml");
    while let Some(argument) = arguments.next() {
        if argument != "--manifest-path" {
            eprintln!(
                "unknown argument: {}\n{}",
                argument.to_string_lossy(),
                usage()
            );
            return ExitCode::from(2);
        }
        let Some(path) = arguments.next() else {
            eprintln!("--manifest-path requires a path\n{}", usage());
            return ExitCode::from(2);
        };
        manifest_path = PathBuf::from(path);
    }

    match och_policy::check_workspace(&manifest_path) {
        Ok(summary) => {
            println!(
                "dependency policy passed: {} native root(s), {} package(s) in the native closure",
                summary.native_root_count(),
                summary.native_closure_package_count()
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("dependency policy failed:\n{error}");
            ExitCode::FAILURE
        }
    }
}
