#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Private, non-authorizing Native Segment V1 bounded-resource evidence tool.

mod crc32c;
mod error;
mod fixture;
mod harness;
mod ledger;
mod model;
mod root;
mod sha256;
mod stream;

#[cfg(test)]
mod tests;

use crate::error::{EvidenceError, Result};
use crate::root::EvidenceRoot;
use std::env;
use std::path::PathBuf;

fn main() {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if let Err(error) = run(&arguments) {
        eprintln!("och-v2-evidence: {error}");
        std::process::exit(1);
    }
}

fn run(arguments: &[String]) -> Result<()> {
    let (command, arguments) = arguments.split_first().ok_or(EvidenceError::Usage)?;
    match command.as_str() {
        "prepare-root" => prepare_root_command(arguments),
        "generate" => generate_command(arguments),
        "stream-build" => build_command(arguments),
        "stream-validate" => validate_command(arguments),
        "validate-set" => validate_set_command(arguments),
        "report-ledger" if arguments.is_empty() => {
            ledger::print_ledger();
            Ok(())
        }
        "native-foundation-check" => harness::foundation_check_command(arguments),
        "native-harness-check" => harness::harness_check_command(arguments),
        "native-collect" => harness::collect_command(arguments),
        "__native-child" => root::hidden_child_command(arguments),
        _ => Err(EvidenceError::Usage),
    }
}

fn prepare_root_command(arguments: &[String]) -> Result<()> {
    let parsed = Parsed::new(arguments, &["--root"], &[])?;
    let _root = EvidenceRoot::prepare(&PathBuf::from(parsed.required("--root")?))?;
    println!("schema=och-v2-evidence-root-preparation-v1");
    println!("safe=true");
    Ok(())
}

fn generate_command(arguments: &[String]) -> Result<()> {
    let parsed = Parsed::new(arguments, &["--root", "--case", "--seed"], &[])?;
    let root = EvidenceRoot::prepare(&PathBuf::from(parsed.required("--root")?))?;
    let case = parsed.required("--case")?;
    let seed = parsed
        .required("--seed")?
        .parse::<u64>()
        .map_err(|_| EvidenceError::Usage)?;
    if case == "open-64" {
        generate_open_64(&root, seed)?;
        println!("schema=och-v2-evidence-generation-v1");
        println!("case=open-64");
        println!("pairs=64");
        println!("external_sort_workspace_bytes=0");
        return Ok(());
    }
    let meta = fixture::generate(&root, case, seed)?;
    println!("schema=och-v2-evidence-generation-v1");
    println!("case={}", meta.case);
    println!("seed={}", meta.seed);
    println!("source_bytes={}", meta.source_length);
    println!("frames={}", meta.frame_count);
    println!("series={}", meta.series_count);
    println!("observations={}", meta.observation_count);
    println!("source_crc32c={:08x}", meta.source_checksum);
    println!("external_sort_workspace_bytes=0");
    Ok(())
}

fn generate_open_64(root: &EvidenceRoot, seed: u64) -> Result<()> {
    let mut set = String::from("schema=och-v2-evidence-set-v1\n");
    for index in 0..64_u64 {
        let case = format!("open-64-{index:02}");
        let pair_seed = seed.checked_add(index).ok_or(EvidenceError::Bounds)?;
        fixture::generate_representative_named(root, &case, pair_seed)?;
        let report = stream::build(root, &case, false)?;
        if report.controlled_bytes_after != 0 || ledger::active_controlled_bytes() != 0 {
            return Err(EvidenceError::Bounds);
        }
        set.push_str(&case);
        set.push('\n');
    }
    root.pr03c_set("open-64")?.write(set.as_bytes())
}

fn build_command(arguments: &[String]) -> Result<()> {
    let parsed = Parsed::new(arguments, &["--root", "--case"], &["--keep-on-failure"])?;
    let root = EvidenceRoot::open(&PathBuf::from(parsed.required("--root")?))?;
    let case = parsed.required("--case")?;
    let meta = model::FixtureMeta::read(root.pr03c_case(case)?.open_fixture_meta()?)?;
    let report = stream::build(&root, case, parsed.flag("--keep-on-failure"));
    match report {
        Ok(report) => {
            report.print("stream-build", &meta);
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn validate_command(arguments: &[String]) -> Result<()> {
    let parsed = Parsed::new(arguments, &["--root", "--case"], &[])?;
    let root = EvidenceRoot::open(&PathBuf::from(parsed.required("--root")?))?;
    let case = parsed.required("--case")?;
    let meta = model::FixtureMeta::read(root.pr03c_case(case)?.open_fixture_meta()?)?;
    let report = stream::validate(&root, case)?;
    report.print("stream-validate", &meta);
    Ok(())
}

fn validate_set_command(arguments: &[String]) -> Result<()> {
    let parsed = Parsed::new(arguments, &["--root", "--set"], &[])?;
    let root = EvidenceRoot::open(&PathBuf::from(parsed.required("--root")?))?;
    let set_name = parsed.required("--set")?;
    let text = model::read_bounded_text(
        root.pr03c_set(set_name)?.open()?,
        8_192,
        EvidenceError::Bounds,
    )?;
    let mut lines = text.lines();
    if lines.next() != Some("schema=och-v2-evidence-set-v1") {
        return Err(EvidenceError::InvalidFixture);
    }
    let mut count = 0_usize;
    for case in lines {
        count = count.checked_add(1).ok_or(EvidenceError::Bounds)?;
        if count > 64 {
            return Err(EvidenceError::Bounds);
        }
        let report = stream::validate(&root, case)?;
        if report.controlled_bytes_after != 0 || ledger::active_controlled_bytes() != 0 {
            return Err(EvidenceError::Bounds);
        }
    }
    if count == 0 {
        return Err(EvidenceError::InvalidFixture);
    }
    println!("schema=och-v2-evidence-set-validation-v1");
    println!("set={set_name}");
    println!("pairs={count}");
    println!("sequential_pair_state=true");
    println!(
        "controlled_bytes_after={}",
        ledger::active_controlled_bytes()
    );
    println!("external_sort_workspace_bytes=0");
    Ok(())
}

struct Parsed<'a> {
    values: std::collections::BTreeMap<&'a str, &'a str>,
    flags: std::collections::BTreeSet<&'a str>,
}

impl<'a> Parsed<'a> {
    fn new(arguments: &'a [String], valued_options: &[&str], flags: &[&str]) -> Result<Self> {
        let mut values = std::collections::BTreeMap::new();
        let mut present_flags = std::collections::BTreeSet::new();
        let mut index = 0_usize;
        while index < arguments.len() {
            let argument = arguments[index].as_str();
            if flags.contains(&argument) {
                if !present_flags.insert(argument) {
                    return Err(EvidenceError::Usage);
                }
                index += 1;
            } else if valued_options.contains(&argument) {
                let value = arguments.get(index + 1).ok_or(EvidenceError::Usage)?;
                if value.starts_with("--") || values.insert(argument, value.as_str()).is_some() {
                    return Err(EvidenceError::Usage);
                }
                index += 2;
            } else {
                return Err(EvidenceError::Usage);
            }
        }
        Ok(Self {
            values,
            flags: present_flags,
        })
    }

    fn required(&self, name: &str) -> Result<&'a str> {
        self.values.get(name).copied().ok_or(EvidenceError::Usage)
    }

    fn flag(&self, name: &str) -> bool {
        self.flags.contains(name)
    }
}
