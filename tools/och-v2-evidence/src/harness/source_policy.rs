//! Test-only closed-source policy for the g1 V2 store-child executor.
//!
//! The policy reads repository source, so it is deliberately not a production
//! harness module. Production `harness/*.rs` modules are closed below and only
//! `v2_io.rs` may own low-level store-child filesystem operations. `root.rs`
//! owns evidence-parent/child lifecycle, while `#[cfg(test)]` fixture modules
//! own their temporary setup; neither is a V2 store-child executor path.

use crate::error::{EvidenceError, Result};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const MAX_SOURCE_BYTES: usize = 512 * 1_024;
const MAX_TOKENS: usize = 128 * 1_024;
const MAX_IDENTIFIER_BYTES: usize = 256;
const OWNER_FILE: &str = "v2_io.rs";
const PRODUCTION_FILES: [&str; 8] = [
    "fault.rs",
    "inventory.rs",
    "mod.rs",
    "oracle.rs",
    "runtime.rs",
    "schema.rs",
    "transaction.rs",
    OWNER_FILE,
];
const TEST_ONLY_FILES: [&str; 1] = ["source_policy.rs"];
const PRODUCTION_MODULES: [&str; 7] = [
    "fault",
    "inventory",
    "oracle",
    "runtime",
    "schema",
    "transaction",
    "v2_io",
];
const APPROVED_CRATE_HELPERS: [&str; 5] = ["crc32c", "error", "fixture", "root", "sha256"];
const IO_METHODS: &[&str] = &[
    "canonicalize",
    "create_dir",
    "create_dir_all",
    "create_new",
    "exists",
    "flush",
    "is_dir",
    "is_file",
    "metadata",
    "read",
    "read_exact",
    "read_to_end",
    "read_to_string",
    "rename",
    "remove_dir",
    "remove_dir_all",
    "remove_file",
    "seek",
    "set_len",
    "sync_all",
    "sync_data",
    "try_lock",
    "write",
    "write_all",
];

#[derive(Clone, Debug, Eq, PartialEq)]
enum Token {
    Ident(String),
    Punct(char),
    Number,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ModuleDeclaration {
    name: String,
    test_only: bool,
    external: bool,
}

fn validate_current_source_boundary() -> Result<()> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/harness");
    let names = source_inventory(&directory)?;
    validate_source_inventory(&names)?;

    let root_tokens = lex(&read_source(&directory.join("mod.rs"))?)?;
    validate_root_modules(&root_tokens)?;

    for name in PRODUCTION_FILES {
        let tokens = lex(&read_source(&directory.join(name))?)?;
        reject_path_attributes(&tokens)?;
        if name != "mod.rs" {
            validate_leaf_modules(&tokens)?;
        }
        let production = production_tokens(&tokens)?;
        if name != OWNER_FILE {
            reject_low_level_io(&production)?;
        }
        reject_unknown_helper_paths(&production)?;
    }
    Ok(())
}

fn source_inventory(directory: &Path) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(directory).map_err(|_| EvidenceError::Io)? {
        let entry = entry.map_err(|_| EvidenceError::Io)?;
        if !entry.file_type().map_err(|_| EvidenceError::Io)?.is_file() {
            return Err(EvidenceError::InvalidHarness);
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| EvidenceError::InvalidHarness)?;
        if !names.insert(name) {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    Ok(names)
}

fn validate_source_inventory(names: &BTreeSet<String>) -> Result<()> {
    let expected = PRODUCTION_FILES
        .into_iter()
        .chain(TEST_ONLY_FILES)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if names == &expected {
        Ok(())
    } else {
        Err(EvidenceError::InvalidHarness)
    }
}

fn read_source(path: &Path) -> Result<String> {
    let metadata = fs::metadata(path).map_err(|_| EvidenceError::Io)?;
    let length = usize::try_from(metadata.len()).map_err(|_| EvidenceError::Bounds)?;
    if !metadata.is_file() || length > MAX_SOURCE_BYTES {
        return Err(EvidenceError::Bounds);
    }
    let source = fs::read_to_string(path).map_err(|_| EvidenceError::Io)?;
    if source.len() != length {
        return Err(EvidenceError::Io);
    }
    Ok(source)
}

fn validate_root_modules(tokens: &[Token]) -> Result<()> {
    reject_path_attributes(tokens)?;
    let actual = module_declarations(tokens)?;
    let mut expected = PRODUCTION_MODULES
        .into_iter()
        .map(|name| ModuleDeclaration {
            name: name.to_owned(),
            test_only: false,
            external: true,
        })
        .collect::<BTreeSet<_>>();
    expected.insert(ModuleDeclaration {
        name: "source_policy".to_owned(),
        test_only: true,
        external: true,
    });
    expected.insert(ModuleDeclaration {
        name: "tests".to_owned(),
        test_only: true,
        external: false,
    });
    if actual == expected {
        Ok(())
    } else {
        Err(EvidenceError::InvalidHarness)
    }
}

fn validate_leaf_modules(tokens: &[Token]) -> Result<()> {
    let actual = module_declarations(tokens)?;
    let expected = [ModuleDeclaration {
        name: "tests".to_owned(),
        test_only: true,
        external: false,
    }]
    .into_iter()
    .collect();
    if actual == expected {
        Ok(())
    } else {
        Err(EvidenceError::InvalidHarness)
    }
}

fn module_declarations(tokens: &[Token]) -> Result<BTreeSet<ModuleDeclaration>> {
    let mut declarations = BTreeSet::new();
    let mut index = 0_usize;
    let mut brace_depth = 0_usize;
    let mut pending_test = false;
    while index < tokens.len() {
        if brace_depth == 0
            && is_punct(tokens.get(index), '#')
            && is_punct(tokens.get(index + 1), '[')
        {
            let end = attribute_end(tokens, index + 1)?;
            pending_test |= attribute_is_cfg_test(&tokens[index + 2..end]);
            index = end + 1;
            continue;
        }
        if brace_depth == 0 && ident(tokens.get(index)) == Some("mod") {
            let name = ident(tokens.get(index + 1)).ok_or(EvidenceError::InvalidHarness)?;
            let external = if is_punct(tokens.get(index + 2), ';') {
                true
            } else if is_punct(tokens.get(index + 2), '{') {
                false
            } else {
                return Err(EvidenceError::InvalidHarness);
            };
            if !declarations.insert(ModuleDeclaration {
                name: name.to_owned(),
                test_only: pending_test,
                external,
            }) {
                return Err(EvidenceError::InvalidHarness);
            }
            pending_test = false;
        }
        match tokens.get(index) {
            Some(Token::Punct('{')) => {
                brace_depth = brace_depth.checked_add(1).ok_or(EvidenceError::Bounds)?;
                pending_test = false;
            }
            Some(Token::Punct('}')) => {
                brace_depth = brace_depth
                    .checked_sub(1)
                    .ok_or(EvidenceError::InvalidHarness)?;
            }
            Some(Token::Punct(';')) if brace_depth == 0 => pending_test = false,
            _ => {}
        }
        index += 1;
    }
    let declared_count = tokens
        .iter()
        .filter(|token| ident(Some(token)) == Some("mod"))
        .count();
    if brace_depth == 0 && declared_count == declarations.len() {
        Ok(declarations)
    } else {
        Err(EvidenceError::InvalidHarness)
    }
}

fn production_tokens(tokens: &[Token]) -> Result<Vec<Token>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(tokens.len())
        .map_err(|_| EvidenceError::Bounds)?;
    let mut index = 0_usize;
    let mut brace_depth = 0_usize;
    while index < tokens.len() {
        if brace_depth == 0
            && is_punct(tokens.get(index), '#')
            && is_punct(tokens.get(index + 1), '[')
        {
            let end = attribute_end(tokens, index + 1)?;
            if attribute_is_cfg_test(&tokens[index + 2..end]) {
                index = end + 1;
                while matches!(ident(tokens.get(index)), Some("pub"))
                    || is_punct(tokens.get(index), '(')
                    || is_punct(tokens.get(index), ')')
                    || matches!(ident(tokens.get(index)), Some("crate" | "super" | "self"))
                {
                    index += 1;
                }
                if ident(tokens.get(index)) != Some("mod") {
                    return Err(EvidenceError::InvalidHarness);
                }
                index += 2;
                if is_punct(tokens.get(index), ';') {
                    index += 1;
                    continue;
                }
                if !is_punct(tokens.get(index), '{') {
                    return Err(EvidenceError::InvalidHarness);
                }
                index = skip_braced(tokens, index)?;
                continue;
            }
        }
        let token = tokens.get(index).ok_or(EvidenceError::InvalidHarness)?;
        match token {
            Token::Punct('{') => {
                brace_depth = brace_depth.checked_add(1).ok_or(EvidenceError::Bounds)?;
            }
            Token::Punct('}') => {
                brace_depth = brace_depth
                    .checked_sub(1)
                    .ok_or(EvidenceError::InvalidHarness)?;
            }
            _ => {}
        }
        output.push(token.clone());
        index += 1;
    }
    if brace_depth == 0 {
        Ok(output)
    } else {
        Err(EvidenceError::InvalidHarness)
    }
}

fn reject_path_attributes(tokens: &[Token]) -> Result<()> {
    let mut index = 0_usize;
    while index + 1 < tokens.len() {
        if is_punct(tokens.get(index), '#') && is_punct(tokens.get(index + 1), '[') {
            let end = attribute_end(tokens, index + 1)?;
            if tokens[index + 2..end]
                .iter()
                .any(|token| ident(Some(token)) == Some("path"))
            {
                return Err(EvidenceError::InvalidHarness);
            }
            index = end + 1;
        } else {
            index += 1;
        }
    }
    Ok(())
}

fn reject_low_level_io(tokens: &[Token]) -> Result<()> {
    let mut std_aliases = BTreeSet::from(["std".to_owned()]);
    let mut index = 0_usize;
    while index < tokens.len() {
        if ident(tokens.get(index)) == Some("use")
            && ident(tokens.get(index + 1)) == Some("std")
            && ident(tokens.get(index + 2)) == Some("as")
        {
            std_aliases.insert(
                ident(tokens.get(index + 3))
                    .ok_or(EvidenceError::InvalidHarness)?
                    .to_owned(),
            );
        }
        if ident(tokens.get(index)) == Some("extern")
            && ident(tokens.get(index + 1)) == Some("crate")
            && ident(tokens.get(index + 2)) == Some("std")
            && ident(tokens.get(index + 3)) == Some("as")
        {
            std_aliases.insert(
                ident(tokens.get(index + 4))
                    .ok_or(EvidenceError::InvalidHarness)?
                    .to_owned(),
            );
        }
        if matches!(
            ident(tokens.get(index)),
            Some("fs" | "File" | "OpenOptions")
        ) {
            return Err(EvidenceError::InvalidHarness);
        }
        if is_punct(tokens.get(index), '.')
            && ident(tokens.get(index + 1)).is_some_and(|name| IO_METHODS.contains(&name))
        {
            return Err(EvidenceError::InvalidHarness);
        }
        if ident(tokens.get(index)).is_some_and(|name| std_aliases.contains(name))
            && is_double_colon(tokens, index + 1)
        {
            match ident(tokens.get(index + 3)) {
                Some("fs") => return Err(EvidenceError::InvalidHarness),
                Some("io") if is_double_colon(tokens, index + 4) => {
                    let io_item = ident(tokens.get(index + 6));
                    if io_item != Some("ErrorKind")
                        && (io_item.is_some()
                            || tokens[index + 6..]
                                .iter()
                                .take_while(|token| !is_punct(Some(token), ';'))
                                .any(|token| {
                                    matches!(ident(Some(token)), Some("Read" | "Write" | "Seek"))
                                }))
                    {
                        return Err(EvidenceError::InvalidHarness);
                    }
                }
                _ => {}
            }
        }
        index += 1;
    }
    Ok(())
}

fn reject_unknown_helper_paths(tokens: &[Token]) -> Result<()> {
    let mut index = 0_usize;
    while index + 3 < tokens.len() {
        if ident(tokens.get(index)) == Some("crate") && is_double_colon(tokens, index + 1) {
            let helper = ident(tokens.get(index + 3)).ok_or(EvidenceError::InvalidHarness)?;
            if !APPROVED_CRATE_HELPERS.contains(&helper) {
                return Err(EvidenceError::InvalidHarness);
            }
        }
        if ident(tokens.get(index)) == Some("super") && is_double_colon(tokens, index + 1) {
            let helper = ident(tokens.get(index + 3)).ok_or(EvidenceError::InvalidHarness)?;
            if !PRODUCTION_MODULES.contains(&helper) {
                return Err(EvidenceError::InvalidHarness);
            }
        }
        index += 1;
    }
    Ok(())
}

fn lex(source: &str) -> Result<Vec<Token>> {
    if source.len() > MAX_SOURCE_BYTES {
        return Err(EvidenceError::Bounds);
    }
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0_usize;
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if bytes[index..].starts_with(b"//") {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes[index..].starts_with(b"/*") {
            index = skip_block_comment(bytes, index)?;
            continue;
        }
        if let Some(end) = raw_string_end(bytes, index)? {
            index = end;
            continue;
        }
        if bytes[index] == b'"' || (bytes[index] == b'b' && bytes.get(index + 1) == Some(&b'"')) {
            index = skip_quoted(bytes, index + usize::from(bytes[index] == b'b'), b'"')?;
            continue;
        }
        if bytes[index] == b'\'' && looks_like_char_literal(bytes, index) {
            index = skip_quoted(bytes, index, b'\'')?;
            continue;
        }
        if is_ident_start(bytes[index]) {
            let start = index;
            index += 1;
            while index < bytes.len() && is_ident_continue(bytes[index]) {
                index += 1;
            }
            if index - start > MAX_IDENTIFIER_BYTES {
                return Err(EvidenceError::Bounds);
            }
            tokens.push(Token::Ident(source[start..index].to_owned()));
        } else if bytes[index].is_ascii_digit() {
            index += 1;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
            {
                index += 1;
            }
            tokens.push(Token::Number);
        } else {
            tokens.push(Token::Punct(char::from(bytes[index])));
            index += 1;
        }
        if tokens.len() > MAX_TOKENS {
            return Err(EvidenceError::Bounds);
        }
    }
    Ok(tokens)
}

fn skip_block_comment(bytes: &[u8], mut index: usize) -> Result<usize> {
    let mut depth = 0_usize;
    while index < bytes.len() {
        if bytes[index..].starts_with(b"/*") {
            depth = depth.checked_add(1).ok_or(EvidenceError::Bounds)?;
            index += 2;
        } else if bytes[index..].starts_with(b"*/") {
            depth = depth.checked_sub(1).ok_or(EvidenceError::InvalidHarness)?;
            index += 2;
            if depth == 0 {
                return Ok(index);
            }
        } else {
            index += 1;
        }
    }
    Err(EvidenceError::InvalidHarness)
}

fn raw_string_end(bytes: &[u8], index: usize) -> Result<Option<usize>> {
    let mut cursor = index;
    if bytes.get(cursor) == Some(&b'b') {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'r') {
        return Ok(None);
    }
    cursor += 1;
    let hash_start = cursor;
    while bytes.get(cursor) == Some(&b'#') {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'"') {
        return Ok(None);
    }
    let hashes = cursor - hash_start;
    cursor += 1;
    while cursor < bytes.len() {
        if bytes[cursor] == b'"'
            && bytes.get(cursor + 1..cursor + 1 + hashes)
                == Some(&bytes[hash_start..hash_start + hashes])
        {
            return Ok(Some(cursor + 1 + hashes));
        }
        cursor += 1;
    }
    Err(EvidenceError::InvalidHarness)
}

fn skip_quoted(bytes: &[u8], quote_index: usize, quote: u8) -> Result<usize> {
    let mut index = quote_index + 1;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            index = index.checked_add(2).ok_or(EvidenceError::Bounds)?;
        } else if bytes[index] == quote {
            return Ok(index + 1);
        } else {
            index += 1;
        }
    }
    Err(EvidenceError::InvalidHarness)
}

fn looks_like_char_literal(bytes: &[u8], index: usize) -> bool {
    let mut cursor = index + 1;
    if bytes.get(cursor) == Some(&b'\\') {
        cursor += 2;
    } else {
        cursor += 1;
    }
    bytes.get(cursor) == Some(&b'\'')
}

fn attribute_end(tokens: &[Token], open: usize) -> Result<usize> {
    let mut depth = 0_usize;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        match token {
            Token::Punct('[') => depth = depth.checked_add(1).ok_or(EvidenceError::Bounds)?,
            Token::Punct(']') => {
                depth = depth.checked_sub(1).ok_or(EvidenceError::InvalidHarness)?;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
    }
    Err(EvidenceError::InvalidHarness)
}

fn attribute_is_cfg_test(tokens: &[Token]) -> bool {
    tokens.iter().any(|token| ident(Some(token)) == Some("cfg"))
        && tokens
            .iter()
            .any(|token| ident(Some(token)) == Some("test"))
}

fn skip_braced(tokens: &[Token], open: usize) -> Result<usize> {
    let mut depth = 0_usize;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        match token {
            Token::Punct('{') => depth = depth.checked_add(1).ok_or(EvidenceError::Bounds)?,
            Token::Punct('}') => {
                depth = depth.checked_sub(1).ok_or(EvidenceError::InvalidHarness)?;
                if depth == 0 {
                    return Ok(index + 1);
                }
            }
            _ => {}
        }
    }
    Err(EvidenceError::InvalidHarness)
}

fn ident(token: Option<&Token>) -> Option<&str> {
    match token {
        Some(Token::Ident(value)) => Some(value),
        _ => None,
    }
}

fn is_punct(token: Option<&Token>, expected: char) -> bool {
    matches!(token, Some(Token::Punct(actual)) if *actual == expected)
}

fn is_double_colon(tokens: &[Token], index: usize) -> bool {
    is_punct(tokens.get(index), ':') && is_punct(tokens.get(index + 1), ':')
}

const fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

const fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_current_production_inventory_and_owner_are_accepted() {
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/harness");
        let names = source_inventory(&directory).expect("read current source inventory");
        validate_source_inventory(&names).expect("exact source inventory");
        let root_tokens =
            lex(&read_source(&directory.join("mod.rs")).expect("read root harness module"))
                .expect("lex root harness module");
        validate_root_modules(&root_tokens).expect("closed root module declarations");
        for name in PRODUCTION_FILES {
            let tokens = lex(&read_source(&directory.join(name)).expect("read production module"))
                .expect("lex production module");
            reject_path_attributes(&tokens).unwrap_or_else(|_| panic!("path escape in {name}"));
            if name != "mod.rs" {
                validate_leaf_modules(&tokens)
                    .unwrap_or_else(|_| panic!("unknown module declaration in {name}"));
            }
            let production = production_tokens(&tokens)
                .unwrap_or_else(|_| panic!("invalid production boundary in {name}"));
            if name != OWNER_FILE {
                reject_low_level_io(&production)
                    .unwrap_or_else(|_| panic!("low-level I/O escape in {name}"));
            }
            reject_unknown_helper_paths(&production)
                .unwrap_or_else(|_| panic!("unknown helper path in {name}"));
        }
        validate_current_source_boundary().expect("closed current source boundary");
    }

    #[test]
    fn unlisted_production_module_is_rejected() {
        let mut names = PRODUCTION_FILES
            .into_iter()
            .chain(TEST_ONLY_FILES)
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        names.insert("escape.rs".to_owned());
        assert!(validate_source_inventory(&names).is_err());
    }

    #[test]
    fn aliases_and_qualified_low_level_io_are_rejected_after_lexing() {
        for hostile in [
            "use std::{fs}; fn escape() { fs::write(\"x\", b\"x\"); }",
            "use std::fs as disk; fn escape() { disk::rename(\"a\", \"b\"); }",
            "use std as platform; fn escape() { platform::fs::remove_file(\"x\"); }",
            "use std::fs::File as Handle; fn escape() { let _ = Handle::open(\"x\"); }",
            "use std::fs::OpenOptions as Options; fn escape() { let _ = Options::new(); }",
            "use std::io::{Read as Input, Write as Output, Seek as Move};",
            "fn escape(handle: &mut Handle) { handle.read(buf); handle.sync_all(); }",
            "fn escape(owner: &mut Owner) { owner.write(buf); owner.rename(dst); owner.remove_file(); }",
        ] {
            let tokens = lex(hostile).expect("lex hostile source");
            assert!(reject_low_level_io(&tokens).is_err(), "{hostile}");
        }
    }

    #[test]
    fn comments_and_literals_do_not_create_false_io_owners() {
        let benign = r#"
            // use std::fs as fake;
            /* File::open("not code") */
            fn pure() { let text = "fs::write and value.read()"; consume(text); }
        "#;
        reject_low_level_io(&lex(benign).expect("lex benign source"))
            .expect("comments and strings are stripped");
    }

    #[test]
    fn unknown_module_declaration_and_path_escape_are_rejected() {
        let unknown = lex("mod escape;").expect("lex unknown module");
        assert!(validate_root_modules(&unknown).is_err());

        let path_escape =
            lex("#[path = \"elsewhere.rs\"] mod fault;").expect("lex path module escape");
        assert!(reject_path_attributes(&path_escape).is_err());
        assert!(validate_root_modules(&path_escape).is_err());
    }

    #[test]
    fn unknown_crate_or_super_helper_indirection_is_rejected() {
        for hostile in ["crate::escape::write()", "super::escape::mutate()"] {
            assert!(
                reject_unknown_helper_paths(&lex(hostile).expect("lex helper escape")).is_err()
            );
        }
    }
}
