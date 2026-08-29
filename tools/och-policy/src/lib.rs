#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Enforcement for the `OpenControl` Historian workspace dependency boundary.
//!
//! The checker deliberately starts from explicitly configured native workspace
//! members and follows package identities from `cargo metadata`. Dependency
//! aliases are retained only for diagnostics, so renaming a forbidden crate
//! cannot bypass the policy. Tool packages remain visible for ownership
//! validation but are not roots of the native product closure.

use cargo_metadata::{CargoOpt, DependencyKind, Metadata, MetadataCommand, PackageId};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::path::Path;

/// Counts produced by a successful dependency-policy check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolicySummary {
    native_root_count: usize,
    native_closure_package_count: usize,
}

impl PolicySummary {
    /// Returns the number of explicitly configured native roots.
    #[must_use]
    pub const fn native_root_count(self) -> usize {
        self.native_root_count
    }

    /// Returns the number of distinct packages reachable from native roots.
    #[must_use]
    pub const fn native_closure_package_count(self) -> usize {
        self.native_closure_package_count
    }
}

/// A fail-closed metadata or dependency-policy error.
#[derive(Debug, Eq, PartialEq)]
pub struct PolicyError {
    messages: Vec<String>,
}

impl PolicyError {
    fn single(message: impl Into<String>) -> Self {
        Self {
            messages: vec![message.into()],
        }
    }

    fn from_messages(messages: BTreeSet<String>) -> Self {
        Self {
            messages: messages.into_iter().collect(),
        }
    }
}

impl fmt::Display for PolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, message) in self.messages.iter().enumerate() {
            if index > 0 {
                writeln!(formatter)?;
            }
            write!(formatter, "- {message}")?;
        }
        Ok(())
    }
}

impl Error for PolicyError {}

/// Loads Cargo metadata for `manifest_path` and checks the configured boundary.
///
/// All declared features are resolved for policy traversal. This is stricter
/// than the default build and prevents an optional feature from concealing a
/// forbidden native dependency. Only explicitly configured native packages are
/// used as traversal roots.
///
/// # Errors
///
/// Returns an actionable error when Cargo metadata cannot be loaded, role or
/// policy metadata is missing or malformed, defaults violate package roles, or
/// a native dependency path reaches a forbidden, adapter, or tooling package.
/// A forbidden-package exception also fails unless its direct manifest
/// declaration and resolved unified features exactly match policy.
pub fn check_workspace(manifest_path: &Path) -> Result<PolicySummary, PolicyError> {
    let mut command = MetadataCommand::new();
    command.manifest_path(manifest_path);
    command.features(CargoOpt::AllFeatures);
    command.other_options(vec!["--locked".to_owned()]);
    let metadata = command
        .exec()
        .map_err(|error| PolicyError::single(format!("could not load Cargo metadata: {error}")))?;
    check_metadata(&metadata)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    Native,
    Adapter,
    Tooling,
}

impl Role {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Adapter => "adapter",
            Self::Tooling => "tooling",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "native" => Some(Self::Native),
            "adapter" => Some(Self::Adapter),
            "tooling" => Some(Self::Tooling),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
struct PackageNode {
    id: String,
    name: String,
    workspace_member: bool,
    role: Option<Role>,
    unsafe_policy: Option<String>,
    missing_docs_policy: Option<String>,
    dependencies: Vec<DependencyEdge>,
    resolved_features: Vec<String>,
}

#[derive(Clone, Debug)]
struct DependencyEdge {
    package_id: String,
    dependency_name: String,
    declarations: Vec<DependencyDeclaration>,
}

#[derive(Clone, Debug)]
struct DependencyDeclaration {
    kind: ManifestDependencyKind,
    optional: bool,
    unconditional: bool,
    uses_default_features: bool,
    features: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ManifestDependencyKind {
    Normal,
    Other,
}

#[derive(Debug)]
struct PolicyConfig {
    native: BTreeSet<String>,
    adapters: BTreeSet<String>,
    tools: BTreeSet<String>,
    dependency_free_native: BTreeSet<String>,
    forbidden_exceptions: BTreeSet<ForbiddenDependencyException>,
    forbidden: BTreeSet<String>,
    forbidden_prefixes: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ForbiddenDependencyException {
    source: String,
    target: String,
    default_features: bool,
    features: BTreeSet<String>,
}

#[derive(Debug)]
struct PackageGraph {
    packages: BTreeMap<String, PackageNode>,
    default_members: BTreeSet<String>,
    config: PolicyConfig,
}

fn check_metadata(metadata: &Metadata) -> Result<PolicySummary, PolicyError> {
    let config = parse_policy_config(&metadata.workspace_metadata)?;
    let workspace_members: BTreeSet<String> = metadata
        .workspace_members
        .iter()
        .map(ToString::to_string)
        .collect();
    let default_members = metadata
        .workspace_default_members
        .iter()
        .map(ToString::to_string)
        .collect();
    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or_else(|| PolicyError::single("Cargo metadata did not contain a resolve graph"))?;
    let resolved_nodes: BTreeMap<&PackageId, _> =
        resolve.nodes.iter().map(|node| (&node.id, node)).collect();
    let metadata_packages: BTreeMap<&PackageId, _> = metadata
        .packages
        .iter()
        .map(|package| (&package.id, package))
        .collect();

    let mut packages = BTreeMap::new();
    for package in &metadata.packages {
        let id = package.id.to_string();
        let is_workspace_member = workspace_members.contains(&id);
        let (role, unsafe_policy, missing_docs_policy) = if is_workspace_member {
            parse_package_role(&package.metadata).unwrap_or((None, None, None))
        } else {
            (None, None, None)
        };
        let dependencies = resolved_nodes
            .get(&package.id)
            .map_or_else(Vec::new, |node| {
                node.deps
                    .iter()
                    .map(|dependency| {
                        let target_name = metadata_packages
                            .get(&dependency.pkg)
                            .map(|target| target.name.to_string());
                        let declarations = target_name.map_or_else(Vec::new, |target_name| {
                            package
                                .dependencies
                                .iter()
                                .filter(|declaration| {
                                    let alias_matches = declaration.rename.as_ref().map_or_else(
                                        || declaration.name == dependency.name,
                                        |rename| rename == &dependency.name,
                                    );
                                    declaration.name == target_name && alias_matches
                                })
                                .map(|declaration| DependencyDeclaration {
                                    kind: if declaration.kind == DependencyKind::Normal {
                                        ManifestDependencyKind::Normal
                                    } else {
                                        ManifestDependencyKind::Other
                                    },
                                    optional: declaration.optional,
                                    unconditional: declaration.target.is_none(),
                                    uses_default_features: declaration.uses_default_features,
                                    features: declaration.features.clone(),
                                })
                                .collect()
                        });
                        DependencyEdge {
                            package_id: dependency.pkg.to_string(),
                            dependency_name: dependency.name.clone(),
                            declarations,
                        }
                    })
                    .collect()
            });
        let resolved_features = resolved_nodes
            .get(&package.id)
            .map_or_else(Vec::new, |node| {
                node.features.iter().map(ToString::to_string).collect()
            });
        packages.insert(
            id.clone(),
            PackageNode {
                id,
                name: package.name.to_string(),
                workspace_member: is_workspace_member,
                role,
                unsafe_policy,
                missing_docs_policy,
                dependencies,
                resolved_features,
            },
        );
    }

    check_graph(&PackageGraph {
        packages,
        default_members,
        config,
    })
}

fn parse_policy_config(metadata: &Value) -> Result<PolicyConfig, PolicyError> {
    let object = metadata
        .get("och-policy")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            PolicyError::single(
                "workspace metadata is missing table `workspace.metadata.och-policy`",
            )
        })?;
    if object.get("schema").and_then(Value::as_u64) != Some(2) {
        return Err(PolicyError::single(
            "workspace dependency policy must declare integer `schema = 2`",
        ));
    }
    Ok(PolicyConfig {
        native: required_string_set(object, "native-packages")?,
        adapters: required_string_set(object, "adapter-packages")?,
        tools: required_string_set(object, "tool-packages")?,
        dependency_free_native: required_string_set(object, "dependency-free-native-packages")?,
        forbidden_exceptions: required_exception_set(object, "forbidden-dependency-exceptions")?,
        forbidden: required_string_set(object, "forbidden-packages")?,
        forbidden_prefixes: required_string_set(object, "forbidden-package-prefixes")?,
    })
}

fn required_string_set(
    object: &Map<String, Value>,
    key: &str,
) -> Result<BTreeSet<String>, PolicyError> {
    let values = object.get(key).and_then(Value::as_array).ok_or_else(|| {
        PolicyError::single(format!(
            "workspace dependency policy field `{key}` must be an array of strings"
        ))
    })?;
    let mut result = BTreeSet::new();
    for value in values {
        let value = value.as_str().ok_or_else(|| {
            PolicyError::single(format!(
                "workspace dependency policy field `{key}` must contain only strings"
            ))
        })?;
        if value.is_empty() || !result.insert(value.to_owned()) {
            return Err(PolicyError::single(format!(
                "workspace dependency policy field `{key}` contains an empty or duplicate value"
            )));
        }
    }
    Ok(result)
}

fn required_exception_set(
    object: &Map<String, Value>,
    key: &str,
) -> Result<BTreeSet<ForbiddenDependencyException>, PolicyError> {
    let values = object.get(key).and_then(Value::as_array).ok_or_else(|| {
        PolicyError::single(format!(
            "workspace dependency policy field `{key}` must be an array of exception tables"
        ))
    })?;
    let mut result = BTreeSet::new();
    for value in values {
        let exception = value.as_object().ok_or_else(|| {
            PolicyError::single(format!(
                "workspace dependency policy field `{key}` must contain only exception tables"
            ))
        })?;
        if exception.len() != 4
            || !exception.contains_key("source")
            || !exception.contains_key("target")
            || !exception.contains_key("default-features")
            || !exception.contains_key("features")
        {
            return Err(PolicyError::single(format!(
                "workspace dependency policy field `{key}` entries must contain exactly `source`, `target`, `default-features`, and `features`"
            )));
        }
        let source = exception
            .get("source")
            .and_then(Value::as_str)
            .filter(|source| !source.is_empty())
            .ok_or_else(|| {
                PolicyError::single(format!(
                    "workspace dependency policy field `{key}` entry has an invalid `source`"
                ))
            })?;
        let target = exception
            .get("target")
            .and_then(Value::as_str)
            .filter(|target| !target.is_empty())
            .ok_or_else(|| {
                PolicyError::single(format!(
                    "workspace dependency policy field `{key}` entry has an invalid `target`"
                ))
            })?;
        let default_features = exception
            .get("default-features")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                PolicyError::single(format!(
                    "workspace dependency policy field `{key}` entry has a non-Boolean `default-features`"
                ))
            })?;
        if default_features {
            return Err(PolicyError::single(format!(
                "workspace dependency policy field `{key}` exceptions must set `default-features = false`"
            )));
        }
        let features = required_exception_features(exception, key)?;
        if result
            .iter()
            .any(|existing: &ForbiddenDependencyException| {
                existing.source == source && existing.target == target
            })
        {
            return Err(PolicyError::single(format!(
                "workspace dependency policy field `{key}` contains a duplicate source/target exception"
            )));
        }
        result.insert(ForbiddenDependencyException {
            source: source.to_owned(),
            target: target.to_owned(),
            default_features,
            features,
        });
    }
    Ok(result)
}

fn required_exception_features(
    exception: &Map<String, Value>,
    key: &str,
) -> Result<BTreeSet<String>, PolicyError> {
    let values = exception
        .get("features")
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
        .ok_or_else(|| {
            PolicyError::single(format!(
                "workspace dependency policy field `{key}` exception `features` must be a non-empty array of strings"
            ))
        })?;
    let mut features = BTreeSet::new();
    for value in values {
        let feature = value.as_str().filter(|feature| !feature.is_empty()).ok_or_else(|| {
            PolicyError::single(format!(
                "workspace dependency policy field `{key}` exception `features` must contain non-empty strings"
            ))
        })?;
        if !features.insert(feature.to_owned()) {
            return Err(PolicyError::single(format!(
                "workspace dependency policy field `{key}` exception `features` contains a duplicate `{feature}`"
            )));
        }
    }
    Ok(features)
}

fn parse_package_role(metadata: &Value) -> Option<(Option<Role>, Option<String>, Option<String>)> {
    let object = metadata.get("och")?.as_object()?;
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .and_then(Role::parse);
    let unsafe_policy = object
        .get("unsafe-policy")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let missing_docs_policy = object
        .get("missing-docs-policy")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    Some((role, unsafe_policy, missing_docs_policy))
}

fn check_graph(graph: &PackageGraph) -> Result<PolicySummary, PolicyError> {
    let mut violations = BTreeSet::new();
    let workspace_by_name: BTreeMap<&str, &PackageNode> = graph
        .packages
        .values()
        .filter(|package| package.workspace_member)
        .map(|package| (package.name.as_str(), package))
        .collect();

    validate_ownership(graph, &workspace_by_name, &mut violations);
    validate_dependency_laws(graph, &workspace_by_name, &mut violations);

    let mut closure = BTreeSet::new();
    let mut used_exceptions = BTreeSet::new();
    for native_name in &graph.config.native {
        if let Some(root) = workspace_by_name.get(native_name.as_str()) {
            walk_native_closure(
                graph,
                root,
                &mut closure,
                &mut used_exceptions,
                &mut violations,
            );
        }
    }
    for exception in graph
        .config
        .forbidden_exceptions
        .difference(&used_exceptions)
    {
        violations.insert(format!(
            "forbidden dependency exception `{} -> {}` is unused",
            exception.source, exception.target
        ));
    }

    if violations.is_empty() {
        Ok(PolicySummary {
            native_root_count: graph.config.native.len(),
            native_closure_package_count: closure.len(),
        })
    } else {
        Err(PolicyError::from_messages(violations))
    }
}

fn validate_dependency_laws(
    graph: &PackageGraph,
    workspace_by_name: &BTreeMap<&str, &PackageNode>,
    violations: &mut BTreeSet<String>,
) {
    if graph.config.dependency_free_native.is_empty() {
        violations.insert(
            "policy must explicitly name at least one dependency-free native package".to_owned(),
        );
    }
    for name in &graph.config.dependency_free_native {
        if !graph.config.native.contains(name) {
            violations.insert(format!(
                "dependency-free package `{name}` is not configured as native"
            ));
            continue;
        }
        if let Some(package) = workspace_by_name.get(name.as_str())
            && !package.dependencies.is_empty()
        {
            violations.insert(format!(
                "dependency-free native package `{name}` has resolved dependencies"
            ));
        }
    }

    let mut exception_pairs = BTreeSet::new();
    for exception in &graph.config.forbidden_exceptions {
        if !exception_pairs.insert((exception.source.as_str(), exception.target.as_str())) {
            violations.insert(format!(
                "forbidden dependency exception `{} -> {}` is duplicated",
                exception.source, exception.target
            ));
        }
        if !graph.config.native.contains(&exception.source) {
            violations.insert(format!(
                "forbidden dependency exception source `{}` is not a native package",
                exception.source
            ));
        }
        if !is_forbidden(&exception.target, &graph.config) {
            violations.insert(format!(
                "forbidden dependency exception target `{}` is not forbidden",
                exception.target
            ));
        }
        if exception.default_features {
            violations.insert(format!(
                "forbidden dependency exception `{} -> {}` must disable default features",
                exception.source, exception.target
            ));
        }
        if exception.features.is_empty() {
            violations.insert(format!(
                "forbidden dependency exception `{} -> {}` must name at least one allowed feature",
                exception.source, exception.target
            ));
        }
    }
}

fn validate_ownership(
    graph: &PackageGraph,
    workspace_by_name: &BTreeMap<&str, &PackageNode>,
    violations: &mut BTreeSet<String>,
) {
    let configured_roles = [
        (&graph.config.native, Role::Native),
        (&graph.config.adapters, Role::Adapter),
        (&graph.config.tools, Role::Tooling),
    ];
    let mut configured_names = BTreeMap::<&str, Role>::new();
    for (names, role) in configured_roles {
        for name in names {
            if let Some(previous) = configured_names.insert(name, role) {
                violations.insert(format!(
                    "workspace package `{name}` is assigned to both `{}` and `{}` policy roles",
                    previous.as_str(),
                    role.as_str()
                ));
            }
            if !workspace_by_name.contains_key(name.as_str()) {
                violations.insert(format!(
                    "policy role `{}` names unknown workspace package `{name}`",
                    role.as_str()
                ));
            }
        }
    }

    for package in workspace_by_name.values() {
        let configured_role = configured_names.get(package.name.as_str()).copied();
        let Some(declared_role) = package.role else {
            violations.insert(format!(
                "workspace package `{}` is missing a valid `package.metadata.och.role`",
                package.name
            ));
            continue;
        };
        match configured_role {
            None => {
                violations.insert(format!(
                    "workspace package `{}` has role `{}` but is absent from workspace role ownership",
                    package.name,
                    declared_role.as_str()
                ));
            }
            Some(expected) if expected != declared_role => {
                violations.insert(format!(
                    "workspace package `{}` declares role `{}` but policy ownership says `{}`",
                    package.name,
                    declared_role.as_str(),
                    expected.as_str()
                ));
            }
            Some(_) => {}
        }

        if matches!(declared_role, Role::Native | Role::Adapter) {
            if package.unsafe_policy.as_deref() != Some("forbid") {
                violations.insert(format!(
                    "product package `{}` must declare `package.metadata.och.unsafe-policy = \"forbid\"`",
                    package.name
                ));
            }
            if package.missing_docs_policy.as_deref() != Some("deny") {
                violations.insert(format!(
                    "product package `{}` must declare `package.metadata.och.missing-docs-policy = \"deny\"`",
                    package.name
                ));
            }
        }

        let is_default = graph.default_members.contains(&package.id);
        if is_default && declared_role != Role::Native {
            violations.insert(format!(
                "default workspace member `{}` has role `{}`; adapters and tooling cannot be implicit defaults",
                package.name,
                declared_role.as_str()
            ));
        }
        if declared_role == Role::Native && !is_default {
            violations.insert(format!(
                "native package `{}` is not selected as a default workspace member",
                package.name
            ));
        }
    }
}

fn walk_native_closure(
    graph: &PackageGraph,
    root: &PackageNode,
    closure: &mut BTreeSet<String>,
    used_exceptions: &mut BTreeSet<ForbiddenDependencyException>,
    violations: &mut BTreeSet<String>,
) {
    let mut visited = BTreeSet::from([root.id.clone()]);
    let mut queue = VecDeque::from([(root.id.clone(), vec![root.name.clone()])]);
    closure.insert(root.id.clone());

    while let Some((package_id, path)) = queue.pop_front() {
        let Some(package) = graph.packages.get(&package_id) else {
            violations.insert(format!(
                "Cargo resolve graph references unknown package id `{package_id}`"
            ));
            continue;
        };
        let mut dependencies = package.dependencies.clone();
        dependencies.sort_by(|left, right| {
            left.dependency_name
                .cmp(&right.dependency_name)
                .then_with(|| left.package_id.cmp(&right.package_id))
        });
        for dependency in dependencies {
            let Some(target) = graph.packages.get(&dependency.package_id) else {
                violations.insert(format!(
                    "dependency `{}` from `{}` references unknown package id `{}`",
                    dependency.dependency_name, package.name, dependency.package_id
                ));
                continue;
            };
            let path_name = if dependency.dependency_name == target.name {
                target.name.clone()
            } else {
                format!("{} (package {})", dependency.dependency_name, target.name)
            };
            let mut dependency_path = path.clone();
            dependency_path.push(path_name);
            let rendered_path = dependency_path.join(" -> ");

            if target.workspace_member && matches!(target.role, Some(Role::Adapter | Role::Tooling))
            {
                violations.insert(format!(
                    "native dependency direction reaches workspace {} package `{}`: {rendered_path}",
                    target.role.map_or("unowned", Role::as_str),
                    target.name
                ));
            }
            if is_forbidden(&target.name, &graph.config) {
                let configured_exception =
                    graph.config.forbidden_exceptions.iter().find(|exception| {
                        exception.source == root.name && exception.target == target.name
                    });
                let is_direct_from_exception_source = path.len() == 1 && package.id == root.id;
                if let Some(exception) = configured_exception
                    && is_direct_from_exception_source
                {
                    if validate_forbidden_exception_edge(exception, &dependency, target, violations)
                    {
                        used_exceptions.insert(exception.clone());
                    }
                } else {
                    violations.insert(format!(
                        "forbidden package identity `{}` is in the native closure: {rendered_path}",
                        target.name
                    ));
                }
            }

            closure.insert(target.id.clone());
            if visited.insert(target.id.clone()) {
                queue.push_back((target.id.clone(), dependency_path));
            }
        }
    }
}

fn validate_forbidden_exception_edge(
    exception: &ForbiddenDependencyException,
    dependency: &DependencyEdge,
    target: &PackageNode,
    violations: &mut BTreeSet<String>,
) -> bool {
    let edge = format!("{} -> {}", exception.source, exception.target);
    let mut valid = !exception.default_features && !exception.features.is_empty();
    if dependency.declarations.len() == 1 {
        let declaration = &dependency.declarations[0];
        if declaration.kind != ManifestDependencyKind::Normal {
            violations.insert(format!(
                "forbidden dependency exception `{edge}` requires a normal manifest dependency"
            ));
            valid = false;
        }
        if declaration.optional {
            violations.insert(format!(
                "forbidden dependency exception `{edge}` requires a non-optional manifest dependency"
            ));
            valid = false;
        }
        if !declaration.unconditional {
            violations.insert(format!(
                "forbidden dependency exception `{edge}` requires an unconditional manifest dependency"
            ));
            valid = false;
        }
        if declaration.uses_default_features != exception.default_features {
            violations.insert(format!(
                "forbidden dependency exception `{edge}` requires manifest `default-features = false`"
            ));
            valid = false;
        }
        match checked_feature_set(&declaration.features) {
            None => {
                violations.insert(format!(
                    "forbidden dependency exception `{edge}` manifest declaration contains an empty or duplicate feature"
                ));
                valid = false;
            }
            Some(features) if features != exception.features => {
                violations.insert(format!(
                    "forbidden dependency exception `{edge}` manifest features are {}; expected exactly {}",
                    render_features(&features),
                    render_features(&exception.features)
                ));
                valid = false;
            }
            Some(_) => {}
        }
    } else {
        violations.insert(format!(
            "forbidden dependency exception `{edge}` requires exactly one matching direct manifest declaration; found {}",
            dependency.declarations.len()
        ));
        valid = false;
    }

    match checked_feature_set(&target.resolved_features) {
        None => {
            violations.insert(format!(
                "resolved package identity `{}` reports an empty or duplicate enabled feature",
                target.name
            ));
            valid = false;
        }
        Some(features) if features != exception.features => {
            violations.insert(format!(
                "resolved package identity `{}` enables features {}; exception `{edge}` permits exactly {}",
                target.name,
                render_features(&features),
                render_features(&exception.features)
            ));
            valid = false;
        }
        Some(_) => {}
    }
    valid
}

fn checked_feature_set(features: &[String]) -> Option<BTreeSet<String>> {
    let mut result = BTreeSet::new();
    for feature in features {
        if feature.is_empty() || !result.insert(feature.clone()) {
            return None;
        }
    }
    Some(result)
}

fn render_features(features: &BTreeSet<String>) -> String {
    format!(
        "[{}]",
        features
            .iter()
            .map(|feature| format!("`{feature}`"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn is_forbidden(package_name: &str, config: &PolicyConfig) -> bool {
    config.forbidden.contains(package_name)
        || config
            .forbidden_prefixes
            .iter()
            .any(|prefix| package_name.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::{
        DependencyDeclaration, DependencyEdge, ForbiddenDependencyException,
        ManifestDependencyKind, PackageGraph, PackageNode, PolicyConfig, Role, check_graph,
        parse_policy_config,
    };
    use serde::Deserialize;
    use serde_json::{Value, json};
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Debug, Deserialize)]
    struct FixtureCase {
        name: String,
        policy: FixturePolicy,
        default_members: Vec<String>,
        packages: Vec<FixturePackage>,
        expected: FixtureExpected,
    }

    #[derive(Debug, Deserialize)]
    struct FixturePolicy {
        native: Vec<String>,
        #[serde(default)]
        adapters: Vec<String>,
        #[serde(default)]
        tools: Vec<String>,
        #[serde(default)]
        dependency_free_native: Vec<String>,
        #[serde(default)]
        forbidden_exceptions: Vec<FixtureException>,
        #[serde(default)]
        forbidden: Vec<String>,
        #[serde(default)]
        forbidden_prefixes: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    struct FixtureException {
        source: String,
        target: String,
        default_features: bool,
        features: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    struct FixturePackage {
        id: String,
        name: String,
        #[serde(default)]
        external: bool,
        role: Option<String>,
        unsafe_policy: Option<String>,
        missing_docs_policy: Option<String>,
        #[serde(default)]
        dependencies: Vec<FixtureDependency>,
        #[serde(default)]
        resolved_features: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    struct FixtureDependency {
        package_id: String,
        name: String,
        #[serde(default)]
        declarations: Vec<FixtureDeclaration>,
    }

    #[derive(Debug, Deserialize)]
    struct FixtureDeclaration {
        kind: String,
        optional: bool,
        unconditional: bool,
        uses_default_features: bool,
        features: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    struct FixtureExpected {
        pass: bool,
        message: Option<String>,
        closure_packages: Option<usize>,
    }

    #[test]
    fn policy_fixtures_cover_boundary_failures_and_cycle_termination() {
        let cases: Vec<FixtureCase> =
            serde_json::from_str(include_str!("../tests/fixtures/policy-cases.json"))
                .expect("fixture JSON should parse");
        assert!(
            cases.len() >= 28,
            "the policy fixture suite must remain broad"
        );

        for case in cases {
            let graph = fixture_graph(&case);
            let result = check_graph(&graph);
            if case.expected.pass {
                let summary = result.unwrap_or_else(|error| {
                    panic!("fixture `{}` unexpectedly failed: {error}", case.name)
                });
                assert_eq!(
                    Some(summary.native_closure_package_count()),
                    case.expected.closure_packages,
                    "fixture `{}` closure count",
                    case.name
                );
            } else {
                let error = result.unwrap_err();
                let expected = case
                    .expected
                    .message
                    .as_deref()
                    .expect("failing fixture needs an expected message");
                assert!(
                    error.to_string().contains(expected),
                    "fixture `{}` error `{error}` did not contain `{expected}`",
                    case.name
                );
            }
        }
    }

    fn fixture_graph(case: &FixtureCase) -> PackageGraph {
        let packages = case
            .packages
            .iter()
            .map(|package| {
                let node = PackageNode {
                    id: package.id.clone(),
                    name: package.name.clone(),
                    workspace_member: !package.external,
                    role: package.role.as_deref().and_then(Role::parse),
                    unsafe_policy: package.unsafe_policy.clone(),
                    missing_docs_policy: package.missing_docs_policy.clone(),
                    dependencies: package
                        .dependencies
                        .iter()
                        .map(|dependency| DependencyEdge {
                            package_id: dependency.package_id.clone(),
                            dependency_name: dependency.name.clone(),
                            declarations: dependency
                                .declarations
                                .iter()
                                .map(|declaration| DependencyDeclaration {
                                    kind: if declaration.kind == "normal" {
                                        ManifestDependencyKind::Normal
                                    } else {
                                        ManifestDependencyKind::Other
                                    },
                                    optional: declaration.optional,
                                    unconditional: declaration.unconditional,
                                    uses_default_features: declaration.uses_default_features,
                                    features: declaration.features.clone(),
                                })
                                .collect(),
                        })
                        .collect(),
                    resolved_features: package.resolved_features.clone(),
                };
                (node.id.clone(), node)
            })
            .collect::<BTreeMap<_, _>>();
        let ids_by_name = packages
            .values()
            .map(|package| (package.name.as_str(), package.id.as_str()))
            .collect::<BTreeMap<_, _>>();
        PackageGraph {
            default_members: case
                .default_members
                .iter()
                .map(|name| {
                    ids_by_name
                        .get(name.as_str())
                        .unwrap_or_else(|| panic!("fixture default `{name}` must exist"))
                        .to_string()
                })
                .collect(),
            packages,
            config: PolicyConfig {
                native: case.policy.native.iter().cloned().collect(),
                adapters: case.policy.adapters.iter().cloned().collect(),
                tools: case.policy.tools.iter().cloned().collect(),
                dependency_free_native: case
                    .policy
                    .dependency_free_native
                    .iter()
                    .cloned()
                    .collect(),
                forbidden_exceptions: case
                    .policy
                    .forbidden_exceptions
                    .iter()
                    .map(|exception| ForbiddenDependencyException {
                        source: exception.source.clone(),
                        target: exception.target.clone(),
                        default_features: exception.default_features,
                        features: exception.features.iter().cloned().collect(),
                    })
                    .collect(),
                forbidden: case.policy.forbidden.iter().cloned().collect(),
                forbidden_prefixes: case.policy.forbidden_prefixes.iter().cloned().collect(),
            },
        }
    }

    #[test]
    fn duplicate_policy_ownership_is_rejected() {
        let package = PackageNode {
            id: "native-id".to_owned(),
            name: "native-root".to_owned(),
            workspace_member: true,
            role: Some(Role::Native),
            unsafe_policy: Some("forbid".to_owned()),
            missing_docs_policy: Some("deny".to_owned()),
            dependencies: Vec::new(),
            resolved_features: Vec::new(),
        };
        let graph = PackageGraph {
            packages: BTreeMap::from([(package.id.clone(), package)]),
            default_members: BTreeSet::from(["native-id".to_owned()]),
            config: PolicyConfig {
                native: BTreeSet::from(["native-root".to_owned()]),
                adapters: BTreeSet::from(["native-root".to_owned()]),
                tools: BTreeSet::new(),
                dependency_free_native: BTreeSet::from(["native-root".to_owned()]),
                forbidden_exceptions: BTreeSet::new(),
                forbidden: BTreeSet::new(),
                forbidden_prefixes: BTreeSet::new(),
            },
        };

        let error = check_graph(&graph).expect_err("duplicate ownership must fail");
        assert!(error.to_string().contains("assigned to both"));
    }

    #[test]
    fn policy_schema_and_exception_shape_fail_closed() {
        let wrong_schema = json!({
            "och-policy": {
                "schema": 1,
                "native-packages": ["core"],
                "adapter-packages": [],
                "tool-packages": [],
                "dependency-free-native-packages": ["core"],
                "forbidden-dependency-exceptions": [],
                "forbidden-packages": ["tokio"],
                "forbidden-package-prefixes": []
            }
        });
        assert!(
            parse_policy_config(&wrong_schema)
                .expect_err("old schemas must fail")
                .to_string()
                .contains("schema = 2")
        );

        let malformed_exception = json!({
            "och-policy": {
                "schema": 2,
                "native-packages": ["core"],
                "adapter-packages": [],
                "tool-packages": [],
                "dependency-free-native-packages": ["core"],
                "forbidden-dependency-exceptions": [
                    {
                        "source": "core",
                        "target": "tokio",
                        "default-features": false,
                        "features": ["rt", "sync"],
                        "reason": "too broad"
                    }
                ],
                "forbidden-packages": ["tokio"],
                "forbidden-package-prefixes": []
            }
        });
        assert!(
            parse_policy_config(&malformed_exception)
                .expect_err("extra exception fields must fail")
                .to_string()
                .contains("exactly `source`, `target`, `default-features`, and `features`")
        );

        let missing_features = policy_metadata_with_exception(&json!({
            "source": "core",
            "target": "tokio",
            "default-features": false
        }));
        assert!(
            parse_policy_config(&missing_features)
                .expect_err("missing exception fields must fail")
                .to_string()
                .contains("exactly `source`, `target`, `default-features`, and `features`")
        );
    }

    #[test]
    fn duplicate_policy_values_and_exceptions_fail_closed() {
        let duplicate_exception = json!({
            "och-policy": {
                "schema": 2,
                "native-packages": ["core"],
                "adapter-packages": [],
                "tool-packages": [],
                "dependency-free-native-packages": ["core"],
                "forbidden-dependency-exceptions": [
                    { "source": "core", "target": "tokio", "default-features": false, "features": ["rt", "sync"] },
                    { "source": "core", "target": "tokio", "default-features": false, "features": ["rt", "time"] }
                ],
                "forbidden-packages": ["tokio"],
                "forbidden-package-prefixes": []
            }
        });
        assert!(
            parse_policy_config(&duplicate_exception)
                .expect_err("duplicate exceptions must fail")
                .to_string()
                .contains("duplicate source/target exception")
        );

        let duplicate_root = json!({
            "och-policy": {
                "schema": 2,
                "native-packages": ["core"],
                "adapter-packages": [],
                "tool-packages": [],
                "dependency-free-native-packages": ["core", "core"],
                "forbidden-dependency-exceptions": [],
                "forbidden-packages": ["tokio"],
                "forbidden-package-prefixes": []
            }
        });
        assert!(
            parse_policy_config(&duplicate_root)
                .expect_err("duplicate dependency-free roots must fail")
                .to_string()
                .contains("empty or duplicate value")
        );
    }

    #[test]
    fn exception_default_and_feature_configuration_fail_closed() {
        let default_features = policy_metadata_with_exception(&json!({
            "source": "core",
            "target": "tokio",
            "default-features": true,
            "features": ["rt", "sync"]
        }));
        assert!(
            parse_policy_config(&default_features)
                .expect_err("default features must not be configurable for an exception")
                .to_string()
                .contains("must set `default-features = false`")
        );

        let non_boolean_default = policy_metadata_with_exception(&json!({
            "source": "core",
            "target": "tokio",
            "default-features": "false",
            "features": ["rt", "sync"]
        }));
        assert!(
            parse_policy_config(&non_boolean_default)
                .expect_err("non-Boolean default feature policy must fail")
                .to_string()
                .contains("non-Boolean `default-features`")
        );

        for (features, expected) in [
            (json!("rt"), "non-empty array of strings"),
            (json!([]), "non-empty array of strings"),
            (json!(["rt", ""]), "non-empty strings"),
            (json!(["rt", "rt"]), "contains a duplicate `rt`"),
        ] {
            let metadata = policy_metadata_with_exception(&json!({
                "source": "core",
                "target": "tokio",
                "default-features": false,
                "features": features
            }));
            let error = parse_policy_config(&metadata)
                .expect_err("malformed exception features must fail")
                .to_string();
            assert!(
                error.contains(expected),
                "feature error `{error}` did not contain `{expected}`"
            );
        }
    }

    fn policy_metadata_with_exception(exception: &Value) -> Value {
        json!({
            "och-policy": {
                "schema": 2,
                "native-packages": ["core"],
                "adapter-packages": [],
                "tool-packages": [],
                "dependency-free-native-packages": ["core"],
                "forbidden-dependency-exceptions": [exception],
                "forbidden-packages": ["tokio"],
                "forbidden-package-prefixes": []
            }
        })
    }
}
