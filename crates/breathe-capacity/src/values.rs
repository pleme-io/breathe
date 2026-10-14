use crate::model::{Chart, Sizing};
use crate::quantity::{Bytes, Millicores};
use serde::Serialize;
use serde_yaml::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Edit {
    pub service: String,
    pub key: String,
    pub from: String,
    pub to: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValuesError {
    #[error("values file is not YAML: {0}")]
    Yaml(String),
    #[error("{0}.{1} is absent: the workload is not deployed by this values file")]
    ServiceAbsent(String, String),
    #[error("{0}.{1}.{2} = {3:?} is not a valid quantity")]
    Malformed(String, String, String, String),
    #[error("no top-level `{0}:` line to edit under")]
    NoRoot(String),
    #[error("after editing, {0}.{1}.{2} reads {3:?}, not {4:?}")]
    Verify(String, String, String, String, String),
}

fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

pub fn read_sizing(text: &str, chart: &Chart, svc: &str) -> Result<Sizing, ValuesError> {
    let doc: Value = serde_yaml::from_str(text).map_err(|e| ValuesError::Yaml(e.to_string()))?;
    let block = doc
        .get(chart.root.as_str())
        .and_then(|r| r.get(svc))
        .ok_or_else(|| ValuesError::ServiceAbsent(chart.root.clone(), svc.to_string()))?;
    let mut s = chart.defaults;
    let k = &chart.keys;
    let get = |key: &str| block.get(key).and_then(scalar);
    let bad = |key: &str, v: String| ValuesError::Malformed(chart.root.clone(), svc.to_string(), key.to_string(), v);
    if let Some(v) = get(&k.cpu_request) {
        s.cpu_request = Millicores::parse(&v).ok_or_else(|| bad(&k.cpu_request, v))?;
    }
    if let Some(v) = get(&k.mem_request) {
        s.mem_request = Bytes::parse(&v).ok_or_else(|| bad(&k.mem_request, v))?;
    }
    if let Some(v) = get(&k.mem_limit) {
        s.mem_limit = Bytes::parse(&v).ok_or_else(|| bad(&k.mem_limit, v))?;
    }
    for (key, slot) in [
        (&k.hpa_min, &mut s.hpa_min),
        (&k.hpa_max, &mut s.hpa_max),
        (&k.hpa_mem_pct, &mut s.hpa_mem_pct),
        (&k.hpa_cpu_pct, &mut s.hpa_cpu_pct),
    ] {
        if let Some(v) = get(key) {
            *slot = v.trim().parse().map_err(|_| bad(key, v))?;
        }
    }
    Ok(s)
}

fn indent(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

fn is_key_line(line: &str, key: &str) -> bool {
    line.trim_start().strip_prefix(key).is_some_and(|r| r.starts_with(':'))
}

fn is_content(line: &str) -> bool {
    let t = line.trim_start();
    !t.is_empty() && !t.starts_with('#')
}

fn block_end(lines: &[String], start: usize, parent_indent: usize) -> usize {
    let mut end = start + 1;
    let mut last = start;
    while end < lines.len() {
        let l = &lines[end];
        if is_content(l) && indent(l) <= parent_indent {
            break;
        }
        if is_content(l) {
            last = end;
        }
        end += 1;
    }
    last + 1
}

fn child_indent(lines: &[String], start: usize, end: usize, parent_indent: usize) -> usize {
    lines[start + 1..end].iter().find(|l| is_content(l)).map_or(parent_indent + 2, |l| indent(l))
}

fn set_value(line: &str, to: &str) -> String {
    let (head, rest) = line.split_once(':').expect("key line has a colon");
    let comment = rest.find(" #").map_or("", |i| &rest[i..]);
    let value = rest[..rest.len() - comment.len()].trim();
    let quote = value.chars().next().filter(|c| *c == '"' || *c == '\'').map(String::from).unwrap_or_default();
    format!("{head}: {quote}{to}{quote}{comment}")
}

fn apply_one(lines: &mut Vec<String>, root: &str, e: &Edit) -> Result<(), ValuesError> {
    let r = lines
        .iter()
        .position(|l| indent(l) == 0 && is_key_line(l, root))
        .ok_or_else(|| ValuesError::NoRoot(root.to_string()))?;
    let root_end = block_end(lines, r, 0);
    let svc_indent = child_indent(lines, r, root_end, 0);
    let Some(svc_line) = (r + 1..root_end).find(|&i| indent(&lines[i]) == svc_indent && is_key_line(&lines[i], &e.service))
    else {
        return Err(ValuesError::ServiceAbsent(root.to_string(), e.service.clone()));
    };
    let svc_end = block_end(lines, svc_line, svc_indent);
    let key_indent = child_indent(lines, svc_line, svc_end, svc_indent);
    match (svc_line + 1..svc_end).find(|&i| indent(&lines[i]) == key_indent && is_key_line(&lines[i], &e.key)) {
        Some(i) => lines[i] = set_value(&lines[i], &e.to),
        None => lines.insert(svc_end, format!("{}{}: {}", " ".repeat(key_indent), e.key, e.to)),
    }
    Ok(())
}

pub fn apply(text: &str, root: &str, edits: &[Edit]) -> Result<String, ValuesError> {
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    for e in edits {
        apply_one(&mut lines, root, e)?;
    }
    let mut out = lines.join("\n");
    if text.ends_with('\n') {
        out.push('\n');
    }
    let doc: Value = serde_yaml::from_str(&out).map_err(|e| ValuesError::Yaml(e.to_string()))?;
    for e in edits {
        let got = doc
            .get(root)
            .and_then(|r| r.get(e.service.as_str()))
            .and_then(|b| b.get(e.key.as_str()))
            .and_then(scalar)
            .unwrap_or_default();
        if got != e.to {
            return Err(ValuesError::Verify(root.to_string(), e.service.clone(), e.key.clone(), got, e.to.clone()));
        }
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::model::Keys;

    pub(crate) fn chart() -> Chart {
        Chart {
            root: "services".into(),
            keys: Keys {
                cpu_request: "cpu".into(),
                mem_request: "memory".into(),
                mem_limit: "memoryLimit".into(),
                hpa_min: "minReplicas".into(),
                hpa_max: "maxReplicas".into(),
                hpa_mem_pct: "memoryTarget".into(),
                hpa_cpu_pct: "cpuTarget".into(),
            },
            defaults: Sizing {
                cpu_request: Millicores(200),
                mem_request: Bytes(1 << 30),
                mem_limit: Bytes(2 << 30),
                hpa_min: 2,
                hpa_max: 20,
                hpa_mem_pct: 60,
                hpa_cpu_pct: 70,
            },
            pod_label: None,
        }
    }

    const DOC: &str = "\
global:
  x: 1
services:
  api:
    cpu: 3500m
    memory: 2Gi # sized for peak
    memoryLimit: \"4Gi\"

  worker:
    cpu: 700m
other:
  y: 2
";

    fn edit(svc: &str, key: &str, from: &str, to: &str) -> Edit {
        Edit { service: svc.into(), key: key.into(), from: from.into(), to: to.into() }
    }

    #[test]
    fn reads_with_chart_defaults_underneath() {
        let c = chart();
        let s = read_sizing(DOC, &c, "worker").unwrap();
        assert_eq!(s.cpu_request, Millicores(700));
        assert_eq!(s.mem_limit.render(), "2Gi");
        assert_eq!(s.hpa_mem_pct, 60);
        assert_eq!(read_sizing(DOC, &c, "queue"), Err(ValuesError::ServiceAbsent("services".into(), "queue".into())));
    }

    #[test]
    fn edits_in_place_keeping_comments_and_quotes_and_inserts_missing_keys() {
        let edits = [
            edit("api", "memory", "2Gi", "3Gi"),
            edit("api", "memoryLimit", "4Gi", "6Gi"),
            edit("worker", "memoryLimit", "2Gi", "3Gi"),
        ];
        let out = apply(DOC, "services", &edits).unwrap();
        assert!(out.contains("    memory: 3Gi # sized for peak\n"));
        assert!(out.contains("    memoryLimit: \"6Gi\"\n"));
        assert!(out.contains("  worker:\n    cpu: 700m\n    memoryLimit: 3Gi\nother:"));
        assert_eq!(out.lines().count(), DOC.lines().count() + 1);
    }
}
