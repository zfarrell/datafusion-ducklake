use std::env;
use std::fs;
use std::io::Write;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let slt_dir = Path::new("tests/sqllogictests/sql");

    // Re-run if the test directory changes
    println!("cargo:rerun-if-changed=tests/sqllogictests/sql");

    let mut test_files = Vec::new();
    if slt_dir.exists() {
        find_test_files(slt_dir, &mut test_files);
        test_files.sort();
    }

    let dest_path = Path::new(&out_dir).join("slt_tests.rs");
    let mut f = fs::File::create(&dest_path).unwrap();

    for test_file in &test_files {
        let rel_path = test_file
            .strip_prefix("tests/sqllogictests/sql/")
            .unwrap_or(test_file.as_path());
        let test_name = path_to_test_name(&rel_path.to_string_lossy());

        writeln!(f, "#[tokio::test]").unwrap();
        writeln!(f, "async fn slt_{test_name}() {{").unwrap();
        writeln!(
            f,
            "    run_hybrid_test({:?}).await.unwrap_or_else(|e| {{",
            test_file.to_string_lossy()
        )
        .unwrap();
        writeln!(
            f,
            "        panic!(\"SLT test {{}} failed: {{}}\", {:?}, e);",
            test_file.to_string_lossy()
        )
        .unwrap();
        writeln!(f, "    }});").unwrap();
        writeln!(f, "}}").unwrap();
        writeln!(f).unwrap();
    }
}

fn find_test_files(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                find_test_files(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "test") {
                files.push(path);
            }
        }
    }
}

fn path_to_test_name(rel_path: &str) -> String {
    rel_path
        .trim_end_matches(".test")
        .replace(['/', '\\'], "_")
        .replace('-', "_")
        .replace('.', "_")
        .to_lowercase()
}
