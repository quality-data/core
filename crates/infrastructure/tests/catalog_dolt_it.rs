//! The catalog on an ephemeral Dolt (decision 0021): an import is one
//! commit on `main`, read back as of that commit; a second import that
//! changes nothing makes no commit; a tenant's database sees only its own
//! catalog; a draft is a branch from `main` that leaves `main` as it was.
//! Synthetic rules only.

use infrastructure::catalog_dolt::{
    is_commit, DatasetImport, DoltCatalog, FileChange, FileEdit, FileSaved, FilesEdit, MappingEdit,
    Merged,
};

const MAPPING: &str =
    "dataset: loans\nsource: loans\nkey: [id]\nfields:\n  id: { column: ID, type: text }\n";

fn rules(check: &str) -> Vec<(String, String)> {
    vec![
        (
            "amounts.yaml".into(),
            format!("dataset: loans\nrules:\n  - id: L-01\n    field: id\n    check: {check}\n"),
        ),
        ("ids.yaml".into(), "dataset: loans\nrules: []\n".into()),
    ]
}

#[tokio::test]
async fn an_import_is_a_commit_read_back_as_of_it() {
    let dolt = testkit::spawn_dolt().await;
    dolt.create_database("tenant_a").await;
    dolt.create_database("tenant_b").await;
    let catalog = DoltCatalog::new(&dolt.url);
    assert_eq!(
        catalog.files("tenant_a", "loans").await.unwrap(),
        None,
        "no catalog yet"
    );
    assert!(catalog.mappings("tenant_a").await.unwrap().is_empty());

    let first_rules = rules("required");
    let import = DatasetImport {
        dataset: "loans",
        description: Some("Synthetic loans"),
        mapping: MAPPING,
        rules: &first_rules,
    };
    let first = catalog
        .import("tenant_a", &import, "Test <test@example.org>")
        .await
        .unwrap();
    let files = catalog.files("tenant_a", "loans").await.unwrap().unwrap();
    assert_eq!(files.commit, first);
    assert_eq!(files.mapping, MAPPING);
    assert_eq!(files.rules, first_rules);
    assert!(files.others.is_empty());

    // The same again: no new commit. A change: a new one, the files replaced.
    assert_eq!(
        catalog
            .import("tenant_a", &import, "Test <test@example.org>")
            .await
            .unwrap(),
        first
    );
    let changed = vec![rules("unique").remove(0)];
    let second = catalog
        .import(
            "tenant_a",
            &DatasetImport {
                rules: &changed,
                ..import.clone()
            },
            "Test <test@example.org>",
        )
        .await
        .unwrap();
    assert_ne!(second, first);
    assert_eq!(
        catalog
            .files("tenant_a", "loans")
            .await
            .unwrap()
            .unwrap()
            .rules,
        changed
    );

    // A run's manifest's commit reads the files as they were then
    // (decision 0090, step 22's backfill); a bad hash is refused.
    let then = catalog
        .files_at_commit("tenant_a", &first, "loans")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (then.commit.as_str(), then.rules),
        (first.as_str(), first_rules.clone())
    );
    assert!(catalog
        .files_at_commit("tenant_a", "x' OR '1", "loans")
        .await
        .is_err());

    // Another dataset's mapping comes with it, for the references.
    let none = Vec::new();
    let advisors = DatasetImport {
        dataset: "advisors",
        description: None,
        mapping: "dataset: advisors\n",
        rules: &none,
    };
    let third = catalog
        .import("tenant_a", &advisors, "Test <test@example.org>")
        .await
        .unwrap();
    let files = catalog.files("tenant_a", "loans").await.unwrap().unwrap();
    assert_eq!(
        (files.commit, files.others),
        (
            third,
            vec![("advisors".to_string(), "dataset: advisors\n".to_string())]
        )
    );

    // Every dataset's mapping on `main`, by dataset.
    assert_eq!(
        catalog.mappings("tenant_a").await.unwrap(),
        vec![
            ("advisors".to_string(), "dataset: advisors\n".to_string()),
            ("loans".to_string(), MAPPING.to_string()),
        ]
    );

    // Another tenant's database has none of it.
    assert_eq!(catalog.files("tenant_b", "loans").await.unwrap(), None);
    assert!(catalog.mappings("tenant_b").await.unwrap().is_empty());
}

#[tokio::test]
async fn a_draft_is_a_branch_from_main_and_leaves_main_as_it_was() {
    let dolt = testkit::spawn_dolt().await;
    dolt.create_database("tenant_a").await;
    let catalog = DoltCatalog::new(&dolt.url);
    let proposed = rules("required");
    let import = DatasetImport {
        dataset: "loans",
        description: Some("Synthetic loans"),
        mapping: MAPPING,
        rules: &proposed,
    };
    let branch = "proposal/loans/00000000-0000-0000-0000-000000000001";

    // On a catalog with nothing yet: the draft has it all, main nothing.
    let draft = catalog
        .draft("tenant_a", branch, &import, "Map job <map@example.org>")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog.files("tenant_a", "loans").await.unwrap(), None);
    assert!(catalog.mappings("tenant_a").await.unwrap().is_empty());
    let db = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{}/tenant_a", dolt.url))
        .await
        .unwrap();
    let on_draft: String = sqlx::query_scalar(&format!(
        "SELECT body FROM mappings AS OF '{draft}' WHERE dataset = 'loans'"
    ))
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(on_draft, MAPPING);
    let rules_on_draft: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM rules AS OF '{draft}'"))
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(rules_on_draft, proposed.len() as i64);

    // A draft is never written over: a second one finds it made.
    assert_eq!(
        catalog
            .draft("tenant_a", branch, &import, "Map job <map@example.org>")
            .await
            .unwrap(),
        None
    );

    // After an import, another draft still leaves main where it was.
    let main = catalog
        .import("tenant_a", &import, "Test <test@example.org>")
        .await
        .unwrap();
    let changed = vec![rules("unique").remove(0)];
    let other = DatasetImport {
        rules: &changed,
        ..import
    };
    catalog
        .draft(
            "tenant_a",
            "proposal/loans/2",
            &other,
            "Map job <map@example.org>",
        )
        .await
        .unwrap();
    let files = catalog.files("tenant_a", "loans").await.unwrap().unwrap();
    assert_eq!((files.commit, files.rules), (main, proposed));
}

/// `main` with loans, and two drafts of it made from that same `main`:
/// `proposal/loans/1` changes its rules, `proposal/loans/2` its mapping.
async fn two_drafts(catalog: &DoltCatalog) -> Vec<(String, String)> {
    let base = rules("required");
    let import = DatasetImport {
        dataset: "loans",
        description: None,
        mapping: MAPPING,
        rules: &base,
    };
    catalog
        .import("tenant_a", &import, "Test <test@example.org>")
        .await
        .unwrap();
    let one = vec![rules("unique").remove(0)];
    let two = vec![
        rules("required").remove(0),
        ("other.yaml".into(), "dataset: loans\nrules: []\n".into()),
    ];
    let mapping_two = MAPPING.replace("type: text", "type: number");
    for (branch, rules, mapping) in [
        ("proposal/loans/1", &one, MAPPING.to_string()),
        ("proposal/loans/2", &two, mapping_two),
    ] {
        let draft = DatasetImport {
            dataset: "loans",
            description: None,
            mapping: &mapping,
            rules,
        };
        catalog
            .draft("tenant_a", branch, &draft, "Map job <map@example.org>")
            .await
            .unwrap();
    }
    one
}

#[tokio::test]
async fn drafts_are_listed_by_prefix_and_read_as_their_branch_has_them() {
    let dolt = testkit::spawn_dolt().await;
    dolt.create_database("tenant_a").await;
    let catalog = DoltCatalog::new(&dolt.url);
    let one = two_drafts(&catalog).await;
    let drafts = catalog.drafts("tenant_a", "proposal/").await.unwrap();
    assert_eq!(
        drafts.iter().map(|d| d.branch.as_str()).collect::<Vec<_>>(),
        ["proposal/loans/1", "proposal/loans/2"]
    );
    assert!(drafts[0].author.contains("map@example.org"));
    let on_branch = catalog
        .files_at("tenant_a", "proposal/loans/1", "loans")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(on_branch.rules, one);
    assert_eq!(
        catalog
            .files_at("tenant_a", "proposal/none", "loans")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn a_draft_is_approved_into_main_once_and_a_stale_one_is_not() {
    let dolt = testkit::spawn_dolt().await;
    dolt.create_database("tenant_a").await;
    let catalog = DoltCatalog::new(&dolt.url);
    let one = two_drafts(&catalog).await;
    let approve = |branch: &'static str| {
        let catalog = &catalog;
        async move {
            catalog
                .approve(
                    "tenant_a",
                    branch,
                    "loans",
                    "Approver <ok@example.org>",
                    "approve loans",
                )
                .await
                .unwrap()
        }
    };
    // The first approved: main has it; the branch is gone.
    let Merged::Merged(commit) = approve("proposal/loans/1").await else {
        panic!("not merged");
    };
    let main = catalog.files("tenant_a", "loans").await.unwrap().unwrap();
    assert_eq!((main.commit, main.rules), (commit.clone(), one));
    assert_eq!(
        catalog.drafts("tenant_a", "proposal/").await.unwrap().len(),
        1
    );

    // The second was made from the main before: main changed the dataset
    // since, so it is not merged cell by cell into one nobody reviewed.
    assert_eq!(approve("proposal/loans/2").await, Merged::Stale);
    assert_eq!(
        catalog
            .files("tenant_a", "loans")
            .await
            .unwrap()
            .unwrap()
            .commit,
        commit
    );

    // Discarded, it is gone; a draft not there is neither approved nor discarded.
    assert!(catalog
        .discard("tenant_a", "proposal/loans/2")
        .await
        .unwrap());
    assert!(!catalog
        .discard("tenant_a", "proposal/loans/2")
        .await
        .unwrap());
    assert_eq!(approve("proposal/loans/2").await, Merged::NotFound);
    assert!(catalog
        .drafts("tenant_a", "proposal/")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn an_edit_is_saved_on_its_draft_one_file_and_one_commit_a_save() {
    let dolt = testkit::spawn_dolt().await;
    dolt.create_database("tenant_a").await;
    let catalog = DoltCatalog::new(&dolt.url);
    let on_main = rules("required");
    let import = DatasetImport {
        dataset: "loans",
        description: Some("Synthetic loans"),
        mapping: MAPPING,
        rules: &on_main,
    };
    let main = catalog
        .import("tenant_a", &import, "Test <test@example.org>")
        .await
        .unwrap();
    let body = "dataset: loans\nrules:\n  - id: L-01\n    field: id\n    check: unique\n";
    let edit = FileEdit {
        branch: "proposal/loans/e1",
        dataset: "loans",
        file: "amounts.yaml",
        body,
        author: "Ana <ana@example.org>",
        adds: false,
    };

    // Not made unless asked; then made from main with that file changed.
    assert_eq!(
        catalog.save_file("tenant_a", &edit, false).await.unwrap(),
        FileSaved::NoDraft
    );
    let FileSaved::Saved(first) = catalog.save_file("tenant_a", &edit, true).await.unwrap() else {
        panic!("saved");
    };
    let draft = catalog
        .files_at("tenant_a", "proposal/loans/e1", "loans")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(draft.commit, first);
    assert_eq!(draft.mapping, MAPPING, "the rest as main has it");
    assert_eq!(
        draft.rules,
        vec![
            ("amounts.yaml".to_string(), body.to_string()),
            on_main[1].clone()
        ]
    );
    let drafts = catalog.drafts("tenant_a", "proposal/").await.unwrap();
    assert_eq!(drafts[0].author, "Ana <ana@example.org>");

    // The same text again: no commit; another text: one more.
    assert_eq!(
        catalog.save_file("tenant_a", &edit, false).await.unwrap(),
        FileSaved::Saved(first.clone())
    );
    let again = FileEdit {
        body: "dataset: loans\nrules: []\n",
        ..edit
    };
    let FileSaved::Saved(second) = catalog.save_file("tenant_a", &again, false).await.unwrap()
    else {
        panic!("saved");
    };
    assert_ne!(second, first);

    // A file the dataset does not have: nothing, and no draft left behind.
    let none = FileEdit {
        branch: "proposal/loans/e2",
        file: "nope.yaml",
        ..edit
    };
    assert_eq!(
        catalog.save_file("tenant_a", &none, true).await.unwrap(),
        FileSaved::NoFile
    );
    assert_eq!(
        catalog.drafts("tenant_a", "proposal/").await.unwrap().len(),
        1
    );

    // Main as it was; the draft approves into it.
    let files = catalog.files("tenant_a", "loans").await.unwrap().unwrap();
    assert_eq!((files.commit, files.rules), (main, on_main));
    assert!(matches!(
        catalog
            .approve(
                "tenant_a",
                "proposal/loans/e1",
                "loans",
                "Bo <bo@example.org>",
                "approve e1"
            )
            .await
            .unwrap(),
        Merged::Merged(_)
    ));
}

#[tokio::test]
async fn a_draft_adds_a_file_only_when_told_it_may() {
    let dolt = testkit::spawn_dolt().await;
    dolt.create_database("tenant_a").await;
    let catalog = DoltCatalog::new(&dolt.url);
    let on_main = rules("required");
    let import = DatasetImport {
        dataset: "loans",
        description: None,
        mapping: MAPPING,
        rules: &on_main,
    };
    catalog
        .import("tenant_a", &import, "Test <test@example.org>")
        .await
        .unwrap();
    let body = "dataset: loans\nrules:\n  - id: L-01\n    field: id\n    check: unique\n";
    // A field's own file (decision 0044): added on the draft, not on main.
    let added = FileEdit {
        branch: "proposal/loans/a1",
        dataset: "loans",
        file: "id.yaml",
        body,
        author: "Ana <ana@example.org>",
        adds: true,
    };
    let FileSaved::Saved(_) = catalog.save_file("tenant_a", &added, true).await.unwrap() else {
        panic!("added");
    };
    let draft = catalog
        .files_at("tenant_a", "proposal/loans/a1", "loans")
        .await
        .unwrap()
        .unwrap();
    assert!(draft
        .rules
        .contains(&("id.yaml".to_string(), body.to_string())));
    let main = catalog.files("tenant_a", "loans").await.unwrap().unwrap();
    assert_eq!(main.rules, on_main);
}

#[tokio::test]
async fn a_draft_writes_and_removes_several_files_in_one_commit() {
    let dolt = testkit::spawn_dolt().await;
    dolt.create_database("tenant_a").await;
    let catalog = DoltCatalog::new(&dolt.url);
    let on_main = rules("required");
    let import = DatasetImport {
        dataset: "loans",
        description: None,
        mapping: MAPPING,
        rules: &on_main,
    };
    let main = catalog
        .import("tenant_a", &import, "Test <test@example.org>")
        .await
        .unwrap();
    // Split by field (decision 0044): one file rewritten, one new, one gone.
    let files = vec![
        (
            "ids.yaml".to_string(),
            Some("dataset: loans\nrules: []\n# kept\n".to_string()),
        ),
        ("id.yaml".to_string(), Some(on_main[0].1.clone())),
        ("amounts.yaml".to_string(), None),
    ];
    let edit = FilesEdit {
        branch: "proposal/loans/s1",
        dataset: "loans",
        files: &files,
        mapping: Some(
            "dataset: loans\nsource: loans\nkey: [ID]\nfields:\n  ID: { column: ID, type: text }\n",
        ),
        author: "Ana <ana@example.org>",
        message: "catalog: split loans by field",
    };
    let commit = catalog
        .save_files("tenant_a", &edit)
        .await
        .unwrap()
        .unwrap();
    let draft = catalog
        .files_at("tenant_a", "proposal/loans/s1", "loans")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(draft.commit, commit);
    let names: Vec<&str> = draft.rules.iter().map(|(f, _)| f.as_str()).collect();
    assert_eq!(names, ["id.yaml", "ids.yaml"]);
    assert!(
        draft.mapping.contains("  ID: { column: ID"),
        "the mapping in the same commit"
    );
    let history = catalog
        .history("tenant_a", "proposal/loans/s1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.commits.len(), 1, "one commit for every file");
    // Main as it was; a draft of that name is not made twice.
    let files_main = catalog.files("tenant_a", "loans").await.unwrap().unwrap();
    assert_eq!((files_main.commit, files_main.rules), (main, on_main));
    assert_eq!(catalog.save_files("tenant_a", &edit).await.unwrap(), None);
}

#[tokio::test]
async fn a_drafts_history_is_its_commits_and_what_each_changed() {
    let dolt = testkit::spawn_dolt().await;
    dolt.create_database("tenant_a").await;
    let catalog = DoltCatalog::new(&dolt.url);
    let on_main = rules("required");
    let import = DatasetImport {
        dataset: "loans",
        description: None,
        mapping: MAPPING,
        rules: &on_main,
    };
    let main = catalog
        .import("tenant_a", &import, "Test <test@example.org>")
        .await
        .unwrap();
    let branch = "proposal/loans/h1";
    let one = "dataset: loans\nrules: []\n";
    let two = "dataset: loans\nrules:\n  - id: L-09\n    field: id\n    check: unique\n";
    let edit = FileEdit {
        branch,
        dataset: "loans",
        file: "amounts.yaml",
        body: one,
        author: "Ana <ana@example.org>",
        adds: false,
    };
    catalog.save_file("tenant_a", &edit, true).await.unwrap();
    let second = FileEdit {
        body: two,
        author: "Ben <ben@example.org>",
        ..edit
    };
    let FileSaved::Saved(last) = catalog.save_file("tenant_a", &second, false).await.unwrap()
    else {
        panic!("saved");
    };

    let history = catalog.history("tenant_a", branch).await.unwrap().unwrap();
    assert_eq!(history.base, main, "it left main where main was");
    let authors: Vec<&str> = history.commits.iter().map(|c| c.author.as_str()).collect();
    assert_eq!(
        authors,
        ["Ben <ben@example.org>", "Ana <ana@example.org>"],
        "newest first"
    );
    assert_eq!(history.commits[0].commit, last);
    assert_eq!(history.commits[1].parent, main);
    assert!(history.commits.iter().all(|c| is_commit(&c.commit)));

    let whole = catalog
        .changes("tenant_a", &main, branch, "loans")
        .await
        .unwrap();
    assert_eq!(
        whole,
        [FileChange {
            file: "amounts.yaml".into(),
            before: Some(on_main[0].1.clone()),
            after: Some(two.into()),
        }]
    );
    let newest = &history.commits[0];
    let one_commit = catalog
        .changes("tenant_a", &newest.parent, &newest.commit, "loans")
        .await
        .unwrap();
    assert_eq!(one_commit[0].before.as_deref(), Some(one));
    assert!(catalog
        .history("tenant_a", "proposal/loans/none")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn a_drafts_mapping_takes_a_field_and_main_keeps_its_own() {
    let dolt = testkit::spawn_dolt().await;
    dolt.create_database("tenant_a").await;
    let catalog = DoltCatalog::new(&dolt.url);
    let import = DatasetImport {
        dataset: "loans",
        description: None,
        mapping: MAPPING,
        rules: &rules("required"),
    };
    let main = catalog
        .import("tenant_a", &import, "Test <test@example.org>")
        .await
        .unwrap();
    // _Add to the mapping_ (decision 0043): a field more, on a draft.
    let body = format!("{MAPPING}  Amount: {{ column: AMOUNT, type: number }}\n");
    let edit = MappingEdit {
        branch: "proposal/loans/m1",
        dataset: "loans",
        body: &body,
        author: "Ana <ana@example.org>",
        starts: false,
    };
    assert_eq!(
        catalog
            .save_mapping("tenant_a", &edit, false)
            .await
            .unwrap(),
        FileSaved::NoDraft
    );
    let FileSaved::Saved(commit) = catalog.save_mapping("tenant_a", &edit, true).await.unwrap()
    else {
        panic!("saved");
    };
    let draft = catalog
        .files_at("tenant_a", "proposal/loans/m1", "loans")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((draft.commit, draft.mapping), (commit, body.clone()));
    // A second field lands on the same draft, one more commit.
    assert!(matches!(
        catalog
            .save_mapping("tenant_a", &edit, false)
            .await
            .unwrap(),
        FileSaved::Saved(_)
    ));
    let none = MappingEdit {
        branch: "proposal/other/m2",
        dataset: "other",
        ..edit
    };
    let none = catalog.save_mapping("tenant_a", &none, true).await.unwrap();
    assert_eq!(none, FileSaved::NoFile, "no dataset to add to");
    // A table no dataset reads starts one.
    let other = "dataset: other\nsource: other\nfields:\n  A: { column: a, type: text }\n";
    let start = MappingEdit {
        branch: "proposal/other/m3",
        dataset: "other",
        body: other,
        author: "Ana <ana@example.org>",
        starts: true,
    };
    let saved = catalog
        .save_mapping("tenant_a", &start, true)
        .await
        .unwrap();
    assert!(matches!(saved, FileSaved::Saved(_)));
    let started = catalog
        .files_at("tenant_a", "proposal/other/m3", "other")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.mapping, other);
    let drafts = catalog.drafts("tenant_a", "proposal/other/").await.unwrap();
    assert_eq!(drafts.len(), 1, "the draft made for nothing is gone");
    let files_main = catalog.files("tenant_a", "loans").await.unwrap().unwrap();
    assert_eq!(
        (files_main.commit, files_main.mapping),
        (main, MAPPING.to_string()),
        "drafts leave main as it was"
    );
    // On `main` itself (decision 0047): one commit there, no draft.
    let on_main = MappingEdit {
        branch: "main",
        dataset: "loans",
        body: &body,
        author: "Ana <ana@example.org>",
        starts: false,
    };
    let FileSaved::Saved(commit) = catalog
        .save_mapping("tenant_a", &on_main, false)
        .await
        .unwrap()
    else {
        panic!("saved on main");
    };
    let now = catalog.files("tenant_a", "loans").await.unwrap().unwrap();
    assert_eq!((now.commit, now.mapping), (commit, body.clone()));
}
