// use assert_cmd::prelude::*;
use assert_cmd::Command;

#[test]
fn insert_and_retrieve() {
    let mut cmd = Command::cargo_bin("simpledb").unwrap();
    let assert = cmd
        .write_stdin(
            r#"insert 1 user1 person1@example.com
        select
        .exit
        "#,
        )
        .assert();

    assert.success().stdout(
        r#"db > Executed.
db > (1, user1, person1@example.com)
Executed.
db > "#,
    );
}

#[test]
fn table_full() {
    let mut cmd = Command::cargo_bin("simpledb").unwrap();
    let mut buf = String::new();
    for i in 0..=1400 {
        buf.push_str(&format!("insert {} user{} person{}@example.com\n", i, i, i));
    }
    buf.push_str(".exit\n");
    let assert = cmd.write_stdin(buf).assert();

    assert.success().stdout(predicates::str::ends_with(
        r#"db > Error: table full
db > "#,
    ));
}

#[test]
fn insert_maximum_length() {
    let long_username = "a".repeat(32);
    let long_email = "a".repeat(255);

    let mut cmd = Command::cargo_bin("simpledb").unwrap();
    let assert = cmd
        .write_stdin(format!(
            r#"insert 1 {} {}
        select
        .exit
        "#,
            long_username, long_email
        ))
        .assert();

    assert.success().stdout(format!(
        r#"db > Executed.
db > (1, {}, {})
Executed.
db > "#,
        long_username, long_email
    ));
}

#[test]
fn insert_too_long() {
    let long_username = "a".repeat(33);
    let long_email = "a".repeat(256);

    let mut cmd = Command::cargo_bin("simpledb").unwrap();
    let assert = cmd
        .write_stdin(format!(
            r#"insert 1 {} {}
        select
        .exit
        "#,
            long_username, long_email
        ))
        .assert();

    assert.success().stdout(predicates::str::starts_with(
        r#"db > Error: string is too long
db > Executed.
db > "#,
    ));
}
