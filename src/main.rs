use std::io::{self, Write};
use thiserror::Error;

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn read_input(buf: &mut String) -> io::Result<usize> {
    io::stdin().read_line(buf)
}

#[derive(Error, Debug)]
enum MetaCommandError {
    #[error("unrecognized command '{0}'")]
    UnrecognizedCommand(String),
}

fn db_meta_command(input: &str) -> Result<(), MetaCommandError> {
    match input {
        ".exit" => {
            std::process::exit(0);
        }
        _ => Err(MetaCommandError::UnrecognizedCommand(input.to_string())),
    }
}

#[derive(Debug)]
enum Statement {
    Insert,
    Select,
}

#[derive(Error, Debug)]
enum PrepareError {
    #[error("unrecognized keyword at start of '{0}'")]
    UnrecognizedKeyword(String),
}

fn prepare_statement(input: &str) -> Result<Statement, PrepareError> {
    if input.starts_with("insert") {
        Ok(Statement::Insert)
    } else if input.starts_with("select") {
        Ok(Statement::Select)
    } else {
        Err(PrepareError::UnrecognizedKeyword(input.to_string()))
    }
}

fn execute_statement(statement: Statement) {
    match statement {
        Statement::Insert => println!("This is where we would do an insert."),
        Statement::Select => println!("This is where we would do a select."),
    }
}

fn main() {
    loop {
        print_prompt();

        let mut input: String = String::new();
        read_input(&mut input).expect("Failed to read input");
        let input = input.trim();

        if input.starts_with(".") {
            match db_meta_command(input) {
                Ok(_) => continue,
                Err(e) => {
                    println!("{}", e);
                }
            }
        }

        match prepare_statement(input) {
            Ok(statement) => {
                execute_statement(statement);
            }
            Err(e) => {
                println!("{}", e);
            }
        }
    }
}
