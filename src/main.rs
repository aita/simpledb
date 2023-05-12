use byteorder::{ByteOrder, LittleEndian};
use std::{
    fmt::Display,
    io::{self, Write},
};
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

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
#[derive(Debug)]
struct Row {
    id: u32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let username = std::str::from_utf8(&self.username).unwrap();
        let email = std::str::from_utf8(&self.email).unwrap();
        write!(f, "({}, {}, {})", self.id, username, email)
    }
}

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = std::mem::size_of::<[u8; COLUMN_USERNAME_SIZE]>();
const EMAIL_SIZE: usize = std::mem::size_of::<[u8; COLUMN_EMAIL_SIZE]>();
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

impl Row {
    fn serialize(&self, dest: &mut [u8]) {
        LittleEndian::write_u32(&mut dest[ID_OFFSET..USERNAME_OFFSET], self.id);
        dest[USERNAME_OFFSET..EMAIL_OFFSET].copy_from_slice(&self.username);
        dest[EMAIL_OFFSET..ROW_SIZE].copy_from_slice(&self.email);
    }

    fn deserialize(src: &[u8]) -> Self {
        let id = LittleEndian::read_u32(&src[ID_OFFSET..USERNAME_OFFSET]);
        let mut username = [0; COLUMN_USERNAME_SIZE];
        username.copy_from_slice(&src[USERNAME_OFFSET..EMAIL_OFFSET]);
        let mut email = [0; COLUMN_EMAIL_SIZE];
        email.copy_from_slice(&src[EMAIL_OFFSET..ROW_SIZE]);
        Self {
            id,
            username,
            email,
        }
    }
}

#[derive(Debug)]
struct Table {
    num_rows: u32,
    pages: [Vec<u8>; TABLE_MAX_PAGES],
}

impl Table {
    fn new() -> Self {
        Self {
            num_rows: 0,
            pages: [(); TABLE_MAX_PAGES].map(|_| Vec::with_capacity(0)),
        }
    }

    fn row_slot(&self, row_num: usize) -> Option<&[u8]> {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = &self.pages[page_num];
        if page.is_empty() {
            return None;
        }
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        Some(&page[byte_offset..byte_offset + ROW_SIZE])
    }

    fn row_slot_mut(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = &mut self.pages[page_num];
        if page.is_empty() {
            page.reserve_exact(PAGE_SIZE);
            page.resize(PAGE_SIZE, 0);
        }
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }
}

#[derive(Debug)]
enum Statement {
    Insert(Row),
    Select,
}

#[derive(Error, Debug)]
enum PrepareError {
    #[error("syntax error")]
    SyntaxError,
    #[error("unrecognized keyword at start of '{0}'")]
    UnrecognizedKeyword(String),
}

fn prepare_statement(input: &str) -> Result<Statement, PrepareError> {
    if input.starts_with("insert") {
        let tokens = input.split_whitespace().collect::<Vec<_>>();
        if tokens.len() != 4 {
            return Err(PrepareError::SyntaxError);
        }
        let id = tokens
            .get(1)
            .ok_or(PrepareError::SyntaxError)?
            .parse::<u32>()
            .map_err(|_| PrepareError::SyntaxError)?;
        let mut username = tokens
            .get(2)
            .ok_or(PrepareError::SyntaxError)?
            .as_bytes()
            .to_vec();
        username.resize(COLUMN_USERNAME_SIZE, 0);
        let mut email = tokens
            .get(3)
            .ok_or(PrepareError::SyntaxError)?
            .as_bytes()
            .to_vec();
        email.resize(COLUMN_EMAIL_SIZE, 0);
        let row = Row {
            id: id,
            username: username.try_into().unwrap(),
            email: email.try_into().unwrap(),
        };
        Ok(Statement::Insert(row))
    } else if input.starts_with("select") {
        Ok(Statement::Select)
    } else {
        Err(PrepareError::UnrecognizedKeyword(input.to_string()))
    }
}

#[derive(Error, Debug)]
enum ExecutionError {
    #[error("table full")]
    TableFull,
}

fn execute_statement(statement: Statement, table: &mut Table) -> Result<(), ExecutionError> {
    match statement {
        Statement::Insert(row) => execute_insert(&row, table),
        Statement::Select => execute_select(table),
    }
}

fn execute_insert(row: &Row, table: &mut Table) -> Result<(), ExecutionError> {
    if table.num_rows >= TABLE_MAX_ROWS as u32 {
        return Err(ExecutionError::TableFull);
    }

    let row_num = table.num_rows as usize;
    row.serialize(table.row_slot_mut(row_num));
    table.num_rows += 1;
    Ok(())
}

fn execute_select(table: &Table) -> Result<(), ExecutionError> {
    for row_num in 0..table.num_rows {
        let row_slot = table.row_slot(row_num as usize).unwrap();
        let row = Row::deserialize(row_slot);
        println!("{}", row);
    }
    Ok(())
}

fn main() {
    let mut table = Table::new();

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

        let statement = match prepare_statement(input) {
            Ok(statement) => statement,
            Err(e) => {
                println!("{}", e);
                continue;
            }
        };

        match execute_statement(statement, &mut table) {
            Ok(_) => {
                println!("Executed.");
            }
            Err(e) => {
                println!("{}", e);
            }
        }
    }
}
