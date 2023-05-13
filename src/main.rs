use byteorder::{ByteOrder, LittleEndian};
use std::{
    env::args,
    ffi::CStr,
    fmt::Display,
    fs::File,
    io::{self, Read, Seek, Write},
    os::unix::{fs::PermissionsExt, prelude::FileExt},
    path::Path,
    process::exit,
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
    #[error("exit")]
    Exit,
}

fn db_meta_command(input: &str) -> Result<(), MetaCommandError> {
    match input {
        ".exit" => Err(MetaCommandError::Exit),
        _ => Err(MetaCommandError::UnrecognizedCommand(input.to_string())),
    }
}

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = std::mem::size_of::<[u8; COLUMN_USERNAME_SIZE + 1]>();
const EMAIL_SIZE: usize = std::mem::size_of::<[u8; COLUMN_EMAIL_SIZE + 1]>();
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

#[derive(Debug)]
struct Row {
    id: u32,
    username: [u8; COLUMN_USERNAME_SIZE + 1],
    email: [u8; COLUMN_EMAIL_SIZE + 1],
}

impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let username = CStr::from_bytes_until_nul(&self.username)
            .unwrap()
            .to_str()
            .unwrap();
        let email = CStr::from_bytes_until_nul(&self.email)
            .unwrap()
            .to_str()
            .unwrap();
        write!(f, "({}, {}, {})", self.id, username, email)
    }
}

impl Row {
    fn serialize(&self, dest: &mut [u8]) {
        LittleEndian::write_u32(&mut dest[ID_OFFSET..USERNAME_OFFSET], self.id);
        dest[USERNAME_OFFSET..EMAIL_OFFSET].copy_from_slice(&self.username);
        dest[EMAIL_OFFSET..ROW_SIZE].copy_from_slice(&self.email);
    }

    fn deserialize(src: &[u8]) -> Self {
        let id = LittleEndian::read_u32(&src[ID_OFFSET..USERNAME_OFFSET]);
        let mut username = [0; COLUMN_USERNAME_SIZE + 1];
        username.copy_from_slice(&src[USERNAME_OFFSET..EMAIL_OFFSET]);
        let mut email = [0; COLUMN_EMAIL_SIZE + 1];
        email.copy_from_slice(&src[EMAIL_OFFSET..ROW_SIZE]);
        Self {
            id,
            username,
            email,
        }
    }
}

#[derive(Debug)]
struct Pager {
    file: File,
    file_length: usize,
    pages: [Vec<u8>; TABLE_MAX_PAGES],
}

impl Pager {
    fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let mut perms = file.metadata()?.permissions();
        perms.set_mode(0o600);
        file.set_permissions(perms)?;

        let file_length = file.metadata()?.len() as usize;

        let pages = [(); TABLE_MAX_PAGES].map(|_| Vec::with_capacity(0));

        Ok(Self {
            file,
            file_length,
            pages,
        })
    }

    fn get_page(&mut self, page_num: usize) -> io::Result<&mut [u8]> {
        if page_num > TABLE_MAX_PAGES {
            panic!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            );
        }

        let page = &mut self.pages[page_num];
        if page.is_empty() {
            // Cache miss. Allocate memory and load from file.
            page.resize(PAGE_SIZE, 0);

            let mut num_pages = self.file_length / PAGE_SIZE;
            // println!("num_pages: {}, page_num: {}", num_pages, page_num);

            // We might save a partial page at the end of the file
            if self.file_length % PAGE_SIZE > 0 {
                num_pages += 1;
            }

            if page_num <= num_pages {
                let offset = page_num * PAGE_SIZE;
                self.file.read_at(page, offset as u64)?;
            }
        }

        Ok(&mut self.pages[page_num])
    }

    fn flush(&mut self, page_num: usize, size: usize) -> io::Result<()> {
        if self.pages[page_num].is_empty() {
            panic!("Tried to flush empty page");
        }

        let offset = self
            .file
            .seek(io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;

        self.file
            .write_all_at(&self.pages[page_num][..size], offset)
    }
}

fn db_open<P: AsRef<Path>>(path: P) -> io::Result<Table> {
    let pager = Pager::open(path)?;
    let num_rows = pager.file_length / ROW_SIZE;
    let table = Table { num_rows, pager };
    Ok(table)
}

fn db_close(table: &mut Table) -> io::Result<()> {
    let num_full_pages: usize = table.num_rows / ROWS_PER_PAGE;

    for i in 0..num_full_pages {
        if table.pager.pages[i].is_empty() {
            continue;
        }
        table.pager.flush(i, PAGE_SIZE)?;
    }

    // There may be a partial page at the end of the file
    // This should not be needed after we switch to a B-tree
    let num_additional_rows = table.num_rows % ROWS_PER_PAGE;
    if num_additional_rows > 0 {
        let page_num = num_full_pages;
        if !table.pager.pages[page_num].is_empty() {
            table
                .pager
                .flush(page_num, num_additional_rows * ROW_SIZE)?;
        }
    }

    table.pager.file.flush()?;

    Ok(())
}

#[derive(Debug)]
struct Table {
    num_rows: usize,
    pager: Pager,
}

impl Table {
    fn row_slot(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = self.pager.get_page(page_num).unwrap();
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        db_close(self).unwrap();
    }
}

#[derive(Debug)]
enum Statement {
    Insert(Row),
    Select,
}

#[derive(Error, Debug)]
enum PrepareError {
    #[error("id must be positive")]
    NegativeId,
    #[error("string is too long")]
    StringTooLong,
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
            .parse::<i64>()
            .map_err(|_| PrepareError::SyntaxError)?;
        if id < 0 {
            return Err(PrepareError::NegativeId);
        }
        let id = id as u32;

        let username = tokens.get(2).ok_or(PrepareError::SyntaxError)?.to_string();
        if username.len() > COLUMN_USERNAME_SIZE {
            return Err(PrepareError::StringTooLong);
        }
        let mut username_bytes = [0; COLUMN_USERNAME_SIZE + 1];
        username_bytes[..username.len()].copy_from_slice(username.as_bytes());

        let email = tokens.get(3).ok_or(PrepareError::SyntaxError)?.to_string();
        if email.len() > COLUMN_EMAIL_SIZE {
            return Err(PrepareError::StringTooLong);
        }
        let mut email_bytes = [0; COLUMN_EMAIL_SIZE + 1];
        email_bytes[..email.len()].copy_from_slice(email.as_bytes());

        let row = Row {
            id,
            username: username_bytes,
            email: email_bytes,
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
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err(ExecutionError::TableFull);
    }

    let row_num = table.num_rows as usize;
    row.serialize(table.row_slot(row_num));
    table.num_rows += 1;
    Ok(())
}

fn execute_select(table: &mut Table) -> Result<(), ExecutionError> {
    for row_num in 0..table.num_rows {
        let row_slot = table.row_slot(row_num as usize);
        let row = Row::deserialize(row_slot);
        println!("{}", row);
    }
    Ok(())
}

fn main() {
    // let mut table = Table::new();
    if args().len() != 2 {
        println!("Must supply a database filename.");
        exit(1);
    }
    let filename = args().nth(1).unwrap();
    let mut table = db_open(filename).unwrap();

    loop {
        print_prompt();

        let mut input: String = String::new();
        read_input(&mut input).expect("Failed to read input");
        let input = input.trim();

        if input.starts_with(".") {
            match db_meta_command(input) {
                Ok(_) => continue,
                Err(MetaCommandError::Exit) => {
                    break;
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        }

        let statement = match prepare_statement(input) {
            Ok(statement) => statement,
            Err(e) => {
                println!("Error: {}", e);
                continue;
            }
        };

        match execute_statement(statement, &mut table) {
            Ok(_) => {
                println!("Executed.");
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }
}
