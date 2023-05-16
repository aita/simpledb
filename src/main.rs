use std::{
    env::args,
    ffi::CStr,
    fmt::Display,
    fs::File,
    io::{self, Seek, Write},
    os::unix::prelude::FileExt,
    path::Path,
    process::exit,
};
use thiserror::Error;

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

// Common Node Header Layout
const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>();
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = std::mem::size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf Node Header Layout
const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf Node Body Layout
const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

unsafe fn leaf_node_num_cells(node: &mut [u8]) -> &mut u32 {
    &mut *(node[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_HEADER_SIZE].as_mut_ptr() as *mut u32)
}

fn leaf_node_offset(cell_num: usize) -> usize {
    LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE
}

fn leaf_node_cell(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    &mut node[leaf_node_offset(cell_num)..leaf_node_offset(cell_num + 1)]
}

unsafe fn leaf_node_key(node: &mut [u8], cell_num: usize) -> &mut u32 {
    &mut *(leaf_node_cell(node, cell_num).as_mut_ptr() as *mut u32)
}

fn leaf_node_value(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    leaf_node_cell(node, cell_num)[LEAF_NODE_KEY_SIZE..]
        .split_at_mut(LEAF_NODE_VALUE_SIZE)
        .0
}

unsafe fn initialize_leaf_node(node: &mut [u8]) {
    *leaf_node_num_cells(node) = 0;
}

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
        dest[..USERNAME_OFFSET].copy_from_slice(self.id.to_le_bytes().as_slice());
        dest[USERNAME_OFFSET..EMAIL_OFFSET].copy_from_slice(&self.username);
        dest[EMAIL_OFFSET..ROW_SIZE].copy_from_slice(&self.email);
    }

    fn deserialize(src: &[u8]) -> Self {
        let id = unsafe { *(src[ID_OFFSET..USERNAME_OFFSET].as_ptr() as *const u32) };
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
    num_pages: usize,
    pages: [Vec<u8>; TABLE_MAX_PAGES],
}

impl Pager {
    fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let file_length = file.metadata()?.len() as usize;
        let num_pages = file_length / PAGE_SIZE;

        if file_length % PAGE_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Db file is not a whole number of pages",
            ));
        }

        let pages = [(); TABLE_MAX_PAGES].map(|_| Vec::with_capacity(0));

        Ok(Self {
            file,
            file_length,
            num_pages,
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

            // We might save a partial page at the end of the file
            if self.file_length % PAGE_SIZE > 0 {
                num_pages += 1;
            }

            if page_num <= num_pages {
                let offset = page_num * PAGE_SIZE;
                self.file.read_at(page, offset as u64)?;
            }

            if page_num >= self.num_pages {
                self.num_pages = page_num + 1;
            }
        }

        Ok(&mut self.pages[page_num])
    }

    fn flush(&mut self, page_num: usize) -> io::Result<()> {
        if self.pages[page_num].is_empty() {
            panic!("Tried to flush empty page");
        }

        let offset = self
            .file
            .seek(io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;

        self.file
            .write_all_at(&self.pages[page_num][..PAGE_SIZE], offset)
    }
}

fn db_open<P: AsRef<Path>>(path: P) -> io::Result<Table> {
    let mut pager = Pager::open(path)?;
    let root_page_num = 0;

    if pager.num_pages == 0 {
        // New database file. Initialize page 0 as leaf node.
        let root_node = pager.get_page(root_page_num)?;
        unsafe {
            initialize_leaf_node(root_node);
        }
    }

    Ok(Table {
        root_page_num,
        pager,
    })
}

fn db_close(table: &mut Table) -> io::Result<()> {
    for i in 0..table.pager.num_pages {
        if table.pager.pages[i].is_empty() {
            continue;
        }
        table.pager.flush(i)?;
    }

    table.pager.file.flush()?;

    Ok(())
}

#[derive(Debug)]
struct Cursor<'a> {
    table: &'a mut Table,
    page_num: usize,
    cell_num: usize,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    fn value(&mut self) -> io::Result<&mut [u8]> {
        let page_num = self.page_num;
        let page = self.table.pager.get_page(page_num)?;
        Ok(leaf_node_value(page, self.cell_num))
    }

    fn advance(&mut self) -> io::Result<()> {
        let node = self.table.pager.get_page(self.page_num)?;
        self.cell_num += 1;
        if self.cell_num >= *unsafe { leaf_node_num_cells(node) } as usize {
            self.end_of_table = true;
        }
        Ok(())
    }

    fn leaf_node_insert(&mut self, key: u32, value: &Row) -> io::Result<()> {
        let node = self.table.pager.get_page(self.page_num)?;

        let num_cells = *unsafe { leaf_node_num_cells(node) } as usize;
        if num_cells >= LEAF_NODE_MAX_CELLS {
            // Node full
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Need to implement splitting a leaf node",
            ));
        }

        if self.cell_num <= num_cells {
            // Make room for new cell
            for i in (self.cell_num + 1..=num_cells).rev() {
                let (src, dest) = node[leaf_node_offset(i - 1)..leaf_node_offset(i + 1)]
                    .split_at_mut(LEAF_NODE_CELL_SIZE);
                dest.copy_from_slice(src);
            }
        }

        *unsafe { leaf_node_num_cells(node) } += 1;
        *unsafe { leaf_node_key(node, self.cell_num) } = key;

        value.serialize(leaf_node_value(node, self.cell_num));

        Ok(())
    }
}

#[derive(Debug)]
struct Table {
    pager: Pager,
    root_page_num: usize,
}

impl Table {
    fn table_start(&mut self) -> io::Result<Cursor> {
        let page_num = self.root_page_num;
        let cell_num = 0;

        let root_node = self.pager.get_page(page_num)?;
        let num_cells = *unsafe { leaf_node_num_cells(root_node) } as usize;
        let end_of_table = num_cells == 0;

        Ok(Cursor {
            table: self,
            page_num,
            cell_num,
            end_of_table,
        })
    }

    fn table_end(&mut self) -> io::Result<Cursor> {
        let page_num = self.root_page_num;

        let root_node = self.pager.get_page(page_num)?;
        let cell_num = *unsafe { leaf_node_num_cells(root_node) } as usize;
        let end_of_table = true;

        Ok(Cursor {
            table: self,
            page_num,
            cell_num,
            end_of_table,
        })
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        db_close(self).unwrap();
    }
}

#[derive(Error, Debug)]
enum MetaCommandError {
    #[error("unrecognized command '{0}'")]
    UnrecognizedCommand(String),
    #[error("exit")]
    Exit,
}

fn db_meta_command(input: &str, table: &mut Table) -> Result<(), MetaCommandError> {
    match input {
        ".exit" => Err(MetaCommandError::Exit),
        ".constants" => {
            println!("Constants:");
            print_constants();
            Ok(())
        }
        ".btree" => {
            println!("Tree:");
            print_leaf_node(table.pager.get_page(0).unwrap());
            Ok(())
        }
        _ => Err(MetaCommandError::UnrecognizedCommand(input.to_string())),
    }
}

fn print_constants() {
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}

fn print_leaf_node(node: &mut [u8]) {
    let num_cells = *unsafe { leaf_node_num_cells(node) } as usize;
    println!("leaf (size {})", num_cells);
    for i in 0..num_cells {
        let key = *unsafe { leaf_node_key(node, i) };
        println!("  - {} : {}", i, key);
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
    #[error("cursor error")]
    CursorError(#[from] io::Error),
}

fn execute_statement(statement: Statement, table: &mut Table) -> Result<(), ExecutionError> {
    match statement {
        Statement::Insert(row) => execute_insert(&row, table),
        Statement::Select => execute_select(table),
    }
}

fn execute_insert(row: &Row, table: &mut Table) -> Result<(), ExecutionError> {
    let node = table.pager.get_page(table.root_page_num)?;
    if *unsafe { leaf_node_num_cells(node) } as usize >= LEAF_NODE_MAX_CELLS {
        return Err(ExecutionError::TableFull);
    }

    let mut cursor = table.table_end()?;
    cursor.leaf_node_insert(row.id, row)?;

    Ok(())
}

fn execute_select(table: &mut Table) -> Result<(), ExecutionError> {
    let mut cursor = table.table_start()?;

    while !cursor.end_of_table {
        let row = Row::deserialize(cursor.value()?);
        println!("{}", row);
        cursor.advance()?;
    }
    Ok(())
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn read_input(buf: &mut String) -> io::Result<usize> {
    io::stdin().read_line(buf)
}

fn main() {
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
            match db_meta_command(input, &mut table) {
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
