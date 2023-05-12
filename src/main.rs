use std::io::{self, Write};

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn read_input(buf: &mut String) -> io::Result<usize> {
    io::stdin().read_line(buf)
}

fn main() {
    loop {
        print_prompt();

        let mut input: String = String::new();
        read_input(&mut input).expect("Failed to read input");

        match input.trim() {
            ".exit" => {
                break;
            }
            _ => {
                println!("Unrecognized command '{}'.", input.trim());
            }
        }
    }
}
