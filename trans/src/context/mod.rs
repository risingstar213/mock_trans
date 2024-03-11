pub mod doca;

pub trait Context {
    fn connect();
    fn send();
    fn recv();
}

