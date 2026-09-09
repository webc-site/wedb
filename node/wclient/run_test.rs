fn main() {
    println!("{:?}", std::any::type_name::<crossfire::mpsc::tx::Tx<i32>>());
}
