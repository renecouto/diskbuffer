use std::convert::TryInto;
use std::marker::PhantomData;

use std::io::Cursor;
use std::io::Result;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncSeekExt;
use tokio::{self, io::AsyncBufReadExt, io::AsyncReadExt};

use serde::{de::DeserializeOwned, ser, Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct X {
    pub a: String,
}

struct DiskBuffer<T: ser::Serialize + DeserializeOwned + Clone + Debug> {
    writer: File,
    writer_file_size: u64,
    writer_file_name: String,
    reader_file_name: String,
    reader: File,
    file_size: u64,                      // max file size in bytes
    completed_files: Vec<(String, u64)>, // all files between reader and writer, along with their sizes
    // reader goes to a new file whenever it gets None and has read until the end of the file
    phantom_data: PhantomData<T>,
    file_path: String,
    reader_file_idx: u64,
    writer_file_idx: u64,
    read_buffer: Vec<T>,
}

async fn new_writer(file_path: &str) -> File {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_path)
        .await
        .unwrap()
}
async fn new_reader(file_path: &str) -> File {
    OpenOptions::new()
        .read(true)
        .open(file_path)
        .await
        .unwrap()
}

async fn read_lines_nl_ended(reader: &mut File) -> Result<Vec<String>> {
    use bytes::BytesMut;
    use std::io::SeekFrom;
    let pos = reader.stream_position().await?;
    let mut lines_to_ret:Vec<String> = vec![];
    let mut res: BytesMut = BytesMut::with_capacity(1024);
    let read = reader.read_buf(&mut res).await?;
    let mut cursor = Cursor::new(res);
    // let mut total_bytes_read = 0;
    let mut bytes_read_back = 0;
    loop {
        let mut to_parse = String::new();
        let bytes_read = cursor.read_line(&mut to_parse).await?;
        println!("read: {}", &to_parse);
        if to_parse.ends_with('\n') {
            // total_bytes_read += bytes_read;
            lines_to_ret.push(to_parse);
        } else {
            bytes_read_back += bytes_read;
        }
        if bytes_read == 0 {
            break;
        }
    }
    let relative_position: i64 = bytes_read_back.try_into().unwrap();
    reader
                .seek(SeekFrom::Current(- relative_position))
                .await
                .unwrap();
    
    Ok(lines_to_ret)
}

impl<T: ser::Serialize + DeserializeOwned + Clone + Debug> DiskBuffer<T> {
    pub async fn new(file_path: &str, file_size: u64) -> DiskBuffer<T> {
        let first_file_path = format!("{}{}", file_path, "0");
        let writer = new_writer(&first_file_path).await;

        Self {
            file_path: file_path.into(),
            writer: writer,
            writer_file_name: first_file_path.clone(),
            reader_file_name: first_file_path.clone(),
            reader: new_reader(&first_file_path).await,
            phantom_data: PhantomData,
            file_size: file_size,
            writer_file_size: 0,
            completed_files: vec![],
            reader_file_idx: 0,
            writer_file_idx: 0,
            read_buffer: vec![],
        }
    }

    pub async fn read_one(&mut self) -> Result<Option<T>> {
        

        use bytes::BytesMut;
        use std::io::SeekFrom;
        if self.read_buffer.len() > 0 {
            println!("returning from buffer");
            let to_ret = self.read_buffer[0].clone();
            self.read_buffer = self.read_buffer.as_mut_slice()[1..].into();
            return Ok(Some(to_ret))
        }
        let mut to_ret = None;
        let mut to_buffer: Vec<T> = vec![];
        let mut lines: Vec<T> =  vec![];
        for i in read_lines_nl_ended(&mut self.reader).await? {
            lines.push(serde_json::from_str(&i)?);
        } 
        if lines.len() == 0 {
            return Ok(None)
        } else {
            if lines.len() > 0 {
                to_ret = Some(lines[0].clone());
                to_buffer = lines.as_mut_slice()[1..].into();
            }
        }
        println!("to buffer: {:?}", &to_buffer);
        self.read_buffer = to_buffer;
        let pos = self.reader.stream_position().await?;
    
        if let Some((name, size)) = self.completed_files.first() {
            if *name == self.reader_file_name && *size == pos {
                self.reader_file_idx += 1;
                let new_file_path = format!("{}{}", &self.file_path, &self.reader_file_idx);
                println!("rolling to new file! on reader {}", &new_file_path);
                self.reader = new_reader(&new_file_path).await;
                self.reader_file_name = new_file_path;
                tokio::fs::remove_file(name).await?;
                self.completed_files = self.completed_files.as_mut_slice()[1..].into(); // haha now this is a deque
            }
        }
        Ok(to_ret)
    }

    pub async fn write_one(&mut self, obj: &T) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        self.writer
            .write_all((serde_json::to_string(obj)? + "\n").as_bytes())
            .await?;
        let pos = self.writer.stream_position().await?;
        println!("{:?}", pos);
        if pos >= self.file_size {
            self.writer_file_idx += 1;
            let new_writer_file_name = format!(
                "{}{}",
                self.file_path,
                self.writer_file_idx            );
            println!("rolling over writer to {}", &new_writer_file_name);
            self.writer.flush().await;
            self.writer = new_writer(new_writer_file_name.as_str()).await;
            self.completed_files
                .push((self.writer_file_name.clone(), pos));
            self.writer_file_name = new_writer_file_name;
            self.writer.flush().await;
        }
        self.writer_file_size = pos;
        Ok(())
    }
}


#[tokio::main]
async fn main() {
    let mut db: DiskBuffer<X> = DiskBuffer::new("qwe.jsonl", 600).await;
    println!("initiated");
    db.write_one(&X { a: "123".into() }).await;
    println!("wrote one item");
    db.write_one(&X { a: "456".into() }).await;
    println!("wrote two items");
    let res1 = db.read_one().await;
    println!("{:?}", res1);
    db.write_one(&X { a: "789".into() }).await;
    println!("wrote third item");
    db.write_one(&X { a: "101112".into() }).await;
    println!("wrote third item");
    db.write_one(&X { a: "131415".into() }).await;
    println!("wrote third item");
    db.write_one(&X { a: "161718".into() }).await;
    println!("wrote third item");
    let res1 = db.read_one().await;
    println!("{:?}", res1);
    let res1 = db.read_one().await;
    println!("{:?}", res1);
    let res1 = db.read_one().await;
    println!("{:?}", res1);
}
