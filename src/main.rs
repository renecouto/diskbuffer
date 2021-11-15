use std::convert::TryInto;
use std::marker::PhantomData;

use std::io::Result;
use futures::Stream;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncSeekExt;
use tokio::{self, io::AsyncBufReadExt, io::AsyncReadExt};

use serde::{de::DeserializeOwned, ser, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct X {
    pub a: String,
}

struct DiskBuffer<T: ser::Serialize + DeserializeOwned> {
    writer: File,
    reader: File,
    phantom_data: PhantomData<T>,
}

impl<T: ser::Serialize + DeserializeOwned> DiskBuffer<T> {
    pub async fn new(file_path: &str) -> DiskBuffer<T> {
        Self {
            writer: OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(file_path)
                .await
                .unwrap(),
            reader: OpenOptions::new().read(true).open(file_path).await.unwrap(),
            phantom_data: PhantomData,
        }
    }

    pub async fn read_one(&mut self) -> Result<Option<T>> {
        use bytes::BytesMut;
        use std::io::SeekFrom;
        let mut res: BytesMut = BytesMut::with_capacity(600);
        let read = self.reader.read_buf(&mut res).await?;
        //FIXME make it simpler
        if read > 0 {
            let mut cursor = Cursor::new(res);
            let mut to_parse = String::new();
            cursor.read_line(&mut to_parse).await?;
            let parsed_lines: i64 = to_parse.as_bytes().len().try_into().unwrap();
            let ress: T = serde_json::from_str(to_parse.as_str())?;
            let pos: i64 = usize::try_into(read).expect("invalid number");
            self.reader
                .seek(SeekFrom::Current(parsed_lines - pos))
                .await
                .unwrap();
            Ok(Some(ress))
        } else {
            Ok(None)
        }
    }

    pub async fn write_one(&mut self, obj: &T) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        self.writer
            .write_all((serde_json::to_string(obj)? + "\n").as_bytes())
            .await?;
        Ok(())
    }
}
use std::io::Cursor;

#[tokio::main]
async fn main() {
    
    let mut db: DiskBuffer<X> = DiskBuffer::new("qwe.jsonl").await;
    db.write_one(&X { a: "123".into() }).await;
    db.write_one(&X { a: "456".into() }).await;
    let res1 = db.read_one().await;
    println!("{:?}", res1);
    db.write_one(&X { a: "789".into() }).await;
    let res1 = db.read_one().await;
    println!("{:?}", res1);
    let res1 = db.read_one().await;
    println!("{:?}", res1);
}
