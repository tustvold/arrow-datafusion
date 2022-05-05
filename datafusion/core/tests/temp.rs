use datafusion::prelude::{ParquetReadOptions, SessionContext};

#[tokio::test]
async fn temp() {
    let ctx = SessionContext::new();

    ctx.register_parquet("patient", "/home/raphael/Downloads/part-00000-f6337bce-7fcd-4021-9f9d-040413ea83f8-c000.snappy.parquet",
                         ParquetReadOptions::default()).await.unwrap();

    let df = ctx.sql("SELECT patient.meta FROM patient LIMIT 10").await.unwrap();
    df.show().await.unwrap();
}