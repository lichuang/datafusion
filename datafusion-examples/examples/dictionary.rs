use arrow;
use arrow::array::{ArrayRef, Decimal128Array, DictionaryArray, Int32Array, RecordBatch};
use arrow::compute::kernels::cmp::lt_eq;
use datafusion::error::Result;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::physical_expr_common::datum::apply_cmp;
use datafusion::prelude::*;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use std::fs::File;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    if false {
        let array_keys = Int32Array::from_iter_values(vec![0]);
        let array_values =
            Decimal128Array::from_iter_values(vec![4]).with_precision_and_scale(4, 1)?;
        let array = Arc::new(DictionaryArray::new(array_keys, Arc::new(array_values)));
        let value = ColumnarValue::Array(array);
        println!("array: {:?}", value);

        /*
        let scalar_value = ScalarValue::Dictionary(
            Box::new(arrow_schema::DataType::Int32),
            Box::new(ScalarValue::Decimal128(Some(10), 4, 1)),
        );
        println!(
            "scalar_value: {:?}, to: {:?}",
            scalar_value,
            &scalar_value.to_scalar()?,
        );
        let scalar_value2 = ColumnarValue::Scalar(ScalarValue::Dictionary(
            Box::new(arrow_schema::DataType::Int32),
            Box::new(ScalarValue::Decimal128(Some(10), 4, 1)),
        ));
        */
        let scalar_value = ColumnarValue::Scalar(ScalarValue::Decimal128(Some(10), 4, 1));
        let result = apply_cmp(&scalar_value, &value, lt_eq);
        println!("result: {:?}", result);
        return Ok(());
    }

    // Prepare record batch
    let array_values = Decimal128Array::from_iter_values(vec![10, 20, 30, 40])
        .with_precision_and_scale(4, 1)?;
    let array_keys = Int32Array::from_iter_values(vec![0, 1, 2, 3]);
    let array = Arc::new(DictionaryArray::new(array_keys, Arc::new(array_values)));
    let batch = RecordBatch::try_from_iter(vec![("col", array as ArrayRef)])?;

    // Write batch to parquet
    let file_path = "dictionary_decimal.parquet";

    let file = File::create(file_path)?;
    let properties = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_bloom_filter_enabled(true)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;

    writer.write(&batch)?;
    writer.flush()?;
    writer.close()?;

    // Prepare context
    let config = SessionConfig::default()
        .with_parquet_bloom_filter_pruning(true)
        //.with_parquet_pruning(false)
        .with_collect_statistics(true);
    let ctx = SessionContext::new_with_config(config);

    ctx.register_parquet("t", file_path, ParquetReadOptions::default())
        .await?;

    // In case pruning predicate not created (due to cast), there is a record in resultset
    ctx.sql("select * from t where col = 1")
        .await?
        .show()
        .await?;

    println!("after");

    ctx.sql("select * from t").await?.show().await?;

    println!("after 1");

    // In case of triggered RowGroup pruning -- the only RowGroup eliminated while pruning by statistics
    ctx.sql("select * from t where col = cast(0.1 as decimal(4, 1))")
        .await?
        .show()
        .await?;

    Ok(())
}
