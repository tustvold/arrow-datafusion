use crate::error::{DataFusionError, Result};
use futures::StreamExt;
use object_store::{path::Path, GetResult, ObjectStore};
use std::fs::File;
use std::io::{Seek, Write};

/// Downloads a file to local disk
pub async fn fetch_to_local_file(store: &dyn ObjectStore, path: &Path) -> Result<File> {
    match store.get(path).await {
        Ok(GetResult::File(file, _)) => Ok(file.into_std().await),
        Ok(GetResult::Stream(mut s)) => {
            let mut file = tempfile::tempfile()?;
            while let Some(result) = s.next().await {
                let bytes = result.map_err(|e| DataFusionError::External(Box::new(e)))?;
                file.write_all(&bytes)?;
            }
            file.flush()?;
            file.rewind()?;
            Ok(file)
        }
        Err(e) => Err(DataFusionError::External(Box::new(e))),
    }
}
