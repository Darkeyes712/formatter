async fn fetch_transfers(
    &self,
    _pair: &Pair,
    _exchange: &str,
    _bot_id: String,
    _operation: String,
) -> DriverResult<Option<Vec<KinesisTransaction>>> {
    Err(DriverError::Generic("Function not implemented".to_string()))
}