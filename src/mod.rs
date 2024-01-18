mod http;
mod ws;
use super::{event_loop::WsRequest, *};
use chrono::DateTime;
use dte_traits::{
    reporting::KinesisTransaction, utils::timestamp_millis_to_utc, ConnectionStatus, DriverClient,
    DriverFeature, DriverResult, OrderRequest, RawBalance, RawOrder, RawTrade,
};
use isahc::{prelude::Configurable, HttpClient, HttpClientBuilder};
use klp_types::Pair;
use parking_lot::RwLock;
use std::sync::Arc;

// orders could be cancelled by batches of 20 orders
// https://www.okex.com/docs-v5/en/#rest-api-trade-cancel-multiple-orders
// https://www.okex.com/docs-v5/en/#websocket-api-trade-cancel-multiple-orders
pub(crate) const CANCEL_ORDERS_BATCH_COUNT: usize = 20;

const ONE_DAY_IN_MILLIS: i64 = 86_400_000;

#[derive(Clone)]
pub struct OkexClient {
    pub(super) status: Arc<RwLock<ConnectionStatus>>,
    config: OkexConfig,
    instrument_converter: InstrumentConverter,
    requests_tx: mpsc::UnboundedSender<WsRequest>,
    http_client: HttpClient,
}

impl OkexClient {
    pub fn with_status(
        config: OkexConfig,
        instrument_converter: InstrumentConverter,
        status: ConnectionStatus,
    ) -> (Self, mpsc::UnboundedReceiver<WsRequest>) {
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        let http_client = HttpClientBuilder::new()
            .tcp_nodelay()
            .build()
            .expect("Can't build Isahc for OKX");
        let this = Self {
            status: Arc::new(RwLock::new(status)),
            config,
            instrument_converter,
            requests_tx,
            http_client,
        };

        (this, requests_rx)
    }

    pub(super) fn check_ws_online_status(&self) -> DriverResult<()> {
        if self.status() != ConnectionStatus::Online {
            Err(DriverError::Generic("WS Client isn't Online".to_string()))
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Debug for OkexClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OkexClient")
            .field("status", &self.status)
            .finish()
    }
}

#[async_trait::async_trait]
impl DriverClient for OkexClient {
    /// Checks if driver client implementation supports feature
    fn supports_feature(&self, feature: DriverFeature) -> bool {
        match feature {
            DriverFeature::BatchOpen => false,
            DriverFeature::BatchCancel => false,
            DriverFeature::ImmediateOrCancelOrders => true,
            DriverFeature::PostOnlyOrders => true,
        }
    }

    fn status(&self) -> ConnectionStatus {
        *self.status.read()
    }

    fn pairs(&self) -> &[Pair] {
        &self.config.pairs
    }

    fn has_collateral_balances(&self) -> bool {
        // fetch collateral balances only for Futures Perpetual mode
        self.instrument_converter.instrument_type == OkexInstrumentType::Swap
    }

    async fn fetch_open_orders(&self) -> DriverResult<Vec<RawOrder>> {
        let orders = self.rest_fetch_open_orders().await?;

        let orders = orders
            .into_iter()
            .filter_map(|order| {
                let exchange_created_at = timestamp_millis_to_utc(order.created_at);

                Some(RawOrder {
                    internal_order_id: order.client_order_id,
                    order_id: order.order_id,
                    pair: self
                        .instrument_converter
                        .find_pair(&order.instrument_id)?
                        .clone(),
                    side: order.side,
                    price: order.price,
                    amount: order.amount,
                    internal_created_at: exchange_created_at.unwrap_or_else(Utc::now),
                    exchange_created_at,
                })
            })
            .collect();

        Ok(orders)
    }

    async fn fetch_balances(&self) -> DriverResult<Vec<RawBalance>> {
        let balances = self.rest_fetch_balances().await?;

        let balances = balances.into_iter().map(RawBalance::from).collect();

        Ok(balances)
    }

    async fn fetch_collateral_balances(&self) -> DriverResult<Vec<RawCollateral>> {
        // In Portfolio Margin mode the SPOT baalnces are used as collateral for Futures markets
        let balances = self.rest_fetch_balances().await?;

        let balances = balances.into_iter().map(RawCollateral::from).collect();

        Ok(balances)
    }

    async fn open_order(&self, req: OrderRequest) -> DriverResult<RawOrder> {
        self.ws_open_order(req).await
    }

    async fn cancel_order_by_id(&self, pair: &Pair, order_id: OrderId) -> DriverResult<()> {
        let inst_id = self
            .instrument_converter
            .find_instrument(pair)
            .ok_or_else(|| DriverError::NotSupportedSymbol(pair.to_symbol()))?
            .id();

        let res = self
            .ws_cancel_order_by_id(order_id.clone(), inst_id.clone())
            .await;

        match res {
            Ok(_) => Ok(()),
            Err(
                DriverError::OrderNotFound
                | DriverError::OrderAlreadyCancelled
                | DriverError::OrderAlreadyFilled,
            ) => res,
            Err(e) => {
                error!("Cancel order ws request failed: {:?}. Fallback to REST", e);

                self.rest_cancel_order_by_id(order_id, inst_id).await
            }
        }
    }

    async fn cancel_all(&self, pair: &Pair) -> DriverResult<Vec<OrderId>> {
        let inst_id = self
            .instrument_converter
            .find_instrument(pair)
            .ok_or_else(|| DriverError::NotSupportedSymbol(pair.to_symbol()))?
            .id();

        let order_ids = self
            .rest_fetch_open_orders()
            .await?
            .into_iter()
            .filter_map(|order| {
                if order.instrument_id == inst_id {
                    Some(order.order_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // early return if no orders to cancel
        if order_ids.is_empty() {
            return Ok(vec![]);
        }

        // try to cancel orders with ws request
        // it returns cancelled orders and not cancelled orders ids
        let not_cancelled_order_ids = match self
            .ws_cancel_orders(order_ids.clone(), inst_id.clone())
            .await
        {
            // All orders were cancelled
            Ok((cancelled_order_ids, not_cancelled_order_ids))
                if not_cancelled_order_ids.is_empty() =>
            {
                return Ok(cancelled_order_ids);
            }
            // Some orders were not cancelled
            Ok((_, not_cancelled_order_ids)) => not_cancelled_order_ids,
            // Error
            Err(_) => order_ids,
        };

        self.rest_cancel_orders(&not_cancelled_order_ids, inst_id)
            .await
    }

    async fn fetch_funding_rate_transactions(
        &self,
        _pair: &Pair,
        exchange: &str,
        bot_id: String,
        operation: String,
    ) -> DriverResult<Option<Vec<KinesisTransaction>>> {

        let mut funding_rate_transactions = Vec::new();

        let bill_data = self.rest_fetch_account_bills()
        .await
        .expect("No Kinesis data available");

        for bill in bill_data {
            if bill.type_ == 8 {
                let kinesis_transaction = bill
                    .to_kinesis_transaction(exchange, bot_id.clone(), operation.clone())
                    .await;
                funding_rate_transactions.push(kinesis_transaction)
            }
        }

        Ok(Some(funding_rate_transactions))
    }

    async fn fetch_trades_since(
        &self,
        pair: &Pair,
        _assume_24_hours_before: DateTime<Utc>,
    ) -> DriverResult<Vec<RawTrade>> {
        let inst_id = self
            .instrument_converter
            .find_instrument(pair)
            .ok_or_else(|| DriverError::NotSupportedSymbol(pair.to_symbol()))?
            .id();

        let end_time = Utc::now().timestamp_millis();
        let start_time = end_time - ONE_DAY_IN_MILLIS;

        let trades = self.rest_fetch_trades(Some(inst_id)).await?;

        let trades: Vec<RawTrade> = trades
            .into_iter()
            .filter_map(|trade| {
                if trade.created_at >= start_time && trade.created_at <= end_time {
                    Some(RawTrade {
                        trade_id: trade.trade_id,
                        order_id: trade.order_id,
                        pair: pair.clone(),
                        side: trade.side,
                        price: trade.price,
                        filled_amount: trade.filled_amount,
                        // Negative number represents the user transaction fee charged by the platform.
                        // Positive number represents rebate. Reverse this value.
                        fee_amount: Some(-trade.fee),
                        fee_currency: Some(trade.fee_currency),
                        liquidity: trade.liquidity,
                        internal_created_at: Utc::now(),
                        exchange_created_at: timestamp_millis_to_utc(trade.created_at as u64),
                    })
                } else {
                    None
                }
            })
            .collect();

        debug!("Last day trades ({:?}): {:?}", pair, trades.len());

        Ok(trades)
    }

    /// Fetches recent historical trades for all pairs
    async fn fetch_all_trades_since(
        &self,
        _assume_24_hours_before: DateTime<Utc>,
    ) -> DriverResult<Vec<RawTrade>> {
        let end_time = Utc::now().timestamp_millis();
        let start_time = end_time - ONE_DAY_IN_MILLIS;

        let trades = self.rest_fetch_trades(None).await?;

        let trades: Vec<RawTrade> = trades
            .into_iter()
            .filter_map(|trade| {
                if trade.created_at >= start_time && trade.created_at <= end_time {
                    Some(RawTrade {
                        trade_id: trade.trade_id,
                        order_id: trade.order_id,
                        pair: self
                            .instrument_converter
                            .find_pair(&trade.instrument_id)?
                            .clone(),
                        side: trade.side,
                        price: trade.price,
                        filled_amount: trade.filled_amount,
                        fee_amount: Some(trade.fee),
                        fee_currency: Some(trade.fee_currency),
                        liquidity: trade.liquidity,
                        internal_created_at: Utc::now(),
                        exchange_created_at: timestamp_millis_to_utc(trade.created_at as u64),
                    })
                } else {
                    None
                }
            })
            .collect();

        debug!("Last day trades (all): {:?}", trades.len());

        Ok(trades)
    }

    fn generate_client_id(&self, _: &klp_types::exchange::OrderRequest) -> ClientOrderId {
        ClientOrderId::from(Utc::now().timestamp_nanos_opt().unwrap_or_default())
    }
}
