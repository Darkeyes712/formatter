use super::{super::*, OkexClient, CANCEL_ORDERS_BATCH_COUNT};
use chrono::SecondsFormat;
use dte_shared::{http_client_isahc::AuthenticatedHttpClientIsahc, utils::base64_wrapper};
use dte_traits::DriverResult;
use futures_util::future;
use isahc::{
    http::{HeaderName, HeaderValue, StatusCode},
    HttpClient, Request,
};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

// page size is 100 by default, as stated in OKEX v5 docs
// https://www.okex.com/docs-v5/en/#rest-api-trade-get-order-list
const FETCH_OPEN_ORDERS_COUNT: usize = 100;
// https://www.okex.com/docs-v5/en/#rest-api-trade-get-transaction-details-last-3-days
const FETCH_RECENT_TRADES_COUNT: usize = 100;

#[async_trait::async_trait]
impl AuthenticatedHttpClientIsahc for OkexClient {
    fn base_url(&self) -> &str {
        self.config.http_base_url()
    }

    fn client(&self) -> &HttpClient {
        &self.http_client
    }

    // https://www.okex.com/docs-v5/en/#rest-api-authentication-making-requests
    async fn sign(&self, mut req: Request<String>) -> DriverResult<Request<String>> {
        let iso_date = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

        let mut mac = self.config.mac.clone();

        mac.update(iso_date.as_ref());
        mac.update(req.method().as_str().as_bytes());

        if let Some(pq) = req.uri().path_and_query() {
            mac.update(pq.as_str().as_bytes());
        }
        mac.update(req.body().as_bytes());

        let sign = mac.finalize_reset().into_bytes();

        let headers = req.headers_mut();

        headers.insert(
            HeaderName::from_static("ok-access-key"),
            HeaderValue::from_str(&self.config.api_key).map_err(DriverError::HeaderValueError)?,
        );

        headers.insert(
            HeaderName::from_static("ok-access-passphrase"),
            HeaderValue::from_str(&self.config.password).map_err(DriverError::HeaderValueError)?,
        );

        headers.insert(
            HeaderName::from_static("ok-access-sign"),
            HeaderValue::from_str(&base64_wrapper::base64_encode(sign))
                .map_err(DriverError::HeaderValueError)?,
        );

        headers.insert(
            HeaderName::from_static("ok-access-timestamp"),
            HeaderValue::from_str(&iso_date).map_err(DriverError::HeaderValueError)?,
        );

        if self.config.use_testnet {
            headers.insert(
                HeaderName::from_static("x-simulated-trading"),
                HeaderValue::from(1),
            );
        }

        Ok(req)
    }

    fn parse_call_error(&self, status: StatusCode, v: &[u8]) -> serde_json::Result<DriverError> {
        Ok(DriverError::Generic(format!(
            "Failed to parse call response (code: {status}): {:?}",
            std::str::from_utf8(v)
        )))
    }

    async fn get<T: Serialize + Send + Sync, U: DeserializeOwned>(
        &self,
        path: &str,
        params: Option<&T>,
    ) -> DriverResult<U> {
        let mut url = format!("{}{}", self.base_url(), path);
        let mut url_params = vec![];

        if self.config.use_testnet {
            url_params.push("brokerId=9999".to_owned())
        }

        if let Some(params) = params {
            url_params.push(serde_qs::to_string(params)?);
        }

        if !url_params.is_empty() {
            url = url + "?" + &url_params.join("&")[..];
        }

        let req = Request::get(url).body(String::new())?;
        self.call(req).await
    }
}

impl OkexClient {
    pub(crate) async fn get_position_mode(&self) -> DriverResult<OkexPositionMode> {
        self.get::<_, OkexRestResponse<Vec<OkexAccountConfig>>>(
            "/api/v5/account/config",
            Option::<&()>::None,
        )
        .await?
        .validate()?
        .and_then(|mut res| res.pop())
        .map(|config| config.pos_mode)
        .ok_or_else(|| DriverError::parse_failure("No config response"))
    }

    pub(crate) async fn set_position_mode(
        &self,
        position_mode: OkexPositionMode,
    ) -> DriverResult<()> {
        debug!("Set position mode {position_mode:?}");

        let position_mode_res = self
            .post::<_, OkexRestResponse<Vec<OkexPositionMode>>>(
                "/api/v5/account/set-position-mode",
                &position_mode,
            )
            .await?
            .validate()?
            .and_then(|mut res| res.pop())
            .ok_or_else(|| DriverError::parse_failure("No set-position-mode response"))?;

        if position_mode_res != position_mode {
            return Err(DriverError::Generic(format!(
                "Failed to set position mode {position_mode:?}. Response: {position_mode_res:?}",
            )));
        }

        Ok(())
    }

    pub(super) async fn rest_fetch_account_bills(
        &self,
    ) -> DriverResult<Vec<OkexBillResponse>> {
        debug!("Fetching account bills");

        let bills_response = self
            .get::<_, OkexRestResponse<Vec<OkexBillResponse>>>(
                "/api/v5/account/bills",
                Option::<&()>::None,
            )
            .await?
            .validate()?
            .ok_or_else(|| DriverError::parse_failure("No bill response"))?;

        Ok(bills_response)
    }

    pub(super) async fn rest_fetch_open_orders(&self) -> DriverResult<Vec<OkexPendingOrder>> {
        debug!("Fetching open orders");

        // hold one page without re-allocations
        let mut orders = Vec::with_capacity(FETCH_OPEN_ORDERS_COUNT);
        let mut params = BTreeMap::new();
        params.insert(
            "instType",
            self.instrument_converter.instrument_type.to_string().into(),
        );

        loop {
            let pending_orders = self
                .get::<_, OkexRestResponse<Vec<OkexPendingOrder>>>(
                    "/api/v5/trade/orders-pending",
                    Some(&params),
                )
                .await?
                .validate()?
                .unwrap_or_default();

            let fetched_count = pending_orders.len();

            orders.extend(pending_orders);

            if fetched_count < FETCH_OPEN_ORDERS_COUNT {
                break;
            }

            if let Some(last_order) = orders.last() {
                params.insert("before", Value::from(last_order.order_id.0.clone()));
            }
        }

        debug!("Orders fetched: {:?}", orders.len());

        Ok(orders)
    }

    pub(super) async fn rest_fetch_balances(&self) -> DriverResult<Vec<OkexBalance>> {
        debug!("Fetching balances");

        let balances = self
            .get::<_, OkexRestResponse<Vec<OkexBalances>>>(
                "/api/v5/account/balance",
                Option::<&()>::None,
            )
            .await?
            .validate()?
            .unwrap_or_default()
            .pop()
            .ok_or_else(|| DriverError::balance_error("Failed fo fetch balances"))?
            .details;

        debug!("Balances fetched: {:?}", balances);

        Ok(balances)
    }

    pub(super) async fn rest_fetch_trades(
        &self,
        inst_id: Option<OkexInstrumentId>,
    ) -> DriverResult<Vec<TransactionResult>> {
        debug!("Fetching trades");

        let mut params = BTreeMap::new();

        if let Some(inst_id) = inst_id {
            params.insert("instId", Value::from(inst_id.0));
        }

        params.insert(
            "instType",
            self.instrument_converter.instrument_type.to_string().into(),
        );

        // hold one page without re-allocations
        let mut trades: Vec<TransactionResult> = Vec::with_capacity(FETCH_RECENT_TRADES_COUNT);

        loop {
            if let Some(trade) = trades.last() {
                params.insert("after", Value::from(trade.bill_id.clone()));
            }

            let fetched_trades = self
                .get::<_, OkexRestResponse<Vec<TransactionResult>>>(
                    "/api/v5/trade/fills",
                    Some(&params),
                )
                .await?
                .validate()?
                .unwrap_or_default();

            let fetched_count = fetched_trades.len();

            trades.extend(fetched_trades);

            if fetched_count < FETCH_RECENT_TRADES_COUNT {
                break;
            }
        }

        debug!("Trades fetched: {:?}", trades.len());

        Ok(trades)
    }

    pub(super) async fn rest_cancel_order_by_id(
        &self,
        order_id: OrderId,
        inst_id: OkexInstrumentId,
    ) -> DriverResult<()> {
        let res = self
            .post::<_, OkexRestResponse<Vec<OrderResult>>>(
                "/api/v5/trade/cancel-order",
                &json!({
                  "instId": inst_id.0,
                  "ordId": order_id.0
                }),
            )
            .await?;

        match res {
            OkexRestResponse {
                data: Some(mut order_results),
                ..
            } => order_results
                .pop()
                // if exchange unexpectedly responses with no order information in response
                .ok_or_else(|| {
                    DriverError::generic("Unexpected no order result in cancel order response")
                })?
                // validate if order was cancelled (errors if already cancelled/not exist/already filled)
                .validate(),
            // if exchange unexpectedly responses with no data (optionally could have an error message)
            OkexRestResponse { msg, .. } => Err(DriverError::generic(format!(
                "Unexpected empty cancel order response: {:?}",
                msg
            ))),
        }
    }

    /// Cancel orders with REST request
    /// Returns cancelled orders ids
    pub(super) async fn rest_cancel_orders(
        &self,
        order_ids: &[OrderId],
        inst_id: OkexInstrumentId,
    ) -> DriverResult<Vec<OrderId>> {
        // early return if no orders ids to cancel
        if order_ids.is_empty() {
            return Ok(vec![]);
        }

        let requests = order_ids.chunks(CANCEL_ORDERS_BATCH_COUNT).map(|chunk| {
            let params = chunk
                .iter()
                .map(|order_id| {
                    json!({
                      "instId": inst_id.0,
                      "ordId": order_id.0
                    })
                })
                .collect::<Vec<_>>();

            async move {
                self.post::<_, OkexRestResponse<Vec<OrderResult>>>(
                    "/api/v5/trade/cancel-batch-orders",
                    &params,
                )
                .await
            }
        });

        let cancelled_order_ids = future::join_all(requests)
            .await
            .into_iter()
            .filter_map(|res| match res {
                Ok(OkexRestResponse {
                    data: Some(order_results),
                    ..
                }) => Some(order_results.into_iter().filter_map(|order_result| {
                    match order_result.validate() {
                        Ok(_) => Some(order_result.order_id),
                        Err(
                            DriverError::OrderNotFound
                            | DriverError::OrderAlreadyCancelled
                            | DriverError::OrderAlreadyFilled,
                        ) => Some(order_result.order_id),
                        _ => None,
                    }
                })),
                Err(e) => {
                    error!("Failed to batch cancel orders: {:?}", e);
                    None
                }
                _ => {
                    warn!("Unexpected empty batch cancel orders result");
                    None
                }
            })
            .flatten()
            .collect::<Vec<_>>();

        Ok(cancelled_order_ids)
    }
}
