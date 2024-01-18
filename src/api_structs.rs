use klp_types::{
    exchange::FuturesState,
    instrument::{asset::Asset, kind::InstrumentKind, Instrument},
    Liquidity,
};
use rust_decimal::prelude::*;
use serde::{
    de::{self},
    Deserialize, Serialize,
};
use serde_json::Value;
use serde_with::{serde_as, DefaultOnError, DisplayFromStr};

use dte_shared::utils;
use dte_traits::{
    prelude::DriverError,
    reporting::{KinesisTransaction, TransactionType},
    RawBalance, RawCollateral,
};

use crate::{ClientOrderId, OrderId, Side, TradeId};

#[derive(Debug, Hash, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct OkexInstrumentId(pub String);

impl AsRef<str> for OkexInstrumentId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub(super) struct RequestId(pub String);

impl RequestId {
    /// Convert from any type that implements ToString
    pub fn from<T: ToString>(s: T) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for RequestId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct OkexInstruments {
    pub data: Vec<OkexInstrument>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum OkexContractType {
    /// Contract value currency is base and currency to settle is quote
    Linear,
    /// Contract value currency is quote and currency to settle is base
    Inverse,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Clone)]
#[serde(tag = "instType", rename_all = "UPPERCASE")]
/// OKX instrument info
/// See more <https://www.okx.com/docs-v5/en/#rest-api-public-data-get-instruments>
pub enum OkexInstrument {
    #[serde(rename = "SWAP")]
    FuturePerpetual {
        #[serde(rename = "settleCcy")]
        settle_asset: Asset,
        #[serde(rename = "ctValCcy")]
        contract_value_asset: Asset,
        #[serde(rename = "instId")]
        instrument_id: OkexInstrumentId,
        #[serde(rename = "ctType")]
        contract_type: OkexContractType,
        #[serde(rename = "ctVal")]
        contract_value: Decimal,
    },
    Spot {
        #[serde(rename = "baseCcy")]
        base: Asset,
        #[serde(rename = "quoteCcy")]
        quote: Asset,
        #[serde(rename = "instId")]
        instrument_id: OkexInstrumentId,
    },
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
/// OKX leverage info
/// See more <https://www.okx.com/docs-v5/en/#rest-api-account-get-leverage>
pub(super) struct OkexLeverage {
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "lever")]
    pub(super) leverage: u8,
    #[serde(rename = "mgnMode")]
    pub(super) margin_mode: OkexTradeMode,
    #[serde(rename = "instId")]
    pub(super) instrument_id: OkexInstrumentId,
    #[serde_as(as = "DefaultOnError")]
    #[serde(rename = "posSide")]
    pub(super) position_side: Option<OkexPositionSide>,
}

#[derive(Debug, Deserialize)]
pub(super) struct OkexRestResponse<T> {
    #[serde(deserialize_with = "utils::parse_str")]
    pub code: u64,
    pub msg: Option<String>,
    pub data: Option<T>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(super) enum WsMessage {
    Event(WsEvent),
    Subscription(WsSubscription),
    RequestResult(WsMethodResponse),
}

#[derive(Debug, Deserialize)]
pub(super) struct WsSubscription {
    pub arg: SubscriptionArg,
    pub data: Value,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub(super) enum WsMethodResponse {
    Order(OrderResponse),
    CancelOrder(OrderResponse),
    BatchCancelOrders(OrderResponse),
}

#[derive(Debug, Deserialize)]
pub(super) struct OrderResponse {
    pub id: RequestId,
    pub data: Vec<OrderResult>,
    #[serde(deserialize_with = "utils::parse_str")]
    pub code: u64,
    pub msg: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
/// OKX newly placed order response
/// See more <https://www.okx.com/docs-v5/en/#rest-api-trade-place-order>
pub struct OrderResult {
    #[serde(rename = "ordId")]
    pub order_id: OrderId,
    #[serde(rename = "clOrdId")]
    pub client_oid: ClientOrderId,
    #[serde(deserialize_with = "utils::parse_str", rename = "sCode")]
    pub code: u64,
    #[serde(rename = "sMsg")]
    pub msg: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event", rename_all = "camelCase")]
pub(super) enum WsEvent {
    Login,
    Subscribe { arg: SubscriptionArg },
    Error(WsMessageError),
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(super) struct OkexBalances {
    pub details: Vec<OkexBalance>,
}

#[serde_as]
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
/// OKX account channel update
/// See more <https://www.okx.com/docs-v5/en/#websocket-api-private-channel-account-channel>
pub(super) struct OkexBalancesUpdate {
    pub details: Vec<OkexBalanceUpdate>,
    #[serde_as(as = "DefaultOnError")]
    #[serde(rename = "imr")]
    pub exchange_initial_margin: Decimal,
    #[serde_as(as = "DefaultOnError")]
    #[serde(rename = "adjEq")]
    pub total_collateral: Decimal,
    #[serde_as(as = "DefaultOnError")]
    pub notional_usd: Decimal,
    #[serde_as(as = "DefaultOnError")]
    #[serde(rename = "mmr")]
    pub total_maintenance_margin: Decimal,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
/// OKX transaction details
/// See more <https://www.okx.com/docs-v5/en/#rest-api-trade-get-transaction-details-last-3-days>
pub(super) struct TransactionResult {
    #[serde(rename = "instId")]
    pub instrument_id: OkexInstrumentId,
    pub trade_id: TradeId,
    #[serde(rename = "ordId")]
    pub order_id: OrderId,
    pub bill_id: String,
    #[serde(rename = "fillPx")]
    pub price: Decimal,
    #[serde(rename = "fillSz")]
    pub filled_amount: Decimal,
    pub side: Side,
    #[serde(rename = "execType", deserialize_with = "deserialize_to_liquidity")]
    pub liquidity: Liquidity,
    #[serde(rename = "feeCcy")]
    pub fee_currency: String,
    pub fee: Decimal,
    #[serde(rename = "ts", deserialize_with = "utils::parse_str")]
    pub created_at: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
/// OKX incomplete order info
/// See more <https://www.okx.com/docs-v5/en/#rest-api-trade-get-order-list>
pub(super) struct OkexPendingOrder {
    #[serde(rename = "instId")]
    pub instrument_id: OkexInstrumentId,
    #[serde(rename = "ordId")]
    pub order_id: OrderId,
    #[serde(rename = "clOrdId")]
    pub client_order_id: ClientOrderId,
    #[serde(rename = "px")]
    pub price: Decimal,
    #[serde(rename = "sz")]
    pub amount: Decimal,
    pub side: Side,
    #[serde(rename = "cTime", deserialize_with = "utils::parse_str")]
    pub created_at: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum OrderState {
    Canceled,
    Live,
    PartiallyFilled,
    Filled,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
/// OKX order update
/// See more <https://www.okx.com/docs-v5/en/#websocket-api-private-channel-order-channel>
pub(super) struct OkexOrderUpdate {
    #[serde(rename = "instId")]
    pub instrument_id: OkexInstrumentId,
    pub trade_id: Option<TradeId>,
    #[serde(rename = "ordId")]
    pub order_id: OrderId,
    #[serde(rename = "clOrdId")]
    pub client_order_id: ClientOrderId,
    #[serde(rename = "px", deserialize_with = "utils::parse_opt_str")]
    pub price: Option<Decimal>, // price is provided only for limit,post_only,fok,ioc orders
    #[serde(rename = "sz")]
    pub amount: Decimal,
    #[serde(rename = "fillPx", deserialize_with = "utils::parse_opt_str")]
    pub filled_price: Option<Decimal>,
    #[serde(rename = "fillSz", deserialize_with = "utils::parse_opt_str")]
    pub filled_amount: Option<Decimal>,
    pub side: Side,
    #[serde(rename = "uTime", deserialize_with = "utils::parse_str")]
    pub updated_at: u64,
    #[serde(rename = "cTime", deserialize_with = "utils::parse_str")]
    pub created_at: u64,
    #[serde(rename = "fillTime", deserialize_with = "utils::parse_opt_str")]
    pub filled_at: Option<u64>,
    pub state: OrderState,
    #[serde(rename = "fillFee", deserialize_with = "utils::parse_opt_str")]
    pub fee: Option<Decimal>,
    #[serde(rename = "fillFeeCcy")]
    pub fee_currency: Option<String>,
    #[serde(rename = "execType", deserialize_with = "deserialize_to_liquidity")]
    pub liquidity: Liquidity,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OkexBalanceAndPositions {
    #[serde(rename = "eventType")]
    pub _event_type: OkexEventType,
    pub bal_data: Vec<OkexBalance>,
    pub pos_data: Vec<OkexPosition>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(super) enum OkexPositionSide {
    Long,
    Short,
    Net,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum OkexEventType {
    Snapshot,
    Filled,
    #[serde(other)]
    Update,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
/// OKX position info
/// See more <https://www.okx.com/docs-v5/en/#rest-api-account-get-positions>
pub(super) struct OkexPosition {
    #[serde(rename = "instId")]
    pub instrument_id: OkexInstrumentId,
    #[serde(rename = "posSide")]
    pub position_side: OkexPositionSide,
    #[serde(rename = "pos")]
    pub amount: Decimal,
    #[serde(rename = "avgPx")]
    pub average_price: Decimal,
}

pub fn deserialize_to_liquidity<'de, D>(deserializer: D) -> Result<Liquidity, D::Error>
where
    D: de::Deserializer<'de>,
{
    let data: String = Deserialize::deserialize(deserializer)?;

    match data.as_str() {
        "M" => Ok(Liquidity::Maker),
        "T" => Ok(Liquidity::Taker),
        _ => Ok(Liquidity::Unknown),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OkexBalance {
    #[serde(rename = "ccy")]
    pub asset: Asset,
    pub cash_bal: Option<Decimal>,
    pub avail_bal: Option<Decimal>,
}

#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OkexBalanceUpdate {
    #[serde(rename = "ccy")]
    pub asset: Asset,
    #[serde_as(as = "DefaultOnError")]
    #[serde(rename = "upl")]
    pub unrealized_pnl: Option<Decimal>,
    pub cash_bal: Option<Decimal>,
    pub avail_bal: Option<Decimal>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "channel", rename_all = "snake_case")]
pub(super) enum SubscriptionArg {
    Orders(OrdersArg),
    Account(AccountArg),
    BalanceAndPosition,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrdersArg {
    #[serde(rename = "instId")]
    pub instrument_id: OkexInstrumentId,
    #[serde(rename = "instType")]
    pub instrument_type: OkexInstrumentType,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct AccountArg {
    #[serde(rename = "ccy", skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub(super) enum OkexInstrumentType {
    Spot,
    Margin,
    Swap,
    Futures,
    Option,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Copy, Clone)]
#[serde(tag = "posMode", rename_all = "snake_case")]
pub(crate) enum OkexPositionMode {
    /// In long/short mode, users can hold positions in long and short directions.
    LongShortMode,
    /// In net mode, users can only have positions in one direction
    NetMode,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OkexAccountConfig {
    #[serde(flatten)]
    pub pos_mode: OkexPositionMode,
}

impl Default for OkexPositionMode {
    fn default() -> Self {
        Self::NetMode
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub(super) enum OkexTradeMode {
    Cash,
    Isolated,
    Cross,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum SubscriptionChannel {
    Orders,
    Account,
}

#[derive(Debug, Deserialize)]
pub(super) struct WsMessageError {
    #[allow(unused)]
    pub code: String,
}

impl OkexInstrument {
    pub(super) fn id(&self) -> OkexInstrumentId {
        match self {
            OkexInstrument::Spot { instrument_id, .. } => instrument_id.clone(),
            OkexInstrument::FuturePerpetual { instrument_id, .. } => instrument_id.clone(),
        }
    }

    /// Converts internal order amount to exchange size
    pub(super) fn to_exchange_size(&self, amount: Decimal, price: Decimal) -> Option<Decimal> {
        match self {
            OkexInstrument::Spot { .. } => Some(amount),
            OkexInstrument::FuturePerpetual {
                contract_value,
                contract_type,
                ..
            } => match contract_type {
                OkexContractType::Linear => {
                    if contract_value.is_zero() {
                        None
                    } else {
                        Some(amount / contract_value)
                    }
                }
                OkexContractType::Inverse => {
                    if contract_value.is_zero() {
                        None
                    } else {
                        Some(amount * price / contract_value)
                    }
                }
            },
        }
    }

    /// Converts exchange order size to internal amount
    pub(super) fn to_internal_amount(&self, size: Decimal, price: Decimal) -> Option<Decimal> {
        match self {
            OkexInstrument::Spot { .. } => Some(size),
            OkexInstrument::FuturePerpetual {
                contract_value,
                contract_type,
                ..
            } => match contract_type {
                OkexContractType::Linear => Some(size * contract_value),
                OkexContractType::Inverse => {
                    if price.is_zero() {
                        None
                    } else {
                        Some((size * contract_value) / price)
                    }
                }
            },
        }
    }
}

impl std::cmp::PartialEq<OkexInstrument> for Instrument {
    fn eq(&self, instrument: &OkexInstrument) -> bool {
        match instrument {
            OkexInstrument::Spot { base, quote, .. }
                if self.kind == InstrumentKind::Spot
                    && base == &self.base
                    && quote == &self.quote =>
            {
                true
            }
            OkexInstrument::FuturePerpetual {
                contract_type: OkexContractType::Linear,
                contract_value_asset,
                settle_asset,
                ..
            } if self.kind == InstrumentKind::FuturePerpetual
                && contract_value_asset == &self.base
                && settle_asset == &self.quote =>
            {
                true
            }
            OkexInstrument::FuturePerpetual {
                contract_type: OkexContractType::Inverse,
                contract_value_asset,
                settle_asset,
                ..
            } if self.kind == InstrumentKind::FuturePerpetual
                && settle_asset == &self.base
                && contract_value_asset == &self.quote =>
            {
                true
            }
            _ => false,
        }
    }
}

impl From<&OkexInstrument> for OrdersArg {
    fn from(instrument: &OkexInstrument) -> Self {
        match instrument {
            OkexInstrument::Spot { instrument_id, .. } => Self {
                instrument_id: instrument_id.clone(),
                instrument_type: OkexInstrumentType::Spot,
            },
            OkexInstrument::FuturePerpetual { instrument_id, .. } => Self {
                instrument_id: instrument_id.clone(),
                instrument_type: OkexInstrumentType::Swap,
            },
        }
    }
}

impl std::fmt::Display for OkexInstrumentType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let instrument_type = match self {
            Self::Spot => "SPOT",
            Self::Margin => "MARGIN",
            Self::Swap => "SWAP",
            Self::Futures => "FUTURES",
            Self::Option => "OPTION",
        };

        write!(f, "{instrument_type}")
    }
}

impl From<OkexBalance> for RawBalance {
    fn from(balance: OkexBalance) -> Self {
        let total = balance.cash_bal.unwrap_or_default();
        let free = balance.avail_bal.unwrap_or_default();
        Self {
            symbol: balance.asset.to_string(),
            free,
            used: total - free,
            total,
            last_updated: dte_shared::utils::timestamp(),
        }
    }
}

impl From<OkexBalance> for RawCollateral {
    fn from(balance: OkexBalance) -> Self {
        Self {
            symbol: balance.asset.to_string(),
            free: balance.cash_bal.unwrap_or_default(),
            used: Decimal::default(),
            total: balance.cash_bal.unwrap_or_default(),
        }
    }
}

impl From<(OkexPositionMode, Side)> for OkexPositionSide {
    fn from((pos_mode, side): (OkexPositionMode, Side)) -> Self {
        match side {
            Side::Buy if pos_mode == OkexPositionMode::LongShortMode => Self::Long,
            Side::Sell if pos_mode == OkexPositionMode::LongShortMode => Self::Short,
            _ => Self::Net,
        }
    }
}

impl TryFrom<&OkexBalancesUpdate> for FuturesState {
    type Error = DriverError;

    fn try_from(update: &OkexBalancesUpdate) -> Result<Self, Self::Error> {
        // Decimal to f64 conversion error that is shared by many of the below computations.
        let decimal_to_f64_conversion_error_message = "Unable to convert Decimal to f64";

        let total_position_notional = update
            .notional_usd
            .to_f64()
            .ok_or_else(|| DriverError::generic(decimal_to_f64_conversion_error_message))?;

        let total_collateral = update
            .total_collateral
            .to_f64()
            .ok_or_else(|| DriverError::generic(decimal_to_f64_conversion_error_message))?;

        let available_collateral = update
            .total_collateral
            .saturating_sub(update.exchange_initial_margin)
            .to_f64()
            .ok_or_else(|| DriverError::generic(decimal_to_f64_conversion_error_message))?;

        let unrealized_pnl = update
            .details
            .iter()
            .fold(Decimal::default(), |total_upl, update| {
                total_upl + update.unrealized_pnl.unwrap_or_default()
            })
            .to_f64()
            .ok_or_else(|| DriverError::generic(decimal_to_f64_conversion_error_message))?;

        let total_maintenance_margin = update
            .total_maintenance_margin
            .to_f64()
            .ok_or_else(|| DriverError::generic(decimal_to_f64_conversion_error_message))?;

        let maintenance_margin_ratio = (total_maintenance_margin / total_collateral).min(1.);

        let exchange_initial_margin = update
            .exchange_initial_margin
            .to_f64()
            .ok_or_else(|| DriverError::generic(decimal_to_f64_conversion_error_message))?;

        // Note that we take the absolute value of the total position notional size here to avoid a
        // negative margin fraction value
        let margin_fraction = Some((total_collateral / total_position_notional).min(1.));

        let open_margin_fraction = Some((total_collateral / exchange_initial_margin).min(1.));

        Ok(Self {
            total_collateral,
            available_collateral,
            unrealized_pnl,
            maintenance_margin_ratio,
            position_initial_margin: None,
            open_order_initial_margin: None,
            exchange_initial_margin,
            margin_fraction,
            open_margin_fraction,
        })
    }
}

impl From<OkexBalancesUpdate> for Vec<RawBalance> {
    fn from(updates: OkexBalancesUpdate) -> Self {
        updates
            .details
            .into_iter()
            .map(|balance| {
                let total = balance.cash_bal.unwrap_or_default();
                let free = balance.avail_bal.unwrap_or_default();
                RawBalance {
                    symbol: balance.asset.to_string(),
                    total,
                    free,
                    used: total - free,
                    last_updated: dte_shared::utils::timestamp(),
                }
            })
            .collect()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OkexBillsRequest {
    #[serde(rename = "instType")]
    pub instrument_type: Option<OkexInstrumentType>,
    #[serde(rename = "ccy", skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[allow(non_snake_case)]
    pub type_: Option<OkexBillType>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) enum OkexBillType {
    Transfer,
    Trade,
    MarginTransfer,
    FundingFee,
    Other(String),
}

impl From<OkexBillType> for TransactionType {
    fn from(value: OkexBillType) -> Self {
        match value {
            OkexBillType::Trade => TransactionType::Trade,
            OkexBillType::Transfer => TransactionType::Transfer,
            OkexBillType::FundingFee => TransactionType::FundingFee,
            OkexBillType::MarginTransfer => TransactionType::MarginFee,
            _ => TransactionType::Other(format!("{:?}", value)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OkexBillResponse {
    #[allow(non_snake_case)]
    #[serde(deserialize_with = "utils::parse_str")]
    pub type_: u64,
    #[serde(deserialize_with = "utils::parse_str")]
    pub ts: u64,
    #[serde(rename = "sz", with = "rust_decimal::serde::str")]
    pub amount: Decimal,
    #[serde(rename = "px", with = "rust_decimal::serde::str_option")]
    pub price: Option<Decimal>,
    #[serde(rename = "ccy")]
    pub currency: String,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub fee: Option<Decimal>,
    #[serde(rename = "instId")]
    pub instrument_id: String,
    #[serde(rename = "ordId")]
    pub order_id: Option<String>,
    #[serde(rename = "execType", deserialize_with = "deserialize_to_liquidity")]
    pub liquidity: Liquidity,
    #[serde(rename = "fillTime", deserialize_with = "utils::parse_str")]
    pub updated_at: u64,
    #[serde(rename = "tradeId")]
    pub trade_id: Option<TradeId>,
}

impl OkexBillResponse {
    pub async fn to_kinesis_transaction(
        &self,
        exchange: &str,
        bot_id: String,
        operation: String,
    ) -> KinesisTransaction {
        KinesisTransaction {
            bot_id,
            exchange: exchange.to_string(),
            symbol: self.instrument_id.clone(),
            trade_id: self.trade_id.as_ref().unwrap().to_string(),
            order_id: self.order_id.as_ref().unwrap().to_string(),
            side: if self.fee.unwrap_or_default().is_sign_negative() {
                Side::Sell
            } else {
                Side::Buy
            },
            price: self.price.unwrap_or_default(),
            fee: self.fee,
            fee_currency: Some(self.currency.clone()),
            amount: self.amount,
            filled_amount: Decimal::ZERO,
            liquidity: self.liquidity,
            date: self.updated_at,
            created_at: self.ts,
            base_currency: self.currency.clone(),
            quote_currency: self.currency.clone(),
            operation,
            level_id: String::new(),
            transaction_type: OkexBillType::FundingFee.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_err_rest_response() {
        let msg = "{\"msg\":\"Invalid Sign\",\"code\":\"50113\"}";
        match serde_json::from_str::<OkexRestResponse<OkexBalances>>(msg) {
            Ok(_) => {}
            _ => panic!("Failed to parse rest response error"),
        }
    }

    #[test]
    fn test_parse_bills() {
        let msg: &str = r#"{"code": "0","msg": "","data": [{"bal": "8694.2179403378290202","balChg": "0.0219338232210000","billId": "623950854533513219","ccy": "USDT","clOrdId": "","execType": "T","fee": "-0.000021955779","fillFwdPx": "","fillIdxPx": "27104.1","fillMarkPx": "","fillMarkVol": "","fillPxUsd": "","fillPxVol": "","fillTime": "1695033476166","from": "","instId": "BTC-USDT","instType": "SPOT","interest": "0","mgnMode": "isolated","notes": "","ordId": "623950854525124608","pnl": "0","posBal": "0","posBalChg": "0","px": "27105.9","subType": "1","sz": "0.021955779","tag": "","to": "","tradeId": "586760148","ts": "1695033476167","type": "2"}]}"#;
        match serde_json::from_str::<OkexRestResponse<Vec<OkexBillResponse>>>(msg) {
            Ok(result) => {
                println!("Parsed result: {:?}", result);
            }
            Err(err) => {
                eprintln!("Failed to parse bills: {:?}", err);
                panic!("Failed to parse bills");
            }
        }
    }

    #[test]
    fn test_parse_balances() {
        let msg = r#"{"code":"0","data":[{"adjEq":"","details":[{"availBal":"91.99467489","availEq":"","cashBal":"91.99467489","ccy":"USDT","crossLiab":"","disEq":"92.00203446399121","eq":"91.99467489","eqUsd":"92.00203446399121","frozenBal":"0","interest":"","isoEq":"","isoLiab":"","isoUpl":"","liab":"","maxLoan":"","mgnRatio":"","notionalLever":"","ordFrozen":"0","stgyEq":"0","twap":"0","uTime":"1622638786358","upl":"","uplLiab":""},{"availBal":"0.1233","availEq":"","cashBal":"0.1233","ccy":"LTC","crossLiab":"","disEq":"30.418788150000005","eq":"0.1233","eqUsd":"32.019777000000005","frozenBal":"0","interest":"","isoEq":"","isoLiab":"","isoUpl":"","liab":"","maxLoan":"","mgnRatio":"","notionalLever":"","ordFrozen":"0","stgyEq":"0","twap":"0","uTime":"1622638786358","upl":"","uplLiab":""},{"availBal":"0.0013","availEq":"","cashBal":"0.0013","ccy":"ETH","crossLiab":"","disEq":"5.949827","eq":"0.0013","eqUsd":"5.949827","frozenBal":"0","interest":"","isoEq":"","isoLiab":"","isoUpl":"","liab":"","maxLoan":"","mgnRatio":"","notionalLever":"","ordFrozen":"0","stgyEq":"0","twap":"0","uTime":"1620309530210","upl":"","uplLiab":""}],"imr":"","isoEq":"","mgnRatio":"","mmr":"","notionalUsd":"","ordFroz":"","totalEq":"129.9716384639912","uTime":"1636729644862"}],"msg":""}"#;
        match serde_json::from_str::<OkexRestResponse<Vec<OkexBalances>>>(msg) {
            Ok(_) => {}
            _ => panic!("Failed to parse balances"),
        }
    }

    #[test]
    fn test_parse_notification_empty_balances() {
        let msg = r#"{"arg":{"channel":"account","ccy":"USDC","uid":"77812899493584896"},"data":[{"adjEq":"","details":[],"imr":"","isoEq":"","mgnRatio":"","mmr":"","notionalUsd":"","ordFroz":"","totalEq":"126.27024760020271","uTime":"1637152397731"}]}"#;
        let err_text = "Failed to parse balances notification";
        match serde_json::from_str::<WsMessage>(msg) {
            Ok(WsMessage::Subscription(WsSubscription {
                arg: SubscriptionArg::Account(_),
                data,
            })) => match serde_json::from_value::<Vec<OkexBalancesUpdate>>(data) {
                Ok(mut balances) => match balances.pop().map(Vec::<RawBalance>::from) {
                    Some(balances) if balances.is_empty() => Ok(()),
                    _ => Err(err_text),
                },
                _ => Err(err_text),
            },
            _ => Err(err_text),
        }
        .expect("Expect empty balances");
    }

    #[test]
    fn test_parse_notification_invalid_balance() {
        let msg =
            r#"{"arg":{"channel":"account","ccy":"USDC","uid":"77812899493584896"},"data":[]}"#;
        let err_text = "Failed to parse balances notification";
        let _ = match serde_json::from_str::<WsMessage>(msg) {
            Ok(WsMessage::Subscription(WsSubscription {
                arg: SubscriptionArg::Account(_),
                data,
            })) => match serde_json::from_value::<Vec<OkexBalancesUpdate>>(data) {
                Ok(balances) if balances.is_empty() => Err("Empty balance update"),
                _ => Ok(()),
            },
            _ => Err(err_text),
        }
        .expect_err("Expect an error");
    }

    #[test]
    fn test_parse_notification_balance() {
        let msg = r#"{"arg":{"channel":"account","ccy":"LTC","uid":"77812899493584896"},"data":[{"adjEq":"","details":[{"availBal":"0.1233","availEq":"","cashBal":"0.1233","ccy":"LTC","coinUsdPrice":"224.89","crossLiab":"","disEq":"26.342490149999996","eq":"0.1233","eqUsd":"27.728937","frozenBal":"0","interest":"","isoEq":"","isoLiab":"","isoUpl":"","liab":"","maxLoan":"","mgnRatio":"","notionalLever":"","ordFrozen":"0","stgyEq":"0","twap":"0","uTime":"1622638786358","upl":""}],"imr":"","isoEq":"","mgnRatio":"","mmr":"","notionalUsd":"","ordFroz":"","totalEq":"125.28352790793839","uTime":"1637187825715"}]}"#;
        let err_text = "Failed to parse balances notification";
        match serde_json::from_str::<WsMessage>(msg) {
            Ok(WsMessage::Subscription(WsSubscription {
                arg: SubscriptionArg::Account(_),
                data,
            })) => match serde_json::from_value::<Vec<OkexBalancesUpdate>>(data) {
                Ok(mut balances) => balances
                    .pop()
                    .and_then(|balances| {
                        (!Vec::<RawBalance>::from(balances).is_empty()).then_some(())
                    })
                    .ok_or("Empty balance update"),
                _ => Err(err_text),
            },
            _ => Err(err_text),
        }
        .expect("Expect balance");
    }

    #[test]
    fn test_parse_notification_orders() {
        let msg = r#"{"arg":{"channel":"orders","instType":"SPOT","instId":"ETH-USDT","uid":"77812899493584896"},"data":[{"accFillSz":"0","amendResult":"","avgPx":"0","cTime":"1638127233488","category":"normal","ccy":"","clOrdId":"","code":"0","execType":"","fee":"0","feeCcy":"USDT","fillFee":"0","fillFeeCcy":"","fillNotionalUsd":"","fillPx":"","fillSz":"0","fillTime":"","instId":"ETH-USDT","instType":"SPOT","lever":"0","msg":"","notionalUsd":"100.098","ordId":"385268773242155008","ordType":"limit","pnl":"0","posSide":"","px":"10000","rebate":"0","rebateCcy":"ETH","reduceOnly":"false","reqId":"","side":"sell","slOrdPx":"","slTriggerPx":"","slTriggerPxType":"","source":"","state":"live","sz":"0.01","tag":"","tdMode":"cash","tgtCcy":"","tpOrdPx":"","tpTriggerPx":"","tpTriggerPxType":"","tradeId":"","uTime":"1638127233488"}]}"#;
        let err_text = String::from("Failed to parse orders notification");
        let _ = match serde_json::from_str::<WsMessage>(msg) {
            Ok(WsMessage::Subscription(WsSubscription {
                arg: SubscriptionArg::Orders(_),
                data,
            })) => serde_json::from_value::<Vec<OkexOrderUpdate>>(data).map_err(|e| e.to_string()),
            _ => Err(err_text),
        }
        .expect("Expect orders");

        let msg = r#"{"arg":{"channel":"orders","instType":"SWAP","instId":"NEAR-USDT-SWAP","uid":"203208194003214336"},"data":[{"accFillSz":"0","algoClOrdId":"","algoId":"","amendResult":"","amendSource":"","avgPx":"0","cTime":"1683050677289","cancelSource":"","category":"normal","ccy":"","clOrdId":"","code":"0","execType":"","fee":"0","feeCcy":"USDT","fillFee":"0","fillFeeCcy":"","fillNotionalUsd":"","fillPx":"","fillSz":"0","fillTime":"","instId":"NEAR-USDT-SWAP","instType":"SWAP","lever":"1","msg":"","notionalUsd":"19.1203194","ordId":"573691353268330498","ordType":"market","pnl":"0","posSide":"net","px":"","quickMgnType":"","rebate":"0","rebateCcy":"USDT","reduceOnly":"true","reqId":"","side":"buy","slOrdPx":"","slTriggerPx":"","slTriggerPxType":"","source":"","state":"live","sz":"1","tag":"","tdMode":"cross","tgtCcy":"","tpOrdPx":"","tpTriggerPx":"","tpTriggerPxType":"","tradeId":"","uTime":"1683050677289"}]}"#;
        let err_text = String::from("Failed to parse orders notification");
        let _ = match serde_json::from_str::<WsMessage>(msg) {
            Ok(WsMessage::Subscription(WsSubscription {
                arg: SubscriptionArg::Orders(_),
                data,
            })) => serde_json::from_value::<Vec<OkexOrderUpdate>>(data).map_err(|e| e.to_string()),
            _ => Err(err_text),
        }
        .expect("Expect orders");
    }

    #[test]
    fn test_parse_order_response() {
        let msg = r#"{"id":"1512","op":"order","data":[{"clOrdId":"12345689","ordId":"12345689","sCode":"0","sMsg":""}],"code":"0","msg":""}"#;
        let err_text = String::from("Failed to parse order response");
        let _ = match serde_json::from_str::<WsMessage>(msg) {
            Ok(WsMessage::RequestResult(WsMethodResponse::Order(res))) => Ok(res.data),
            _ => Err(err_text),
        }
        .expect("Expect order response");
    }

    #[test]
    fn test_parse_bills_response() {
        let msg = r#"{"code": "0","msg": "","data": [{"bal": "8694.2179403378290202","balChg": "0.0219338232210000","billId": "623950854533513219","ccy": "USDT","clOrdId": "","execType": "T","fee": "-0.000021955779","fillFwdPx": "","fillIdxPx": "27104.1","fillMarkPx": "","fillMarkVol": "","fillPxUsd": "","fillPxVol": "","fillTime": "1695033476166","from": "","instId": "BTC-USDT","instType": "SPOT","interest": "0","mgnMode": "isolated","notes": "","ordId": "623950854525124608","pnl": "0","posBal": "0","posBalChg": "0","px": "27105.9","subType": "1","sz": "0.021955779","tag": "","to": "","tradeId": "586760148","ts": "1695033476167","type": "2"}]}"#;
        let err_text = String::from("Failed to parse bills response");
        let _ = match serde_json::from_str::<OkexRestResponse<Vec<OkexBillResponse>>>(msg) {
            Ok(OkexRestResponse {
                code: 0,
                msg: _,
                data: Some(res),
            }) => Ok(res),
            _ => Err(err_text),
        }
        .expect("Expect bills response");
    }

    #[test]
    fn test_parse_leverage() {
        let msg = r#"{"code":"0","data":[{"instId":"BTC-USDT-SWAP","lever":"1","mgnMode":"isolated","posSide":"long"}],"msg":""}"#;
        assert_matches::assert_matches!(
            serde_json::from_str::<OkexRestResponse<Vec<OkexLeverage>>>(msg)
                .unwrap()
                .data
                .unwrap()
                .pop(),
            Some(OkexLeverage {
                instrument_id: OkexInstrumentId(id),
                leverage: 1,
                margin_mode: OkexTradeMode::Isolated,
                position_side: Some(OkexPositionSide::Long),
            }) if &id == "BTC-USDT-SWAP"
        );

        let msg = r#"{"code":"0","data":[{"instId":"BTC-USDT-SWAP","lever":"1","mgnMode":"cross","posSide":""}],"msg":""}"#;
        assert_matches::assert_matches!(
            serde_json::from_str::<OkexRestResponse<Vec<OkexLeverage>>>(msg)
                .unwrap()
                .data
                .unwrap()
                .pop(),
            Some(OkexLeverage {
                instrument_id: OkexInstrumentId(id),
                leverage: 1,
                margin_mode: OkexTradeMode::Cross,
                position_side: None,
            }) if &id == "BTC-USDT-SWAP"
        );
    }

    #[test]
    fn test_internal_amount_spot() {
        let instrument = OkexInstrument::Spot {
            base: "eth".into(),
            quote: "usdt".into(),
            instrument_id: OkexInstrumentId("ETH-USDT".into()),
        };
        let size = Decimal::from_f64(0.0001).unwrap();
        let price = Decimal::from_f64(1671.21).unwrap();
        let amount = instrument.to_internal_amount(size, price).unwrap();
        assert_eq!(amount, Decimal::from_f64(0.0001).unwrap());
    }

    #[test]
    fn test_internal_amount_perp_linear() {
        let instrument = OkexInstrument::FuturePerpetual {
            settle_asset: "usdt".into(),
            contract_value_asset: "eth".into(),
            instrument_id: OkexInstrumentId("ETH-USDT-SWAP".into()),
            contract_type: OkexContractType::Linear,
            contract_value: Decimal::from_f64(0.1).unwrap(),
        };
        let size = Decimal::from_f64(0.0001).unwrap();
        let price = Decimal::from_f64(1671.21).unwrap();
        let amount = instrument.to_internal_amount(size, price).unwrap();
        assert_eq!(amount, Decimal::from_f64(0.00001).unwrap());
    }

    #[test]
    fn test_internal_amount_perp_inverse() {
        let instrument = OkexInstrument::FuturePerpetual {
            settle_asset: "eth".into(),
            contract_value_asset: "usd".into(),
            instrument_id: OkexInstrumentId("ETH-USD-SWAP".into()),
            contract_type: OkexContractType::Inverse,
            contract_value: Decimal::from_f64(10.0).unwrap(),
        };
        let size = Decimal::from_f64(17.0).unwrap();
        let price = Decimal::from_f64(1671.21).unwrap();
        let amount = instrument.to_internal_amount(size, price).unwrap();
        assert_eq!(amount.round_dp(4), Decimal::from_f64(0.1017).unwrap());
    }

    #[test]
    fn test_internal_amount_perp_inverse_zero_division() {
        let instrument = OkexInstrument::FuturePerpetual {
            settle_asset: "eth".into(),
            contract_value_asset: "usd".into(),
            instrument_id: OkexInstrumentId("ETH-USD-SWAP".into()),
            contract_type: OkexContractType::Inverse,
            contract_value: Decimal::from_f64(10.0).unwrap(),
        };
        let size = Decimal::from_f64(17.0).unwrap();
        let price = Decimal::from_f64(0.0).unwrap();
        let amount = instrument.to_internal_amount(size, price);
        assert_eq!(amount, None);
    }

    #[test]
    fn test_exchange_size_spot() {
        let instrument = OkexInstrument::Spot {
            base: "eth".into(),
            quote: "usdt".into(),
            instrument_id: OkexInstrumentId("ETH-USDT".into()),
        };
        let amount = Decimal::from_f64(0.00001).unwrap();
        let price = Decimal::from_f64(1671.21).unwrap();
        let size = instrument.to_exchange_size(amount, price).unwrap();
        assert_eq!(size, Decimal::from_f64(0.00001).unwrap());
    }

    #[test]
    fn test_exchange_size_perp_linear() {
        let instrument = OkexInstrument::FuturePerpetual {
            settle_asset: "usdt".into(),
            contract_value_asset: "eth".into(),
            instrument_id: OkexInstrumentId("ETH-USDT-SWAP".into()),
            contract_type: OkexContractType::Linear,
            contract_value: Decimal::from_f64(0.1).unwrap(),
        };
        let amount = Decimal::from_f64(0.00001).unwrap();
        let price = Decimal::from_f64(1671.21).unwrap();
        let size = instrument.to_exchange_size(amount, price).unwrap();
        assert_eq!(size, Decimal::from_f64(0.0001).unwrap());
    }

    #[test]
    fn test_exchange_size_perp_linear_zero_division() {
        let instrument = OkexInstrument::FuturePerpetual {
            settle_asset: "usdt".into(),
            contract_value_asset: "eth".into(),
            instrument_id: OkexInstrumentId("ETH-USDT-SWAP".into()),
            contract_type: OkexContractType::Linear,
            contract_value: Decimal::from_f64(0.0).unwrap(),
        };
        let amount = Decimal::from_f64(0.00001).unwrap();
        let price = Decimal::from_f64(1671.21).unwrap();
        let size = instrument.to_exchange_size(amount, price);
        assert_eq!(size, None);
    }

    #[test]
    fn test_exchange_size_perp_inverse() {
        let instrument = OkexInstrument::FuturePerpetual {
            settle_asset: "eth".into(),
            contract_value_asset: "usd".into(),
            instrument_id: OkexInstrumentId("ETH-USD-SWAP".into()),
            contract_type: OkexContractType::Inverse,
            contract_value: Decimal::from_f64(10.0).unwrap(),
        };
        let amount = Decimal::from_f64(0.00001).unwrap();
        let price = Decimal::from_f64(1671.21).unwrap();
        let size = instrument.to_exchange_size(amount, price).unwrap();
        assert_eq!(size.round_dp(6), Decimal::from_f64(0.001671).unwrap());
    }

    #[test]
    fn test_exchange_size_perp_inverse_zero_division() {
        let instrument = OkexInstrument::FuturePerpetual {
            settle_asset: "eth".into(),
            contract_value_asset: "usd".into(),
            instrument_id: OkexInstrumentId("ETH-USD-SWAP".into()),
            contract_type: OkexContractType::Inverse,
            contract_value: Decimal::from_f64(0.0).unwrap(),
        };
        let amount = Decimal::from_f64(0.00001).unwrap();
        let price = Decimal::from_f64(1671.21).unwrap();
        let size = instrument.to_exchange_size(amount, price);
        assert_eq!(size, None);
    }
}
