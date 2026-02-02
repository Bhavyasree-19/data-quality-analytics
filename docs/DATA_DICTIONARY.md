# Data Dictionary (E-commerce)

## customers (SQLite)
| Field | Type | Description |
| --- | --- | --- |
| customer_id | int | Surrogate customer identifier |
| full_name | string | Customer full name |
| email | string | Primary email address |
| age | int | Age in years |
| status | string | Account status (active/inactive/suspended) |
| created_at | date | Account creation date |
| country | string | Country code |
| state | string | State/region code |
| signup_channel | string | Acquisition channel |
| kyc_status | string | KYC verification state |
| gdpr_consent | bool | GDPR consent flag |
| source_system | string | Source system of record |

## orders (SQLite)
| Field | Type | Description |
| --- | --- | --- |
| order_id | int | Order identifier |
| customer_id | int | FK to customers |
| order_total | float | Order total value |
| order_date | date | Order date |
| currency | string | Currency code |
| payment_method | string | Payment method |
| order_status | string | Order lifecycle status |
| discount_pct | float | Discount percentage (0-0.5) |
| tax_amount | float | Tax amount |
| shipping_amount | float | Shipping charge |
| channel | string | Sales channel |
| source_system | string | Source system of record |

## web_events (JSONL)
| Field | Type | Description |
| --- | --- | --- |
| event_id | string | Event identifier |
| customer_id | int | Related customer |
| event_type | string | Behavioral event type |
| event_ts | datetime | Event timestamp |
| session_length_sec | int | Session length in seconds |
| device_type | string | Device category |
| browser | string | Browser name |
| region | string | Geographic region |
| referrer | string | Traffic source |
| is_bot | bool | Bot traffic flag |
