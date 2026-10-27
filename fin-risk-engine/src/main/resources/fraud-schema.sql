CREATE TABLE IF NOT EXISTS command_lock(id INT PRIMARY KEY);
MERGE INTO command_lock KEY(id) VALUES(1);
CREATE TABLE IF NOT EXISTS users(
 id VARCHAR(80) PRIMARY KEY, status VARCHAR(20) NOT NULL, verified BOOLEAN NOT NULL,
 created_at BIGINT NOT NULL, reset_at BIGINT NOT NULL, typical_cents BIGINT NOT NULL, country VARCHAR(2) NOT NULL);
CREATE TABLE IF NOT EXISTS api_credentials(
 token_hash VARCHAR(64) PRIMARY KEY, user_id VARCHAR(80) NOT NULL, role VARCHAR(20) NOT NULL, device_id VARCHAR(80));
CREATE TABLE IF NOT EXISTS devices(
 id VARCHAR(80) NOT NULL, user_id VARCHAR(80) NOT NULL, trusted BOOLEAN NOT NULL,
 compromised BOOLEAN NOT NULL, PRIMARY KEY(id,user_id));
CREATE TABLE IF NOT EXISTS networks(ip VARCHAR(80) PRIMARY KEY, country VARCHAR(2) NOT NULL, proxy BOOLEAN NOT NULL);
CREATE TABLE IF NOT EXISTS merchants(id VARCHAR(80) PRIMARY KEY, blocked BOOLEAN NOT NULL, provider_mode VARCHAR(40) NOT NULL);
CREATE TABLE IF NOT EXISTS products(
 id VARCHAR(80) PRIMARY KEY, merchant_id VARCHAR(80) NOT NULL, currency VARCHAR(3) NOT NULL,
 price_cents BIGINT NOT NULL CHECK(price_cents>0));
CREATE TABLE IF NOT EXISTS orders(
 id VARCHAR(80) PRIMARY KEY, user_id VARCHAR(80) NOT NULL, merchant_id VARCHAR(80) NOT NULL,
 currency VARCHAR(3) NOT NULL, amount_cents BIGINT NOT NULL CHECK(amount_cents>0),
 status VARCHAR(24) NOT NULL, payment_id VARCHAR(80), created_at BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS order_commands(
 user_id VARCHAR(80) NOT NULL, idem_key VARCHAR(80) NOT NULL, request_hash VARCHAR(64) NOT NULL,
 order_id VARCHAR(80) NOT NULL, PRIMARY KEY(user_id,idem_key));
CREATE TABLE IF NOT EXISTS campaigns(
 id VARCHAR(80) PRIMARY KEY, merchant_id VARCHAR(80) NOT NULL, currency VARCHAR(3) NOT NULL,
 minimum_cents BIGINT NOT NULL, discount_cents BIGINT NOT NULL CHECK(discount_cents>0),
 starts_at BIGINT NOT NULL, expires_at BIGINT NOT NULL, remaining INT NOT NULL CHECK(remaining>=0));
CREATE TABLE IF NOT EXISTS coupons(
 id VARCHAR(80) PRIMARY KEY, user_id VARCHAR(80) NOT NULL, campaign_id VARCHAR(80) NOT NULL,
 device_id VARCHAR(80) NOT NULL, status VARCHAR(24) NOT NULL, payment_id VARCHAR(80), claimed_at BIGINT NOT NULL,
 UNIQUE(user_id,campaign_id), UNIQUE(device_id,campaign_id));
CREATE TABLE IF NOT EXISTS claim_commands(
 user_id VARCHAR(80) NOT NULL, idem_key VARCHAR(80) NOT NULL, request_hash VARCHAR(64) NOT NULL,
 response_json CLOB NOT NULL, PRIMARY KEY(user_id,idem_key));
CREATE TABLE IF NOT EXISTS payments(
 id VARCHAR(80) PRIMARY KEY, user_id VARCHAR(80) NOT NULL, idem_key VARCHAR(80) NOT NULL,
 request_hash VARCHAR(64) NOT NULL, order_id VARCHAR(80) NOT NULL, device_id VARCHAR(80) NOT NULL,
 coupon_id VARCHAR(80), gross_cents BIGINT NOT NULL, discount_cents BIGINT NOT NULL,
 payable_cents BIGINT NOT NULL CHECK(payable_cents>=0), status VARCHAR(24) NOT NULL,
 decision_json CLOB NOT NULL, features_json CLOB NOT NULL, provider_reference VARCHAR(80), created_at BIGINT NOT NULL,
 session_device_id VARCHAR(80),
 UNIQUE(user_id,idem_key));
CREATE INDEX IF NOT EXISTS payments_user_time ON payments(user_id,created_at);
CREATE INDEX IF NOT EXISTS payments_device_time ON payments(device_id,created_at);
CREATE TABLE IF NOT EXISTS wallets(user_id VARCHAR(80) PRIMARY KEY, balance_cents BIGINT NOT NULL CHECK(balance_cents>=0));
CREATE TABLE IF NOT EXISTS provider_ledger(
 operation_key VARCHAR(100) PRIMARY KEY, payment_id VARCHAR(80) NOT NULL, user_id VARCHAR(80) NOT NULL,
 amount_cents BIGINT NOT NULL, outcome VARCHAR(24) NOT NULL, reference_id VARCHAR(80) NOT NULL);
CREATE TABLE IF NOT EXISTS audit_events(
 sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, actor VARCHAR(80) NOT NULL,
 kind VARCHAR(40) NOT NULL, entity_id VARCHAR(80) NOT NULL, detail_json CLOB NOT NULL, happened_at BIGINT NOT NULL);
