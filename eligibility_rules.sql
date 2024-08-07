CREATE TABLE eligibility_rules (
  id STRING(36),
  accept_discounted BOOL,
  accept_multiple_returns BOOL,
  created_at TIMESTAMP,
  ineligible_categories ARRAY<STRING(2000)>,
  ineligible_tags ARRAY<STRING(2000)>,
  return_window INT64,
  shop_id STRING(36),
  updated_at TIMESTAMP,
  return_window_base_on STRING(MAX),
  return_window_base_on_fulfillment_days INT64,
  return_window_base_on_fulfillment_updated_at TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
  return_window_base_on_fulfillment_updated_by STRING(MAX),
  return_window_base_on_delivery_days INT64,
  return_window_base_on_delivery_updated_at TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
  return_window_base_on_delivery_updated_by STRING(MAX),
  organization_id STRING(32),
  limit_single_item_per_return_enabled BOOL,
  limit_single_item_per_return_quantity_exactly_one BOOL,
  prevent_unfulfilled_orders_enabled BOOL,
  prevent_unfulfilled_orders_enabled_updated_at TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
  bundle_line_item_eligibility STRING(256),
  limit_items_with_common_routing_rule_enabled BOOL,
  product_limit_enabled BOOL,
) PRIMARY KEY(id);
CREATE UNIQUE INDEX eligibility_rules_by_organization_id_a_u ON eligibility_rules(organization_id);
CREATE UNIQUE INDEX index_eligibility_rules_shop_id ON eligibility_rules(shop_id);
