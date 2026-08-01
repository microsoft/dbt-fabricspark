select
    1 as order_id,
    array('blue', 'green') as tag_list,
    map('priority', 1, 'retries', 0) as attributes,
    named_struct('city', 'Melbourne', 'postcode', 3000) as shipping_address,
    array(named_struct('sku', 'A-1', 'qty', 2)) as line_items
