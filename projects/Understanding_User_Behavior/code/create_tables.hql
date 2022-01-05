create external table if not exists default.purchase_ingredient ( 
    raw_event string,
    Host string, 
    timestamp string,
    event_type string,
    ingredient_type string,
    gold_cost int
    ) 
    stored as parquet
    location '/tmp/purchase_ingredient'
    tblproperties ("parquet.compress"="SNAPPY");
    
create external table if not exists default.join_restaurant ( 
    raw_event string,
    Host string, 
    timestamp string,
    event_type string,
    restaurant_name string,
    special_ingredient string
    ) 
    stored as parquet
    location '/tmp/join_restaurant'
    tblproperties ("parquet.compress"="SNAPPY");
    
create external table if not exists default.enter_contest ( 
    raw_event string,
    Host string, 
    timestamp string,
    event_type string,
    contest string,
    outcome string
    ) 
    stored as parquet
    location '/tmp/enter_contest'
    tblproperties ("parquet.compress"="SNAPPY");