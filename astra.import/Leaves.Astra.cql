
CREATE TABLE IF NOT EXISTS keyspace_name.table_name(
        id uuid,
		product_name text,
		product_searched text,
		price text,
		offer_details list<text>,		
		discount_percent text,		
		EMI text,
		rating text,
		comment text,
		customer_name text,
		review_age text,	
        PRIMARY KEY (id)
    );