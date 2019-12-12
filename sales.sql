CREATE TABLE SALES(
               table_id        INTEGER NOT NULL
            ,	product_id		INTEGER NOT NULL
            ,	price			FLOAT   NOT NULL
            ,	timestamp	    DATETIME    --DEFAULT CURRENT_TIMESTAMP    Fecha y hora.
            ,   FOREIGN KEY (product_id)  REFERENCES PRODUCTS(product_id)
            );