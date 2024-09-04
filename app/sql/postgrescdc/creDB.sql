
CREATE TABLE public.salespayments (
    invoiceNumber     varchar(40) NOT NULL,
    payDateTime_Ltz   varchar(32),
    payTimestamp_Epoc varchar(14),
    paid              decimal(10,2),
    finTransactionId  varchar(40),
    created_at        timestamptz DEFAULT NOW() NOT NULL,
    PRIMARY KEY (invoiceNumber)
) TABLESPACE pg_default;

CREATE INDEX salespayments_invoicenumber_idx ON public.salespayments (invoicenumber);

ALTER TABLE IF EXISTS public.salespayments OWNER to dbadmin;

CREATE USER flinkcdc LOGIN PASSWORD 'flinkpw';
GRANT ALL PRIVILEGES ON DATABASE sales TO flinkcdc;
