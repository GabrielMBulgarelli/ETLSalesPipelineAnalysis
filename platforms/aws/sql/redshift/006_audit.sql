-- Audit rows are append-only by loader permission and procedure ownership, not immutable against administrators.
CREATE TABLE IF NOT EXISTS audit.warehouse_publication_lock (
  "LockName" VARCHAR(64) NOT NULL, "CreatedAt" TIMESTAMP NOT NULL
) DISTSTYLE ALL;
INSERT INTO audit.warehouse_publication_lock
SELECT 'redshift-warehouse-publication', GETDATE()
WHERE NOT EXISTS (SELECT 1 FROM audit.warehouse_publication_lock WHERE "LockName" = 'redshift-warehouse-publication');
CREATE TABLE IF NOT EXISTS audit.completed_publications (
  "BatchID" VARCHAR(256) NOT NULL, "PublicationFingerprint" VARCHAR(64) NOT NULL,
  "ContractVersion" INTEGER NOT NULL, "PipelineVersion" VARCHAR(64) NOT NULL,
  "CurationMarkerSHA256" VARCHAR(64) NOT NULL, "WarehousePolicyID" VARCHAR(64) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL, "CompletedAt" TIMESTAMP NOT NULL,
  PRIMARY KEY ("BatchID")
) DISTSTYLE AUTO SORTKEY ("BatchID");
CREATE TABLE IF NOT EXISTS audit.publication_datasets (
  "BatchID" VARCHAR(256) NOT NULL, "DatasetOrdinal" INTEGER NOT NULL, "DatasetName" VARCHAR(64) NOT NULL,
  "DatasetSHA256" VARCHAR(64) NOT NULL, "RowCount" BIGINT NOT NULL,
  PRIMARY KEY ("BatchID", "DatasetOrdinal")
) DISTSTYLE AUTO SORTKEY ("BatchID", "DatasetOrdinal");
CREATE TABLE IF NOT EXISTS audit.warehouse_load_events (
  "EventID" VARCHAR(64) NOT NULL, "LoadAttemptID" VARCHAR(64) NOT NULL, "BatchID" VARCHAR(256) NOT NULL,
  "EventTimestamp" TIMESTAMP NOT NULL, "EventType" VARCHAR(64) NOT NULL, "Retryable" BOOLEAN NOT NULL,
  "PublicationFingerprint" VARCHAR(64), "GlueJobRunID" VARCHAR(256), "RedshiftStatementID" VARCHAR(256),
  "FailureStage" VARCHAR(128), "FailureCode" VARCHAR(128), "Details" VARCHAR(65535),
  PRIMARY KEY ("EventID")
) DISTSTYLE AUTO SORTKEY ("BatchID", "EventTimestamp");

-- Default atomic mode: no transaction-control statement or implicit-boundary operation appears here.
CREATE OR REPLACE PROCEDURE audit.publish_warehouse(
  IN p_batch_id VARCHAR(256), IN p_load_attempt_id VARCHAR(64), IN p_publication_fingerprint VARCHAR(64),
  IN p_contract_version INTEGER, IN p_pipeline_version VARCHAR(64), IN p_marker_sha256 VARCHAR(64),
  IN p_warehouse_effective_at TIMESTAMP, IN p_dataset_identity_json VARCHAR(65535), IN p_event_id VARCHAR(64)
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_existing_fingerprint VARCHAR(64);
  v_bad_count BIGINT;
  v_now TIMESTAMP := GETDATE();
BEGIN
  LOCK audit.warehouse_publication_lock;

  SELECT COUNT(*) INTO v_bad_count FROM audit.completed_publications WHERE "BatchID" = p_batch_id;
  IF v_bad_count > 1 THEN RAISE EXCEPTION 'duplicate completed publication registry rows'; END IF;
  SELECT MAX("PublicationFingerprint") INTO v_existing_fingerprint
    FROM audit.completed_publications WHERE "BatchID" = p_batch_id;
  IF v_existing_fingerprint IS NOT NULL AND v_existing_fingerprint <> p_publication_fingerprint THEN
    RAISE EXCEPTION 'publication fingerprint conflict';
  END IF;
  IF v_existing_fingerprint = p_publication_fingerprint THEN
    INSERT INTO audit.warehouse_load_events VALUES
      (p_event_id, p_load_attempt_id, p_batch_id, v_now, 'NO_OP', FALSE,
       p_publication_fingerprint, NULL, NULL, NULL, NULL, 'matching completed Redshift publication');
    RETURN;
  END IF;

  -- All sixteen datasets must be present for exactly this attempt and batch.
  SELECT COUNT(*) INTO v_bad_count FROM (
    SELECT 'dim_customer' n WHERE EXISTS (SELECT 1 FROM staging.dim_customer WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'dim_product' WHERE EXISTS (SELECT 1 FROM staging.dim_product WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'dim_seller' WHERE EXISTS (SELECT 1 FROM staging.dim_seller WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'dim_geography' WHERE EXISTS (SELECT 1 FROM staging.dim_geography WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'dim_date' WHERE EXISTS (SELECT 1 FROM staging.dim_date WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'dim_order_status' WHERE EXISTS (SELECT 1 FROM staging.dim_order_status WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'fact_sales' WHERE EXISTS (SELECT 1 FROM staging.fact_sales WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'fact_reviews' WHERE EXISTS (SELECT 1 FROM staging.fact_reviews WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'sales_by_state' WHERE EXISTS (SELECT 1 FROM staging.sales_by_state WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'sales_by_category' WHERE EXISTS (SELECT 1 FROM staging.sales_by_category WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'monthly_sales' WHERE EXISTS (SELECT 1 FROM staging.monthly_sales WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'order_status' WHERE EXISTS (SELECT 1 FROM staging.order_status WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'cross_state_analysis' WHERE EXISTS (SELECT 1 FROM staging.cross_state_analysis WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'seller_performance' WHERE EXISTS (SELECT 1 FROM staging.seller_performance WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'size_analysis' WHERE EXISTS (SELECT 1 FROM staging.size_analysis WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
    UNION ALL SELECT 'payment_methods' WHERE EXISTS (SELECT 1 FROM staging.payment_methods WHERE "LoadAttemptID"=p_load_attempt_id AND "SourceBatchID"=p_batch_id)
  ) present;
  IF v_bad_count <> 16 THEN RAISE EXCEPTION 'incomplete staged dataset set'; END IF;
  IF p_warehouse_effective_at IS NULL OR p_dataset_identity_json IS NULL THEN
    RAISE EXCEPTION 'missing immutable publication inputs';
  END IF;
  SELECT COUNT(*) INTO v_bad_count FROM staging.publication_datasets
    WHERE "LoadAttemptID"=p_load_attempt_id AND "BatchID"=p_batch_id
      AND "PublicationFingerprint"=p_publication_fingerprint;
  IF v_bad_count<>16 THEN RAISE EXCEPTION 'publication identity must contain exactly 16 datasets'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM staging.publication_datasets
    WHERE "LoadAttemptID"=p_load_attempt_id
      AND ("BatchID"<>p_batch_id OR "PublicationFingerprint"<>p_publication_fingerprint);
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'staged attempt belongs to another publication identity'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM (
    SELECT "DatasetOrdinal" FROM staging.publication_datasets WHERE "LoadAttemptID"=p_load_attempt_id
    GROUP BY 1 HAVING COUNT(*)<>1
  ) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'duplicate publication dataset ordinal'; END IF;

  -- Duplicate incoming business keys and fact grains are deterministic failures.
  SELECT COUNT(*) INTO v_bad_count FROM (SELECT "CustomerID" FROM staging.dim_customer WHERE "LoadAttemptID"=p_load_attempt_id GROUP BY 1 HAVING COUNT(*)<>1) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'duplicate customer business key'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM (SELECT "ProductID" FROM staging.dim_product WHERE "LoadAttemptID"=p_load_attempt_id GROUP BY 1 HAVING COUNT(*)<>1) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'duplicate product business key'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM (SELECT "SellerID" FROM staging.dim_seller WHERE "LoadAttemptID"=p_load_attempt_id GROUP BY 1 HAVING COUNT(*)<>1) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'duplicate seller business key'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM (SELECT "ZipCodePrefix" FROM staging.dim_geography WHERE "LoadAttemptID"=p_load_attempt_id GROUP BY 1 HAVING COUNT(*)<>1) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'duplicate geography business key'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM (SELECT "OrderID","OrderItemID" FROM staging.fact_sales WHERE "LoadAttemptID"=p_load_attempt_id GROUP BY 1,2 HAVING COUNT(*)<>1) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'duplicate fact_sales grain'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM (SELECT "OrderID","ReviewID" FROM staging.fact_reviews WHERE "LoadAttemptID"=p_load_attempt_id GROUP BY 1,2 HAVING COUNT(*)<>1) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'duplicate fact_reviews grain'; END IF;

  -- A different tracked value at an already observed instant is not orderable and must fail.
  SELECT COUNT(*) INTO v_bad_count FROM staging.dim_customer s JOIN warehouse.dim_customer d ON d."CustomerID"=s."CustomerID" AND d."WarehouseEffectiveAt"=s."WarehouseEffectiveAt" AND d."SCD2TrackedHash"<>s."SCD2TrackedHash" WHERE s."LoadAttemptID"=p_load_attempt_id;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'same-effective-time customer conflict'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM staging.dim_product s JOIN warehouse.dim_product d ON d."ProductID"=s."ProductID" AND d."WarehouseEffectiveAt"=s."WarehouseEffectiveAt" AND d."SCD2TrackedHash"<>s."SCD2TrackedHash" WHERE s."LoadAttemptID"=p_load_attempt_id;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'same-effective-time product conflict'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM staging.dim_seller s JOIN warehouse.dim_seller d ON d."SellerID"=s."SellerID" AND d."WarehouseEffectiveAt"=s."WarehouseEffectiveAt" AND d."SCD2TrackedHash"<>s."SCD2TrackedHash" WHERE s."LoadAttemptID"=p_load_attempt_id;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'same-effective-time seller conflict'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM staging.dim_geography s JOIN warehouse.dim_geography d ON d."ZipCodePrefix"=s."ZipCodePrefix" AND d."WarehouseEffectiveAt"=s."WarehouseEffectiveAt" AND d."SCD2TrackedHash"<>s."SCD2TrackedHash" WHERE s."LoadAttemptID"=p_load_attempt_id;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'same-effective-time geography conflict'; END IF;

  -- Expire or split the interval covering an approved tracked change. Hashes are prepared canonically by the loader.
  UPDATE warehouse.dim_customer SET "WarehouseExpirationAt"=s."WarehouseEffectiveAt", "CurrentFlag"=FALSE
    FROM staging.dim_customer s WHERE s."LoadAttemptID"=p_load_attempt_id AND warehouse.dim_customer."CustomerID"=s."CustomerID"
    AND warehouse.dim_customer."WarehouseEffectiveAt" < s."WarehouseEffectiveAt"
    AND COALESCE(warehouse.dim_customer."WarehouseExpirationAt",'9999-12-31') > s."WarehouseEffectiveAt"
    AND warehouse.dim_customer."SCD2TrackedHash"<>s."SCD2TrackedHash";
  UPDATE warehouse.dim_product SET "WarehouseExpirationAt"=s."WarehouseEffectiveAt", "CurrentFlag"=FALSE
    FROM staging.dim_product s WHERE s."LoadAttemptID"=p_load_attempt_id AND warehouse.dim_product."ProductID"=s."ProductID" AND warehouse.dim_product."WarehouseEffectiveAt"<s."WarehouseEffectiveAt" AND COALESCE(warehouse.dim_product."WarehouseExpirationAt",'9999-12-31')>s."WarehouseEffectiveAt" AND warehouse.dim_product."SCD2TrackedHash"<>s."SCD2TrackedHash";
  UPDATE warehouse.dim_seller SET "WarehouseExpirationAt"=s."WarehouseEffectiveAt", "CurrentFlag"=FALSE
    FROM staging.dim_seller s WHERE s."LoadAttemptID"=p_load_attempt_id AND warehouse.dim_seller."SellerID"=s."SellerID" AND warehouse.dim_seller."WarehouseEffectiveAt"<s."WarehouseEffectiveAt" AND COALESCE(warehouse.dim_seller."WarehouseExpirationAt",'9999-12-31')>s."WarehouseEffectiveAt" AND warehouse.dim_seller."SCD2TrackedHash"<>s."SCD2TrackedHash";
  UPDATE warehouse.dim_geography SET "WarehouseExpirationAt"=s."WarehouseEffectiveAt", "CurrentFlag"=FALSE
    FROM staging.dim_geography s WHERE s."LoadAttemptID"=p_load_attempt_id AND warehouse.dim_geography."ZipCodePrefix"=s."ZipCodePrefix" AND warehouse.dim_geography."WarehouseEffectiveAt"<s."WarehouseEffectiveAt" AND COALESCE(warehouse.dim_geography."WarehouseExpirationAt",'9999-12-31')>s."WarehouseEffectiveAt" AND warehouse.dim_geography."SCD2TrackedHash"<>s."SCD2TrackedHash";

  -- Inserts use deterministic MAX + ROW_NUMBER allocation while the publication lock is held.
  INSERT INTO warehouse.dim_customer
  SELECT COALESCE((SELECT MAX("CustomerKey") FROM warehouse.dim_customer),0)+ROW_NUMBER() OVER(ORDER BY s."CustomerID"),
    s."CustomerID",s."CustomerUniqueID",s."CustomerZipCodePrefix",s."CustomerCity",s."CustomerState",s."RecordHash",s."SCD2TrackedHash",
    CASE WHEN NOT EXISTS(SELECT 1 FROM warehouse.dim_customer d WHERE d."CustomerID"=s."CustomerID") THEN '1900-01-01'::TIMESTAMP ELSE s."WarehouseEffectiveAt" END,
    (SELECT MIN(d."WarehouseEffectiveAt") FROM warehouse.dim_customer d WHERE d."CustomerID"=s."CustomerID" AND d."WarehouseEffectiveAt">s."WarehouseEffectiveAt"),
    CASE WHEN EXISTS(SELECT 1 FROM warehouse.dim_customer d WHERE d."CustomerID"=s."CustomerID" AND d."WarehouseEffectiveAt">s."WarehouseEffectiveAt") THEN FALSE ELSE TRUE END,
    p_batch_id,v_now FROM staging.dim_customer s WHERE s."LoadAttemptID"=p_load_attempt_id AND NOT EXISTS
    (SELECT 1 FROM warehouse.dim_customer d WHERE d."CustomerID"=s."CustomerID" AND d."SCD2TrackedHash"=s."SCD2TrackedHash" AND s."WarehouseEffectiveAt">=d."WarehouseEffectiveAt" AND s."WarehouseEffectiveAt"<COALESCE(d."WarehouseExpirationAt",'9999-12-31'));
  -- Product, seller, and geography follow the identical half-open policy.
  INSERT INTO warehouse.dim_product SELECT COALESCE((SELECT MAX("ProductKey") FROM warehouse.dim_product),0)+ROW_NUMBER() OVER(ORDER BY s."ProductID"),s."ProductID",s."ProductCategoryName",s."ProductCategoryNameEnglish",s."ProductWeightG",s."ProductLengthCm",s."ProductHeightCm",s."ProductWidthCm",s."ProductVolumeCm3",s."SizeCategory",s."RecordHash",s."SCD2TrackedHash",CASE WHEN NOT EXISTS(SELECT 1 FROM warehouse.dim_product d WHERE d."ProductID"=s."ProductID") THEN '1900-01-01'::TIMESTAMP ELSE s."WarehouseEffectiveAt" END,(SELECT MIN(d."WarehouseEffectiveAt") FROM warehouse.dim_product d WHERE d."ProductID"=s."ProductID" AND d."WarehouseEffectiveAt">s."WarehouseEffectiveAt"),CASE WHEN EXISTS(SELECT 1 FROM warehouse.dim_product d WHERE d."ProductID"=s."ProductID" AND d."WarehouseEffectiveAt">s."WarehouseEffectiveAt") THEN FALSE ELSE TRUE END,p_batch_id,v_now FROM staging.dim_product s WHERE s."LoadAttemptID"=p_load_attempt_id AND NOT EXISTS(SELECT 1 FROM warehouse.dim_product d WHERE d."ProductID"=s."ProductID" AND d."SCD2TrackedHash"=s."SCD2TrackedHash" AND s."WarehouseEffectiveAt">=d."WarehouseEffectiveAt" AND s."WarehouseEffectiveAt"<COALESCE(d."WarehouseExpirationAt",'9999-12-31'));
  INSERT INTO warehouse.dim_seller SELECT COALESCE((SELECT MAX("SellerKey") FROM warehouse.dim_seller),0)+ROW_NUMBER() OVER(ORDER BY s."SellerID"),s."SellerID",s."SellerZipCodePrefix",s."SellerCity",s."SellerState",s."RecordHash",s."SCD2TrackedHash",CASE WHEN NOT EXISTS(SELECT 1 FROM warehouse.dim_seller d WHERE d."SellerID"=s."SellerID") THEN '1900-01-01'::TIMESTAMP ELSE s."WarehouseEffectiveAt" END,(SELECT MIN(d."WarehouseEffectiveAt") FROM warehouse.dim_seller d WHERE d."SellerID"=s."SellerID" AND d."WarehouseEffectiveAt">s."WarehouseEffectiveAt"),CASE WHEN EXISTS(SELECT 1 FROM warehouse.dim_seller d WHERE d."SellerID"=s."SellerID" AND d."WarehouseEffectiveAt">s."WarehouseEffectiveAt") THEN FALSE ELSE TRUE END,p_batch_id,v_now FROM staging.dim_seller s WHERE s."LoadAttemptID"=p_load_attempt_id AND NOT EXISTS(SELECT 1 FROM warehouse.dim_seller d WHERE d."SellerID"=s."SellerID" AND d."SCD2TrackedHash"=s."SCD2TrackedHash" AND s."WarehouseEffectiveAt">=d."WarehouseEffectiveAt" AND s."WarehouseEffectiveAt"<COALESCE(d."WarehouseExpirationAt",'9999-12-31'));
  INSERT INTO warehouse.dim_geography SELECT COALESCE((SELECT MAX("GeographyKey") FROM warehouse.dim_geography),0)+ROW_NUMBER() OVER(ORDER BY s."ZipCodePrefix"),s."ZipCodePrefix",s."City",s."State",s."Latitude",s."Longitude",s."Region",s."RecordHash",s."SCD2TrackedHash",CASE WHEN NOT EXISTS(SELECT 1 FROM warehouse.dim_geography d WHERE d."ZipCodePrefix"=s."ZipCodePrefix") THEN '1900-01-01'::TIMESTAMP ELSE s."WarehouseEffectiveAt" END,(SELECT MIN(d."WarehouseEffectiveAt") FROM warehouse.dim_geography d WHERE d."ZipCodePrefix"=s."ZipCodePrefix" AND d."WarehouseEffectiveAt">s."WarehouseEffectiveAt"),CASE WHEN EXISTS(SELECT 1 FROM warehouse.dim_geography d WHERE d."ZipCodePrefix"=s."ZipCodePrefix" AND d."WarehouseEffectiveAt">s."WarehouseEffectiveAt") THEN FALSE ELSE TRUE END,p_batch_id,v_now FROM staging.dim_geography s WHERE s."LoadAttemptID"=p_load_attempt_id AND NOT EXISTS(SELECT 1 FROM warehouse.dim_geography d WHERE d."ZipCodePrefix"=s."ZipCodePrefix" AND d."SCD2TrackedHash"=s."SCD2TrackedHash" AND s."WarehouseEffectiveAt">=d."WarehouseEffectiveAt" AND s."WarehouseEffectiveAt"<COALESCE(d."WarehouseExpirationAt",'9999-12-31'));

  -- Static dimensions synchronize under the same transaction; keys stay stable on Type 1 updates.
  UPDATE warehouse.dim_date SET "DateID"=s."DateID","Date"=s."Date","Year"=s."Year","Month"=s."Month","Day"=s."Day","Quarter"=s."Quarter","WeekOfYear"=s."WeekOfYear","DayOfWeek"=s."DayOfWeek","IsWeekend"=s."IsWeekend","MonthName"=s."MonthName","DayName"=s."DayName","FiscalYear"=s."FiscalYear","FiscalQuarter"=s."FiscalQuarter","Holiday"=s."Holiday","IsHoliday"=s."IsHoliday","RecordHash"=s."RecordHash","SourceBatchID"=p_batch_id,"UpdatedAt"=v_now FROM staging.dim_date s WHERE s."LoadAttemptID"=p_load_attempt_id AND warehouse.dim_date."DateKey"=s."DateKey";
  UPDATE warehouse.dim_order_status SET "StatusDescription"=s."StatusDescription","IsApproved"=s."IsApproved","IsDelivered"=s."IsDelivered","IsShipped"=s."IsShipped","IsCancelled"=s."IsCancelled","RecordHash"=s."RecordHash","SourceBatchID"=p_batch_id,"UpdatedAt"=v_now FROM staging.dim_order_status s WHERE s."LoadAttemptID"=p_load_attempt_id AND warehouse.dim_order_status."StatusID"=s."StatusID";
  -- Static dimensions synchronize by insertion of previously unseen keys.
  INSERT INTO warehouse.dim_date SELECT COALESCE((SELECT MAX("DateSurrogateKey") FROM warehouse.dim_date),0)+ROW_NUMBER() OVER(ORDER BY s."DateKey"),s."DateKey",s."DateID",s."Date",s."Year",s."Month",s."Day",s."Quarter",s."WeekOfYear",s."DayOfWeek",s."IsWeekend",s."MonthName",s."DayName",s."FiscalYear",s."FiscalQuarter",s."Holiday",s."IsHoliday",s."RecordHash",p_batch_id,v_now FROM staging.dim_date s WHERE s."LoadAttemptID"=p_load_attempt_id AND NOT EXISTS(SELECT 1 FROM warehouse.dim_date d WHERE d."DateKey"=s."DateKey");
  INSERT INTO warehouse.dim_order_status SELECT COALESCE((SELECT MAX("OrderStatusKey") FROM warehouse.dim_order_status),0)+ROW_NUMBER() OVER(ORDER BY s."StatusID"),s."StatusID",s."StatusDescription",s."IsApproved",s."IsDelivered",s."IsShipped",s."IsCancelled",s."RecordHash",p_batch_id,v_now FROM staging.dim_order_status s WHERE s."LoadAttemptID"=p_load_attempt_id AND NOT EXISTS(SELECT 1 FROM warehouse.dim_order_status d WHERE d."StatusID"=s."StatusID");

  -- Validate dimension history explicitly because Redshift constraints are informational.
  SELECT COUNT(*) INTO v_bad_count FROM warehouse.dim_customer a JOIN warehouse.dim_customer b ON a."CustomerID"=b."CustomerID" AND a."CustomerKey"<b."CustomerKey" AND a."WarehouseEffectiveAt"<COALESCE(b."WarehouseExpirationAt",'9999-12-31') AND b."WarehouseEffectiveAt"<COALESCE(a."WarehouseExpirationAt",'9999-12-31');
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'overlapping customer history'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM (SELECT "CustomerKey" FROM warehouse.dim_customer GROUP BY 1 HAVING COUNT(*)<>1) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'duplicate customer surrogate key'; END IF;
  SELECT COUNT(*) INTO v_bad_count FROM (SELECT "CustomerID" FROM warehouse.dim_customer WHERE "CurrentFlag" GROUP BY 1 HAVING COUNT(*)<>1) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'multiple current customer rows'; END IF;

  -- Strict temporal resolution count: every staged fact must produce exactly one relationship tuple.
  SELECT ABS((SELECT COUNT(*) FROM staging.fact_sales WHERE "LoadAttemptID"=p_load_attempt_id)-COUNT(*)) INTO v_bad_count FROM (
    SELECT s."OrderID",s."OrderItemID" FROM staging.fact_sales s
    JOIN warehouse.dim_customer c ON c."CustomerID"=s."CustomerID" AND s."OrderPurchaseTimestamp">=c."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(c."WarehouseExpirationAt",'9999-12-31')
    JOIN warehouse.dim_product p ON p."ProductID"=s."ProductID" AND s."OrderPurchaseTimestamp">=p."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(p."WarehouseExpirationAt",'9999-12-31')
    JOIN warehouse.dim_seller se ON se."SellerID"=s."SellerID" AND s."OrderPurchaseTimestamp">=se."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(se."WarehouseExpirationAt",'9999-12-31')
    JOIN warehouse.dim_geography cg ON cg."ZipCodePrefix"=s."ZipCodePrefix" AND s."OrderPurchaseTimestamp">=cg."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(cg."WarehouseExpirationAt",'9999-12-31')
    JOIN warehouse.dim_geography sg ON sg."ZipCodePrefix"=se."SellerZipCodePrefix" AND s."OrderPurchaseTimestamp">=sg."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(sg."WarehouseExpirationAt",'9999-12-31')
    JOIN warehouse.dim_date d ON d."DateKey"=s."DateKey" JOIN warehouse.dim_order_status os ON os."StatusID"=s."StatusID"
    WHERE s."LoadAttemptID"=p_load_attempt_id GROUP BY 1,2 HAVING COUNT(*)=1
  ) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'zero or multiple fact_sales relationships'; END IF;
  SELECT ABS((SELECT COUNT(*) FROM staging.fact_reviews WHERE "LoadAttemptID"=p_load_attempt_id)-COUNT(*)) INTO v_bad_count FROM (
    SELECT s."OrderID",s."ReviewID" FROM staging.fact_reviews s
    JOIN warehouse.dim_customer c ON c."CustomerID"=s."CustomerID" AND s."ReviewCreationDate">=c."WarehouseEffectiveAt" AND s."ReviewCreationDate"<COALESCE(c."WarehouseExpirationAt",'9999-12-31')
    JOIN warehouse.dim_date d ON d."DateKey"=s."DateKey" WHERE s."LoadAttemptID"=p_load_attempt_id GROUP BY 1,2 HAVING COUNT(*)=1
  ) q;
  IF v_bad_count<>0 THEN RAISE EXCEPTION 'zero or multiple fact_reviews relationships'; END IF;

  DELETE FROM warehouse.fact_sales;
  INSERT INTO warehouse.fact_sales SELECT s."OrderID",s."OrderItemID",s."CustomerID",s."ProductID",s."SellerID",s."DateKey",s."StatusID",s."ZipCodePrefix",c."CustomerKey",p."ProductKey",se."SellerKey",cg."GeographyKey",sg."GeographyKey",d."DateSurrogateKey",os."OrderStatusKey",s."OrderPurchaseTimestamp",s."OrderDeliveredCustomerDate",s."Price",s."FreightValue",s."TotalItemValue",s."ShippingDays",s."DeliveryDays",s."TotalDays",s."IsDelayed",s."DelayDays",s."IsCrossState",s."PaymentType",s."RecordHash",p_batch_id,v_now FROM staging.fact_sales s JOIN warehouse.dim_customer c ON c."CustomerID"=s."CustomerID" AND s."OrderPurchaseTimestamp">=c."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(c."WarehouseExpirationAt",'9999-12-31') JOIN warehouse.dim_product p ON p."ProductID"=s."ProductID" AND s."OrderPurchaseTimestamp">=p."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(p."WarehouseExpirationAt",'9999-12-31') JOIN warehouse.dim_seller se ON se."SellerID"=s."SellerID" AND s."OrderPurchaseTimestamp">=se."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(se."WarehouseExpirationAt",'9999-12-31') JOIN warehouse.dim_geography cg ON cg."ZipCodePrefix"=s."ZipCodePrefix" AND s."OrderPurchaseTimestamp">=cg."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(cg."WarehouseExpirationAt",'9999-12-31') JOIN warehouse.dim_geography sg ON sg."ZipCodePrefix"=se."SellerZipCodePrefix" AND s."OrderPurchaseTimestamp">=sg."WarehouseEffectiveAt" AND s."OrderPurchaseTimestamp"<COALESCE(sg."WarehouseExpirationAt",'9999-12-31') JOIN warehouse.dim_date d ON d."DateKey"=s."DateKey" JOIN warehouse.dim_order_status os ON os."StatusID"=s."StatusID" WHERE s."LoadAttemptID"=p_load_attempt_id;
  DELETE FROM warehouse.fact_reviews;
  INSERT INTO warehouse.fact_reviews SELECT s."OrderID",s."ReviewID",s."CustomerID",s."DateKey",c."CustomerKey",d."DateSurrogateKey",s."ReviewScore",s."ReviewCommentMessage",s."ReviewCreationDate",s."ReviewAnswerTimestamp",s."ReviewResponseDays",s."RecordHash",p_batch_id,v_now FROM staging.fact_reviews s JOIN warehouse.dim_customer c ON c."CustomerID"=s."CustomerID" AND s."ReviewCreationDate">=c."WarehouseEffectiveAt" AND s."ReviewCreationDate"<COALESCE(c."WarehouseExpirationAt",'9999-12-31') JOIN warehouse.dim_date d ON d."DateKey"=s."DateKey" WHERE s."LoadAttemptID"=p_load_attempt_id;

  -- Physical aggregates are replaced with DELETE plus validated inserts; payment_methods copies item-price attribution only.
  DELETE FROM analytics.sales_by_state; INSERT INTO analytics.sales_by_state SELECT "CustomerState","OrdersCount","UniqueCustomers","TotalSales","AvgItemPrice","AvgDeliveryTime","LastUpdated",p_batch_id FROM staging.sales_by_state WHERE "LoadAttemptID"=p_load_attempt_id;
  DELETE FROM analytics.sales_by_category; INSERT INTO analytics.sales_by_category SELECT "ProductCategoryNameEnglish","OrdersCount","UniqueCustomers","TotalSales","AvgItemPrice","AvgDeliveryTime","DelayedOrders","LastUpdated",p_batch_id FROM staging.sales_by_category WHERE "LoadAttemptID"=p_load_attempt_id;
  DELETE FROM analytics.monthly_sales; INSERT INTO analytics.monthly_sales SELECT "Year","Month","OrdersCount","UniqueCustomers","TotalSales","AvgItemPrice","LastUpdated",p_batch_id FROM staging.monthly_sales WHERE "LoadAttemptID"=p_load_attempt_id;
  DELETE FROM analytics.order_status; INSERT INTO analytics.order_status SELECT "ProductCategoryNameEnglish","OrderStatus","OrdersCount","TotalSales","UniqueCustomers","LastUpdated",p_batch_id FROM staging.order_status WHERE "LoadAttemptID"=p_load_attempt_id;
  DELETE FROM analytics.cross_state_analysis; INSERT INTO analytics.cross_state_analysis SELECT "ProductCategoryNameEnglish","IsCrossState","OrdersCount","TotalSales","AvgDeliveryTime","DelayRate","LastUpdated",p_batch_id FROM staging.cross_state_analysis WHERE "LoadAttemptID"=p_load_attempt_id;
  DELETE FROM analytics.seller_performance; INSERT INTO analytics.seller_performance SELECT "SellerID","SellerState","OrdersCount","TotalSales","AvgShippingCost","AvgDeliveryTime","DelayedOrders","DelayRate","LastUpdated",p_batch_id FROM staging.seller_performance WHERE "LoadAttemptID"=p_load_attempt_id;
  DELETE FROM analytics.size_analysis; INSERT INTO analytics.size_analysis SELECT "ProductCategoryNameEnglish","SizeCategory","OrdersCount","TotalSales","AvgShippingCost","LastUpdated",p_batch_id FROM staging.size_analysis WHERE "LoadAttemptID"=p_load_attempt_id;
  DELETE FROM analytics.payment_methods; INSERT INTO analytics.payment_methods SELECT "PaymentType","OrdersCount","TotalSales","AvgOrderValue","UniqueCustomers","LastUpdated",p_batch_id FROM staging.payment_methods WHERE "LoadAttemptID"=p_load_attempt_id;

  INSERT INTO audit.completed_publications VALUES (p_batch_id,p_publication_fingerprint,p_contract_version,p_pipeline_version,p_marker_sha256,'redshift-scd2-v1',p_warehouse_effective_at,v_now);
  INSERT INTO audit.publication_datasets SELECT p_batch_id,"DatasetOrdinal","DatasetName","DatasetSHA256","RowCount" FROM staging.publication_datasets WHERE "LoadAttemptID"=p_load_attempt_id ORDER BY "DatasetOrdinal";
  INSERT INTO audit.warehouse_load_events VALUES (p_event_id,p_load_attempt_id,p_batch_id,v_now,'COMPLETED',FALSE,p_publication_fingerprint,NULL,NULL,NULL,NULL,p_dataset_identity_json);
END;
$$;
