import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
AZURE = ROOT / "platforms" / "azure"


class AzureContractTests(unittest.TestCase):
    def test_fact_loads_resolve_business_keys_through_staging(self):
        schema = (AZURE / "synapse-sql" / "Schema.sql").read_text(encoding="utf-8")
        sales = (AZURE / "synapse-sql" / "load" / "fact" / "copy_fact_sales.sql").read_text(
            encoding="utf-8"
        )
        reviews = (AZURE / "synapse-sql" / "load" / "fact" / "copy_fact_reviews.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("CREATE TABLE ecom.StageFactSales", schema)
        self.assertIn("CREATE TABLE ecom.StageFactReviews", schema)
        self.assertIn("COPY INTO ecom.StageFactSales", sales)
        self.assertIn("JOIN ecom.DimCustomer", sales)
        self.assertIn("JOIN ecom.DimProduct", sales)
        self.assertIn("JOIN ecom.DimSeller", sales)
        self.assertIn("COPY INTO ecom.StageFactReviews", reviews)
        self.assertIn("JOIN ecom.DimCustomer", reviews)

    def test_aggregate_load_columns_match_descriptive_contract(self):
        analysis_dir = AZURE / "synapse-sql" / "load" / "analysis"
        contents = "\n".join(path.read_text(encoding="utf-8") for path in analysis_dir.glob("*.sql"))

        self.assertNotIn("CategoryKey,", contents)
        self.assertNotIn("PaymentTypeKey,", contents)
        self.assertIn("ProductCategoryNameEnglish", contents)
        self.assertIn("PaymentType", contents)

    def test_order_status_aggregate_has_schema_and_loader(self):
        schema = (AZURE / "synapse-sql" / "Schema.sql").read_text(encoding="utf-8")
        loader = AZURE / "synapse-sql" / "load" / "analysis" / "copy_fact_order_status_analysis.sql"

        self.assertIn("CREATE TABLE ecom.FactOrderStatusAnalysis", schema)
        self.assertTrue(loader.exists())

    def test_curated_notebook_uses_reproducible_contracts(self):
        notebook = json.loads(
            (AZURE / "synapse-notebooks" / "03_EcomSales_Curated_Analytics.ipynb").read_text(encoding="utf-8")
        )
        source = "\n".join(
            "".join(cell.get("source", [])) for cell in notebook["cells"] if cell.get("cell_type") == "code"
        )

        self.assertIn('F.col("customer_state")', source)
        self.assertIn('Window.partitionBy("order_id")', source)
        self.assertIn('F.row_number()', source)
        self.assertNotIn('F.first("payment_type")', source)
        self.assertNotIn("dim_product_with_size", source)
        self.assertNotIn("F.current_timestamp()", source)


if __name__ == "__main__":
    unittest.main()
