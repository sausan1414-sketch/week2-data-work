Summary — ETL + EDA

The ETL pipeline processed all raw orders and users data into analysis-ready tables with consistent schema and validation.
Revenue is highly concentrated, with a small number of countries contributing the majority of total revenue, while refunds represent a very small fraction of total orders.
Outliers were detected using the IQR method and flagged without removal, and winsorization (1%–99%) was applied to reduce the influence of extreme values on aggregate metrics.
Time-based features derived from created_at reveal uneven order distribution across time, indicating potential periods of higher demand.