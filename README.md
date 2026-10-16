# Real-Time Crypto Market Data Pipeline & Financial Analytics Warehouse

Dự án thu thập Binance Spot trades, xử lý OHLCV/VWAP bằng Kafka và Spark,
lưu Parquet trên S3, nạp Redshift bằng Airflow, biến đổi bằng dbt và báo cáo
trên Power BI. Hiện repo có cấu hình phạm vi dữ liệu và Docker Compose cho Kafka
local; producer và các phần xử lý dữ liệu chưa được triển khai.

Hướng dẫn bắt đầu: [Bước 1 — Kafka local](docs/01-kafka-local.md).

## Phạm vi dữ liệu đã chốt

Nguồn cấu hình duy nhất cho danh sách cặp giao dịch là `config/market.json`.
Producer sẽ chỉ đăng ký các symbol trong cấu hình, không tự mở rộng theo thị trường.

| Nhóm phân tích | Cặp giao dịch |
| --- | --- |
| Blue-chip | BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT |
| Thanh toán & Layer 1 | XRPUSDT, ADAUSDT, AVAXUSDT |
| DeFi & AI | LINKUSDT, UNIUSDT, FETUSDT |

Các nhóm là nhãn phân tích của dự án, không phải bảng xếp hạng vốn hóa cập nhật.
Sử dụng stream `trade` cho từng giao dịch; tạo tên subscription bằng
`symbol.lower() + "@trade"`. Kafka message key là symbol viết hoa.
Khi khởi động, producer cần kiểm tra trạng thái giao dịch của từng symbol với Binance;
báo lỗi rõ ràng nếu không khả dụng, không tự thay bằng cặp khác.

## Tải và lưu trữ

- 100–300 bản tin/giây là giả định để lập kế hoạch, cần xác nhận bằng số liệu chạy thực tế.
- Đo tổng throughput, throughput theo symbol, kích thước bản tin và consumer lag.
- Giới hạn 10 cặp giảm phạm vi thu thập nhưng dữ liệu vẫn tăng liên tục.
  Kafka local đã có retention 24 giờ hoặc khoảng 512 MiB mỗi partition và giới hạn
  Docker container log. S3 lifecycle cho raw data và quản lý checkpoint sẽ bổ sung
  khi triển khai các phần tương ứng.
- Lưu timestamp theo UTC. Khóa định danh raw trade là `(symbol, trade_id)`;
  khóa nến 1 phút là `(symbol, window_start)`.

## Thứ tự triển khai

1. Python producer → Kafka cho đúng 10 cặp trong cấu hình.
2. Spark → raw Parquet và OHLCV/VWAP 1 phút trên local, kèm checkpoint.
3. S3 → Glue Data Catalog → Athena.
4. Airflow → Redshift staging → deduplicate → MERGE.
5. dbt → analytics marts → Power BI.
