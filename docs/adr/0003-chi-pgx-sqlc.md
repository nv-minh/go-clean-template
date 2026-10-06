# ADR 0003: chi, pgx và sqlc

Trạng thái: Accepted

## Bối cảnh

Cần stack HTTP và truy cập dữ liệu có hiệu năng cao, dễ bảo trì lâu dài, ít bất ngờ.

## Quyết định

HTTP: `net/http` với router `chi`.
Database: PostgreSQL qua `pgx` (pool, native protocol) và `sqlc` sinh code từ SQL.

## Các lựa chọn đã cân nhắc

| Lựa chọn | Đánh giá |
|----------|----------|
| Fiber (fasthttp) | Nhanh hơn trong benchmark tổng hợp, nhưng `fasthttp` không tương thích `net/http`, mất otelhttp và nhiều middleware chuẩn. Phần lớn thời gian thật nằm ở DB và mạng, không ở router |
| Gin, Echo | Tốt, nhưng `chi` thuần `net/http` hơn và dùng thẳng `http.Handler` |
| GORM | Tiện lúc đầu, nhưng chậm hơn, nhiều magic (hook, lazy), khó đoán truy vấn thực sự chạy. Không hợp với yêu cầu kiểm soát hiệu năng |
| `database/sql` thuần | Chậm hơn pgx và thiếu tính năng riêng của Postgres |
| `sqlc` (chọn) | SQL là nguồn sự thật, kiểm tra kiểu lúc compile, không có ORM runtime. Đổi SQL sai là lỗi biên dịch |

## Hệ quả

- Phải viết SQL bằng tay. Đổi lại truy vấn rõ ràng, tối ưu được, review được.
- Code sinh ra (`internal/adapter/repository/sqlcdb`) được commit và CI kiểm tra không bị lệch (`make sqlc-check`).
- Dùng PgBouncer ở chế độ transaction cần đổi chế độ thực thi của pgx (xem SCALING.md).
