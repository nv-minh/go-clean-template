# ADR 0006: Idempotency bằng header Idempotency-Key

Trạng thái: Accepted

## Bối cảnh

Client retry sau timeout mạng và không biết request trước đã thực thi chưa, dẫn tới trùng dữ liệu (hai order cho một lần bấm).

## Quyết định

Hỗ trợ header `Idempotency-Key` cho `POST /v1/orders`, theo mô hình các API thanh toán.
Lưu kết quả trong Redis theo khóa gồm method, path và key, kèm dấu vân tay SHA-256 của request để phát hiện dùng lại key với payload khác.

Hành vi chi tiết nằm ở ARCHITECTURE.md, mục 6.

## Các lựa chọn đã cân nhắc

| Lựa chọn | Đánh giá |
|----------|----------|
| Ràng buộc UNIQUE ở DB trên một trường tự nhiên | Không phải lúc nào cũng có trường tự nhiên |
| Lưu khóa idempotency trong Postgres cùng transaction | Chặt chẽ hơn (cùng transaction với dữ liệu) nhưng thêm ghi vào đường nóng của DB. Là lựa chọn nâng cấp nếu không chấp nhận được khoảng hở nhỏ ở Redis |
| Redis (chọn) | Nhanh, có TTL tự nhiên, rẻ cho đường nóng |

## Hệ quả

- Khoảng hở rất nhỏ tồn tại nếu handler đã commit DB nhưng process chết trước khi lưu kết quả vào Redis: khóa "đang xử lý" hết hạn sau 60 giây và client thử lại sẽ thực thi lần hai. Nếu cần bảo đảm tuyệt đối, chuyển lưu khóa vào Postgres cùng transaction.
- Cần scope khóa theo người dùng khi có xác thực (đã đánh dấu `TODO(auth)`).
