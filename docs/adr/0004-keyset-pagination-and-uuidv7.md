# ADR 0004: Phân trang keyset và UUIDv7

Trạng thái: Accepted

## Bối cảnh

`OFFSET n` buộc Postgres duyệt và bỏ n dòng, chi phí tăng tuyến tính theo độ sâu, và kết quả lệch khi có dữ liệu mới chèn vào giữa các lần gọi.
UUIDv4 ngẫu nhiên làm chèn rải khắp B-tree, gây phân mảnh và cache miss.

## Quyết định

Phân trang theo `(created_at, id)` với cursor mờ đối với client.
Index `(customer_id, created_at DESC, id DESC)` khớp đúng thứ tự sort.
ID là UUIDv7 (có tiền tố thời gian).

## Hệ quả

- Chi phí mỗi trang là O(kích thước trang) dù đi sâu tới đâu, và kết quả ổn định khi có dữ liệu mới.
- Không có "nhảy tới trang N" và không có tổng số chính xác. Đó là đánh đổi chấp nhận được cho dữ liệu dạng dòng thời gian.
- Chèn gần như append-only nên index gọn và nóng.
- Cursor mờ cho phép đổi khóa sắp xếp sau này mà không phá hợp đồng API.
