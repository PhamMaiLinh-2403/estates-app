BDS_RAW_LISTINGS_TABLE = """
CREATE TABLE IF NOT EXISTS bds_raw (
    listing_id TEXT NOT NULL,
    url TEXT NOT NULL,
    title TEXT,
    short_address TEXT,
    address_parts TEXT,
    latitude REAL,
    longitude REAL,
    main_info TEXT,
    description TEXT,
    other_info TEXT,
    image_urls TEXT,
    key TEXT UNIQUE NOT NULL
)
"""

ONEHOUSING_RAW_LISTINGS_TABLE = """
CREATE TABLE IF NOT EXISTS onehousing_raw (
    property_id TEXT,
    property_url TEXT NOT NULL,
    listing_title TEXT,
    total_price TEXT,
    unit_price TEXT,
    city TEXT,
    district TEXT,
    alley_width TEXT,
    features TEXT, 
    property_description TEXT,
    image_url TEXT,
    key TEXT UNIQUE NOT NULL
)
"""

CLEANED_LISTINGS_TABLE = """
CREATE TABLE IF NOT EXISTS cleaned (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    "Tỉnh/Thành phố" TEXT,
    "Thành phố/Quận/Huyện/Thị xã" TEXT,
    "Xã/Phường/Thị trấn" TEXT,
    "Đường phố" TEXT,
    "Chi tiết" TEXT,
    "Nguồn thông tin" TEXT,
    "Tình trạng giao dịch" TEXT,
    "Thời điểm giao dịch/rao bán" DATE,
    "Thông tin liên hệ" TEXT,
    "Giá rao bán/giao dịch" INTEGER,
    "Giá ước tính" INTEGER,
    "Loại đơn giá (đ/m2 hoặc đ/m ngang)" TEXT,
    "Đơn giá đất" REAL,
    "Lợi thế kinh doanh" TEXT,
    "Số tầng công trình" REAL,
    "Tổng diện tích sàn" REAL,
    "Đơn giá xây dựng" REAL,
    "Năm xây dựng" INTEGER,
    "Chất lượng còn lại" REAL,
    "Diện tích đất (m2)" REAL,
    "Kích thước mặt tiền (m)" REAL,
    "Kích thước chiều dài (m)" REAL,
    "Số mặt tiền tiếp giáp" INTEGER,
    "Hình dạng" TEXT,
    "Độ rộng ngõ/ngách nhỏ nhất (m)" REAL,
    "Khoảng cách tới trục đường chính (m)" REAL,
    "Mục đích sử dụng đất" TEXT,
    "Yếu tố khác" TEXT,
    "Tọa độ (vĩ độ)" REAL,
    "Tọa độ (kinh độ)" REAL,
    "Hình ảnh của bài đăng" TEXT,
    websites TEXT 
)
"""