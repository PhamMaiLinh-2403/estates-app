BDS_RAW_TABLE = """
    CREATE TABLE IF NOT EXISTS bds_raw(
    id TEXT NOT NULL,
    url TEXT NOT NULL,
    title TEXT,
    short_address TEXT,
    address_parts TEXT,
    latitude REAL,
    longitude REAL,
    main_info TEXT,
    description TEXT,
    other_info TEXT,
    image_urls TEXT
    );
    """
UNIQUE_INDEX_BDS_RAW = """
    CREATE UNIQUE INDEX IF NOT EXISTS index_bds_raw
    ON bds_raw(
    id,
    title,
    short_address,
    address_parts,
    main_info,
    description,
    other_info
    );
    """
# Bỏ url, latitude, longitude, image_urls

ONEHOUSING_RAW_TABLE = """
    CREATE TABLE IF NOT EXISTS onehousing_raw(
    property_id TEXT,
    property_url TEXT NOT NULL,
    listing_title TEXT,
    total_price TEXT,
    unit_price TEXT,
    city TEXT,
    district TEXT,
    alley_width TEXT,
    features TEXT, 
    latitude REAL,
    longitude REAL,
    property_description TEXT,
    image_url TEXT
    );
    """
UNIQUE_INDEX_ONEHOUSING_RAW = """
    CREATE UNIQUE INDEX IF NOT EXISTS index_onehousing_raw
    ON onehousing_raw(
    property_id,
    property_description,
    listing_title,
    total_price,
    unit_price,
    city,
    district,
    features
    );
    """
# Bỏ property_url, latitude, longitude, image_url, thêm property_description
# Phải bỏ alley_width đi !!!!

CLEANED_TABLE = """
CREATE TABLE IF NOT EXISTS cleaned(
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
    Web TEXT
);
"""

UNIQUE_INDEX_CLEANED = """
    CREATE UNIQUE INDEX IF NOT EXISTS index_cleaned
    ON cleaned(
        "Tỉnh/Thành phố",  
        "Thành phố/Quận/Huyện/Thị xã",  
        "Xã/Phường/Thị trấn",  
        "Đường phố",  
        "Chi tiết",
        "Giá rao bán/giao dịch", 
        "Số tầng công trình",  
        "Tổng diện tích sàn",  
        "Đơn giá xây dựng",  
        "Chất lượng còn lại",  
        "Diện tích đất (m2)",  
        "Kích thước mặt tiền (m)",  
        "Kích thước chiều dài (m)",  
        "Số mặt tiền tiếp giáp",  
        "Hình dạng",  
        "Độ rộng ngõ/ngách nhỏ nhất (m)",  
        "Khoảng cách tới trục đường chính (m)",  
        "Mục đích sử dụng đất",
        Web);
"""

# Đơn giá đất và Lợi thế kinh doanh của bên Onehousing không có nên không được thêm vào unique index
