
import pandas as pd
import geopy as gp

from geopy.geocoders import Nominatim

from geopy.point import Point

dms_str = "40°26'46\"N"  # 示例输入
point = Point(dms_str)
print(point.latitude)  # 输出十进制纬度：40.4461