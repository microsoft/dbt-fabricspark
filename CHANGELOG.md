# dbt-fabricspark Changelog

- This file provides a full account of all changes to `dbt-fabricspark`.
## 1.9.1rc2 (2024-12-31)

### exprimental feature

- Enable Python model, fixed [Issue #32](https://github.com/microsoft/dbt-fabricspark/issues/32), [get contact](mailto:willem.liang@icloud.com)
  <details><summary>Usage</summary>

    Create your pyspark model in dbt project, and implementation python function `model(dbt, session) -> DataFrame` in your model's code. 
    - [Release Note](https://www.getdbt.com/blog/introducing-support-for-python)
    - [Document](https://docs.getdbt.com/docs/build/python-models)

    Example:
    ```python
    # A udf to calculate distance between two coordinates
    from coordTransform_py import coordTransform_utils    # see also: https://github.com/wandergis/coordTransform_py
    from geopy.distance import geodesic
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType

    @udf(DoubleType())
    def udf_geo_distance(lng1,lat1,lng2,lat2,vendor1,vendor2):
        wgs84_converters = {
            'baidu': coordTransform_utils.bd09_to_wgs84,
            'amap': coordTransform_utils.gcj02_to_wgs84,
            'tencent': lambda lng, lat: (lng, lat),
            'google': lambda lng, lat: (lng, lat)
        }
        
        convert1 = wgs84_converters.get(vendor1)
        convert2 = wgs84_converters.get(vendor2)
        # convert into WGS84
        coord1 = tuple(reversed(convert1(lng1, lat1)))
        coord2 = tuple(reversed(convert2(lng2, lat2)))
        # calculate distance
        distance = geodesic(coord1, coord2).meters
        return distance

    def model(dbt, session) -> DataFrame:
        records = [
            {
                'coord1_vendor':'amap',
                'coord1_addr':'Zhangjiang High-Tech Park',
                'coord1_lng':121.587691,
                'coord1_lat':31.201839,
                'coord2_vendor':'baidu',
                'coord2_addr':'JinKe Rd.',
                'coord2_lng':121.608551,
                'coord2_lat':31.210002
            }
        ]
        souece_df = session.createDataFrame(records)

        # Data processing BY RDD API or UDFs
        final_df = souece_df.withColumn("distance",
            udf_geo_distance(
              souece_df["coord1_lng"],souece_df["coord1_lat"],
              souece_df["coord2_lng"],souece_df["coord2_lat"],
              souece_df["coord1_vendor"],souece_df["coord2_vendor"])
            )
        return final_df
    ```
  </details>

## 1.9.1rc1 (2024-12-25)

### upgrade dbt-core

- Upgrade dbt-core to v1.9.1, keep pace with dbt-spark v1.9.0 & dbt-fabric v1.9.0(both are the latest version), along with logging timezone support
- Version string follows the dbt-core version("1.9.1")


## 1.7.0rc3 (2024-12-18)

### bug fix

- Support Lakehouse schema
- Quick fix dbt-core issue #6185 #9573 (dbt-core v1.8.9)

### new feature

- New custom macro `read_lakehouse_file` which enables querying lakehouse file in a data model
- Add dbt-spark into the requirement list


## 1.7.0rc2 (2024-12-04)

### patch

- upgraded the legacy APIs with dbt v1.8.9 based on dbt-fabricspark v1.7.0rc1. [get contact](mailto:willem.liang@icloud.com)