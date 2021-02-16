NORMAL_SERVICE = "00"


class OpenAPIError(Exception):
    pass


class ServiceKeyError(Exception):
    pass


def decode_key(key):
    from urllib.parse import unquote

    return unquote(key)


def get_temperature_raw_data(start_date, end_date, station_id, key, page_no):
    """기온 raw data 를 들고오는 함수
    공공데이터포털로 요청을 하고,
    header, body 등의 정보가 모두 들어있는 형태를 json 으로 반환한다.

    Arguments:
        start_date: 조회 시작 일자 (YYYYMMDD)
        end_date: 조회 종료 일자 (YYYYMMDD)
        station_id: 조회할 지점 번호 (종관기상관측 지점 번호 문서 참고)

    Returns:
        dict: 조회된 데이터를 json 형식으로 반환한다.

    Dependencies:
        import requests
    """
    import requests
    from json import JSONDecodeError

    url = "http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"
    key = decode_key(key)
    params = {
        "ServiceKey": key,
        "pageNo": page_no,
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "DAY",
        "startDt": start_date,
        "endDt": end_date,
        "stnIds": station_id,
    }

    response = requests.get(url, params=params)
    try:
        raw_data = response.json()
    except JSONDecodeError:
        raise ServiceKeyError("올바른 서비스키가 아닙니다.")

    if get_result_code(raw_data) != NORMAL_SERVICE:
        raise OpenAPIError(get_result_message(raw_data))

    return raw_data


def get_header(raw_data):
    return raw_data["response"]["header"]


def get_result_code(raw_data):
    return get_header(raw_data)["resultCode"]


def get_result_message(raw_data):
    return get_header(raw_data)["resultMsg"]


def get_body(raw_data):
    return raw_data["response"]["body"]


def get_page_no(raw_data):
    return get_body(raw_data)["pageNo"]


def get_total_count(raw_data):
    return get_body(raw_data)["totalCount"]


def get_num_of_rows(raw_data):
    return get_body(raw_data)["numOfRows"]


def get_items(raw_data):
    return get_body(raw_data)["items"]["item"]


def is_complete(page_no, num_of_rows, total_count):
    """현재 들고온 페이지가 마지막 페이지인지 확인한다.

    Arguments:
        page_no: 현재 페이지 넘버
        num_of_rows: 한 페이지당 행의 수
        total_count: 전체 행의 수
    
    Returns:
        bool: 마지막 페이지 여부

    Dependencies:
        import math
    """
    import math

    if not total_count:
        return True

    return page_no == math.ceil(total_count/num_of_rows)


def get_temperature_data(start_date, end_date, station_id, key):
    data = []
    page_no = 0

    while True:
        raw_data = get_temperature_raw_data(start_date, end_date, station_id, key, page_no+1)

        page_no = get_page_no(raw_data)
        num_of_rows = get_num_of_rows(raw_data)
        total_count = get_total_count(raw_data)

        data.extend(get_items(raw_data))

        if is_complete(page_no, num_of_rows, total_count):
            return data
