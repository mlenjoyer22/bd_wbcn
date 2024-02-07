import cianparser

def get_data():
    data = cianparser.parse(
        deal_type="rent_long",
        location="Москва",
        start_page=1,
        end_page=2,
        is_saving_csv=True,
    )
    return data

if __name__ == '__main__':
    print(len(get_data()))
