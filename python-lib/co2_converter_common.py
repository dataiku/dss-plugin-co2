import datetime


def date_chunk(start, end, chunk_size):
    # Set the range
    start_date = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end, "%Y-%m-%d")

    # Compute the difference in days
    delta = (end_date - start_date)
    diff_days = delta.days

    # Create the list of date
    date_list = [(start_date + (datetime.timedelta(days=1) * x)).strftime("%Y-%m-%d") for x in range(0, diff_days + 1)]

    if diff_days < 1:
        date_list = [[start_date.strftime("%Y-%m-%d"), (end_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]]
        return date_list

    chunked_list = list()
    date_list_size = len(date_list)

    for i in range(0, len(date_list), chunk_size):

        if(date_list_size < (2 * chunk_size)):
            size1 = date_list_size // 2

            chunked_list.append(date_list[i:i+size1])
            chunked_list.append(date_list[i+size1:i+date_list_size])
            return chunked_list
        else:
            chunked_list.append(date_list[i:i + chunk_size])

        date_list_size -= chunk_size

    return chunked_list
