


def chunk_list(data_list, n):
    chunk_size = max(1, len(data_list) // n)

    return [
        data_list[i : i + chunk_size] for i in range(0, len(data_list), chunk_size)
    ]